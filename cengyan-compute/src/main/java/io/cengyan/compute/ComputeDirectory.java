package io.cengyan.compute;

import java.time.Duration;
import java.util.*;
import java.util.function.LongSupplier;

/**
 * 成员快照来自 Compute Raft，实时负载只保存在内存。负载新鲜度使用本机接收时间。
 * 节点重启代次来自控制面，拒绝前一进程的迟到心跳覆盖新进程负载。
 */
public final class ComputeDirectory {
    public record Member(String nodeId, long incarnation, boolean draining) {
        public Member {
            if (nodeId == null || nodeId.isBlank() || incarnation < 1) throw new IllegalArgumentException("计算节点身份无效");
        }
    }
    public record Heartbeat(String nodeId, long incarnation, long sequence, ExecutionSlots.Load load,
                            long memoryAvailable, long diskAvailable) {
        public Heartbeat {
            if (nodeId == null || nodeId.isBlank() || incarnation < 1 || sequence < 1 || load == null
                    || memoryAvailable < 0 || diskAvailable < 0 || load.capacity() < 1 || load.active() < 0
                    || load.reserved() < 0 || load.available() < 0 || load.memoryReserved() < 0 || load.diskReserved() < 0
                    || (long) load.active() + load.reserved() + load.available() > load.capacity()) {
                throw new IllegalArgumentException("计算节点负载无效");
            }
        }
    }
    private record Observed(Heartbeat heartbeat, long receivedAt) { }

    private final long staleNanos;
    private final LongSupplier clock;
    private final Map<String, Member> members = new HashMap<>();
    private final Map<String, Observed> observed = new HashMap<>();
    private long version;

    public ComputeDirectory(Duration staleAfter) { this(staleAfter, System::nanoTime); }
    ComputeDirectory(Duration staleAfter, LongSupplier clock) {
        if (staleAfter == null || staleAfter.isZero() || staleAfter.isNegative() || staleAfter.compareTo(Duration.ofHours(1)) > 0) {
            throw new IllegalArgumentException("负载失效时间无效");
        }
        staleNanos = staleAfter.toNanos();
        this.clock = Objects.requireNonNull(clock);
    }

    public synchronized boolean updateMembers(long newVersion, Collection<Member> snapshot) {
        if (newVersion < 1) throw new IllegalArgumentException("成员版本无效");
        if (newVersion <= version) return false;
        Map<String, Member> next = new HashMap<>();
        for (Member member : snapshot) if (next.put(member.nodeId, member) != null) throw new IllegalArgumentException("成员列表重复");
        for (Member member : next.values()) {
            Member previous = members.get(member.nodeId);
            if (previous != null && member.incarnation < previous.incarnation) throw new IllegalArgumentException("节点代次不能倒退");
        }
        observed.entrySet().removeIf(entry -> {
            Member current = next.get(entry.getKey());
            return current == null || current.incarnation != entry.getValue().heartbeat.incarnation;
        });
        members.clear();
        members.putAll(next);
        version = newVersion;
        return true;
    }

    public synchronized boolean heartbeat(Heartbeat heartbeat) {
        Member member = members.get(heartbeat.nodeId);
        if (member == null || member.incarnation != heartbeat.incarnation) return false;
        Observed previous = observed.get(heartbeat.nodeId);
        if (previous != null && previous.heartbeat.sequence >= heartbeat.sequence) return false;
        observed.put(heartbeat.nodeId, new Observed(heartbeat, clock.getAsLong()));
        return true;
    }

    /** 返回候选而不是已获得名额的节点；调度者仍须调用远端原子 reserve。 */
    public synchronized List<Heartbeat> candidates(long requiredMemory, long requiredDisk) {
        if (requiredMemory < 0 || requiredDisk < 0) throw new IllegalArgumentException("请求资源不能为负数");
        long now = clock.getAsLong();
        var candidates = new ArrayList<Heartbeat>();
        for (var entry : observed.entrySet()) {
            Member member = members.get(entry.getKey());
            Observed sample = entry.getValue();
            Heartbeat heartbeat = sample.heartbeat;
            if (!member.draining && now - sample.receivedAt < staleNanos && heartbeat.load.available() > 0
                    && requiredMemory <= heartbeat.memoryAvailable && requiredDisk <= heartbeat.diskAvailable) candidates.add(heartbeat);
        }
        candidates.sort(Comparator.<Heartbeat>comparingInt(h -> h.load.available()).reversed()
                .thenComparingInt(h -> h.load.active()).thenComparing(Heartbeat::nodeId));
        return List.copyOf(candidates);
    }

    public synchronized long version() { return version; }
}
