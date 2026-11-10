package io.cengyan.compute;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.LongSupplier;

/**
 * 本机执行名额的最终准入依据。心跳只用于选候选，不能替代原子预留。
 * 名额生命周期：RESERVED -> RUNNING -> FINISHED；取消执行不能提前释放仍在清理的资源。
 */
public final class ExecutionSlots {
    public enum State { RESERVED, RUNNING }
    public record Reservation(String token, String requestId) { }
    public record Load(int capacity, int active, int reserved, int available, long memoryReserved, long diskReserved) { }

    private record Entry(Reservation reservation, long expiresAt, long memory, long disk, State state) {
        Entry running() { return new Entry(reservation, expiresAt, memory, disk, State.RUNNING); }
    }

    private final int capacity;
    private final long memoryLimit;
    private final long diskLimit;
    private final long ttlNanos;
    private final LongSupplier clock;
    private final Map<String, Entry> byToken = new HashMap<>();
    private final Map<String, String> byRequest = new HashMap<>();
    private long memoryUsed;
    private long diskUsed;
    private boolean draining;

    public ExecutionSlots(int capacity, long memoryLimit, long diskLimit, Duration reservationTtl) {
        this(capacity, memoryLimit, diskLimit, reservationTtl, System::nanoTime);
    }

    ExecutionSlots(int capacity, long memoryLimit, long diskLimit, Duration reservationTtl, LongSupplier clock) {
        Objects.requireNonNull(reservationTtl);
        if (capacity < 1 || memoryLimit < 1 || diskLimit < 1 || reservationTtl.isNegative() || reservationTtl.isZero()
                || reservationTtl.compareTo(Duration.ofHours(1)) > 0) {
            throw new IllegalArgumentException("执行名额或资源配置无效");
        }
        this.capacity = capacity;
        this.memoryLimit = memoryLimit;
        this.diskLimit = diskLimit;
        this.ttlNanos = reservationTtl.toNanos();
        this.clock = Objects.requireNonNull(clock);
    }

    /** 未接纳返回 null。同一请求重复预留返回原票据，不重复占用额度。 */
    public synchronized Reservation reserve(String requestId, long memory, long disk) {
        if (requestId == null || requestId.isBlank() || memory < 0 || disk < 0) {
            throw new IllegalArgumentException("请求标识或资源额度无效");
        }
        sweep();
        String previous = byRequest.get(requestId);
        if (previous != null) {
            Entry entry = byToken.get(previous);
            if (memory != entry.memory || disk != entry.disk) throw new IllegalArgumentException("重复预留的资源额度不一致");
            return entry.reservation;
        }
        if (draining || byToken.size() == capacity || memory > memoryLimit - memoryUsed || disk > diskLimit - diskUsed) {
            return null;
        }
        Reservation reservation = new Reservation(UUID.randomUUID().toString(), requestId);
        Entry entry = new Entry(reservation, clock.getAsLong() + ttlNanos, memory, disk, State.RESERVED);
        byToken.put(reservation.token, entry);
        byRequest.put(requestId, reservation.token);
        memoryUsed += memory;
        diskUsed += disk;
        return reservation;
    }

    /** 仅第一个转发可以消费票据。过期或重复转发不能开始第二次执行。 */
    public synchronized boolean start(Reservation reservation) {
        sweep();
        Entry entry = byToken.get(Objects.requireNonNull(reservation).token);
        if (entry == null || !entry.reservation.equals(reservation) || entry.state != State.RESERVED) return false;
        byToken.put(reservation.token, entry.running());
        return true;
    }

    /** 只能撤销未开始的预留；执行中的取消要等执行器完成清理后调用 finish。 */
    public synchronized boolean cancelReservation(Reservation reservation) {
        Entry entry = byToken.get(Objects.requireNonNull(reservation).token);
        if (entry == null || !entry.reservation.equals(reservation) || entry.state != State.RESERVED) return false;
        remove(entry);
        return true;
    }

    public synchronized boolean finish(Reservation reservation) {
        Entry entry = byToken.get(Objects.requireNonNull(reservation).token);
        if (entry == null || !entry.reservation.equals(reservation) || entry.state != State.RUNNING) return false;
        remove(entry);
        return true;
    }

    public synchronized void sweep() {
        long now = clock.getAsLong();
        var expired = byToken.values().stream()
                .filter(entry -> entry.state == State.RESERVED && now - entry.expiresAt >= 0).toList();
        expired.forEach(this::remove);
    }

    private void remove(Entry entry) {
        byToken.remove(entry.reservation.token);
        byRequest.remove(entry.reservation.requestId);
        memoryUsed -= entry.memory;
        diskUsed -= entry.disk;
    }

    public synchronized void beginDrain() { draining = true; }
    public synchronized boolean drained() { sweep(); return draining && byToken.isEmpty(); }
    public synchronized Load load() {
        sweep();
        int active = (int) byToken.values().stream().filter(entry -> entry.state == State.RUNNING).count();
        int reserved = byToken.size() - active;
        return new Load(capacity, active, reserved, draining ? 0 : capacity - active - reserved, memoryUsed, diskUsed);
    }
}
