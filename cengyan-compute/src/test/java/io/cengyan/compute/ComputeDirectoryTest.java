package io.cengyan.compute;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;

class ComputeDirectoryTest {
    @Test void 按可用槽位排序并过滤资源不足过期及排空节点() {
        AtomicLong time = new AtomicLong();
        var directory = new ComputeDirectory(Duration.ofSeconds(3), time::get);
        assertTrue(directory.updateMembers(1, List.of(new ComputeDirectory.Member("a", 1, false),
                new ComputeDirectory.Member("b", 1, false), new ComputeDirectory.Member("c", 1, true))));
        directory.heartbeat(heartbeat("a", 1, 1, 4));
        directory.heartbeat(heartbeat("b", 1, 1, 6));
        directory.heartbeat(heartbeat("c", 1, 1, 8));
        assertEquals(List.of("b", "a"), directory.candidates(100, 100).stream().map(ComputeDirectory.Heartbeat::nodeId).toList());
        assertTrue(directory.candidates(101, 100).isEmpty());
        time.addAndGet(Duration.ofSeconds(3).toNanos());
        assertTrue(directory.candidates(0, 0).isEmpty());
    }

    @Test void 节点代次和序列号拒绝迟到负载() {
        var directory = new ComputeDirectory(Duration.ofSeconds(3));
        directory.updateMembers(1, List.of(new ComputeDirectory.Member("a", 1, false)));
        assertTrue(directory.heartbeat(heartbeat("a", 1, 2, 4)));
        assertFalse(directory.heartbeat(heartbeat("a", 1, 1, 8)));
        assertFalse(directory.updateMembers(1, List.of()));
        directory.updateMembers(2, List.of(new ComputeDirectory.Member("a", 2, false)));
        assertFalse(directory.heartbeat(heartbeat("a", 1, 3, 8)));
        assertTrue(directory.candidates(0, 0).isEmpty());
        assertTrue(directory.heartbeat(heartbeat("a", 2, 1, 6)));
        assertEquals(6, directory.candidates(0, 0).getFirst().load().available());
        directory.updateMembers(3, List.of());
        assertFalse(directory.heartbeat(heartbeat("a", 2, 2, 6)));
    }

    private static ComputeDirectory.Heartbeat heartbeat(String node, long incarnation, long sequence, int available) {
        return new ComputeDirectory.Heartbeat(node, incarnation, sequence, new ExecutionSlots.Load(8, 8 - available, 0, available, 0, 0), 100, 100);
    }
}
