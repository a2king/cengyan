package io.cengyan.distributed;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

/** 磁盘和网卡速率只依赖 procfs 文本，不读取本机设备。 */
class HostMetricsTest {
    private static final String MOUNTS = """
            31 1 8:0 / / rw - ext4 /dev/sda rw
            25 1 8:1 / /data rw - ext4 /dev/sda1 rw
            40 1 8:2 / /mnt/my\\040disk rw - ext4 /dev/sda2 rw
            """;
    private static final String DISK_BEFORE = """
               8       0 sda 1 0 100 0 1 0 200 0 0 500 0
               8       1 sda1 1 0 100 0 1 0 200 0 0 500 0 0 0 0 0
               8       2 sda2 1 0 1 0 1 0 1 0 0 1 0
            """;
    private static final String DISK_AFTER = """
               8       0 sda 9 0 99999 0 9 0 99999 0 0 9000 0
               8       1 sda1 2 0 2148 0 2 0 1224 0 0 1500 0
               8       2 sda2 1 0 1 0 1 0 1 0 0 1 0
            """;
    private static final String NET_BEFORE = """
            Inter-|   Receive                                                |  Transmit
             face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
                lo: 10 0 0 0 0 0 0 0 10 0 0 0 0 0 0 0
              eth0: 1000 0 0 0 0 0 0 0 2000 0 0 0 0 0 0 0
              eth1: 3000 0 0 0 0 0 0 0 4000 0 0 0 0 0 0 0
             veth0: 9000 0 0 0 0 0 0 0 9000 0 0 0 0 0 0 0
            bond0: 50 0 0 0 0 0 0 0 80 0 0 0 0 0 0 0
            """;
    private static final String NET_AFTER = """
                lo: 10 0 0 0 0 0 0 0 10 0 0 0 0 0 0 0
              eth0: 1049576 0 0 0 0 0 0 0 4048 0 0 0 0 0 0 0
              eth1: 4000 0 0 0 0 0 0 0 4000 0 0 0 0 0 0 0
             veth0: 999999 0 0 0 0 0 0 0 999999 0 0 0 0 0 0 0
            bond0: 50 0 0 0 0 0 0 0 80 0 0 0 0 0 0 0
            """;
    private static final Predicate<String> NONE_SLAVE = name -> false;

    @Test void dataDirectoryUsesItsOwnDiskAndSkipsVirtualNics() {
        HostMetrics.Counters before = HostMetrics.collect(MOUNTS, DISK_BEFORE, NET_BEFORE, "/data/storage-1", NONE_SLAVE, 0);
        HostMetrics.Counters after = HostMetrics.collect(MOUNTS, DISK_AFTER, NET_AFTER, "/data/storage-1", NONE_SLAVE, 1_000_000_000L);
        HostMetrics.Rates rates = HostMetrics.diff(before, after);
        assertEquals(1.0, rates.diskBusy());
        assertEquals(1_048_576.0, rates.diskReadBytesPerSec());
        assertEquals(524_288.0, rates.diskWriteBytesPerSec());
        assertEquals(1_049_576.0, rates.netInBytesPerSec());
        assertEquals(2_048.0, rates.netOutBytesPerSec());
    }

    @Test void siblingMountIsNotTheDataDisk() {
        HostMetrics.Mount mount = HostMetrics.selectMount(HostMetrics.parseMounts(MOUNTS), "/data2/node");
        assertNotNull(mount);
        assertEquals(8, mount.major());
        assertEquals(0, mount.minor());
    }

    @Test void octalSpaceInMountPointIsDecoded() {
        HostMetrics.Mount mount = HostMetrics.selectMount(HostMetrics.parseMounts(MOUNTS), "/mnt/my disk/node");
        assertNotNull(mount);
        assertEquals(8, mount.major());
        assertEquals(2, mount.minor());
        assertEquals("/mnt/my disk", mount.mountPoint());
    }

    @Test void busyTimeAboveTheIntervalIsCapped() {
        HostMetrics.Counters before = new HostMetrics.Counters(true, 0, 0, 0, 0, 0, 0);
        HostMetrics.Counters after = new HostMetrics.Counters(true, 0, 0, 5_000, 0, 0, 1_000_000_000L);
        assertEquals(1.0, HostMetrics.diff(before, after).diskBusy());
    }

    @Test void counterResetOmitsThatDirectionOnly() {
        HostMetrics.Counters before = new HostMetrics.Counters(true, 100, 10, 10, 500, 20, 0);
        HostMetrics.Counters after = new HostMetrics.Counters(true, 40, 12, 10, 100, 28, 1_000_000_000L);
        HostMetrics.Rates rates = HostMetrics.diff(before, after);
        assertNull(rates.diskReadBytesPerSec());
        assertEquals(1024.0, rates.diskWriteBytesPerSec());
        assertNull(rates.netInBytesPerSec());
        assertEquals(8.0, rates.netOutBytesPerSec());
    }

    @Test void bondSlaveIsNotAddedToTheBond() {
        String net = """
                eth0: 5000 0 0 0 0 0 0 0 5000 0 0 0 0 0 0 0
                bond0: 800 0 0 0 0 0 0 0 100 0 0 0 0 0 0 0
                """;
        long[] bytes = HostMetrics.netBytes(HostMetrics.parseNet(net), name -> name.equals("eth0"));
        assertArrayEquals(new long[] {800, 100}, bytes);
    }

    @Test void onlyVirtualNicsFallBackToThoseNics() {
        String net = """
                    lo: 10 0 0 0 0 0 0 0 10 0 0 0 0 0 0 0
                 veth0: 40 0 0 0 0 0 0 0 70 0 0 0 0 0 0 0
                """;
        long[] bytes = HostMetrics.netBytes(HostMetrics.parseNet(net), NONE_SLAVE);
        assertArrayEquals(new long[] {40, 70}, bytes);
    }

    @Test void shortIntervalDoesNotMoveTheBaseline() {
        HostMetrics metrics = new HostMetrics();
        HostMetrics.Counters baseline = new HostMetrics.Counters(true, 0, 0, 0, 0, 0, 0);
        HostMetrics.Counters burst = new HostMetrics.Counters(true, 5_000, 0, 0, 0, 0, 100_000_000L);
        HostMetrics.Counters later = new HostMetrics.Counters(true, 2, 0, 0, 0, 0, 1_000_000_000L);
        assertNull(metrics.accept(baseline).diskReadBytesPerSec());
        assertNull(metrics.accept(burst).diskReadBytesPerSec());
        assertEquals(1024.0, metrics.accept(later).diskReadBytesPerSec());
    }

    @Test void longGapStartsANewBaseline() {
        HostMetrics metrics = new HostMetrics();
        assertNull(metrics.accept(new HostMetrics.Counters(true, 0, 0, 0, 0, 0, 0)).diskReadBytesPerSec());
        assertEquals(512.0, metrics.accept(new HostMetrics.Counters(true, 1, 0, 0, 0, 0, 1_000_000_000L)).diskReadBytesPerSec());
        long resetAt = 1_000_000_000L + HostMetrics.MAX_INTERVAL_NS + 1;
        assertNull(metrics.accept(new HostMetrics.Counters(true, 100_000, 0, 0, 0, 0, resetAt)).diskReadBytesPerSec());
        assertEquals(1024.0, metrics.accept(new HostMetrics.Counters(true, 100_002, 0, 0, 0, 0, resetAt + 1_000_000_000L)).diskReadBytesPerSec());
    }

    @Test void missingRatesAreOmittedFromJson() {
        ObjectNode json = new ObjectMapper().createObjectNode();
        new HostMetrics.Rates(0.25, 12.0, null, null, null).putDisk(json);
        new HostMetrics.Rates(null, null, null, 8.0, Double.NaN).putNet(json);
        assertEquals(0.25, json.get("diskBusy").asDouble());
        assertEquals(12.0, json.get("diskReadBytesPerSec").asDouble());
        assertFalse(json.has("diskWriteBytesPerSec"));
        assertEquals(8.0, json.get("netInBytesPerSec").asDouble());
        assertFalse(json.has("netOutBytesPerSec"));
    }
}
