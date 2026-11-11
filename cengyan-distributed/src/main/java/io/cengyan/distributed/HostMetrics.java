package io.cengyan.distributed;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Linux 节点的磁盘繁忙、磁盘吞吐和网卡吞吐。
 * 磁盘取数据目录所在块设备的 {@code /proc/diskstats}，网卡取 {@code /proc/net/dev}。
 * 速率是相邻两次计数的差，后台约 2 秒采样一次，运维页打开时就能读到当前速度。
 */
public final class HostMetrics {
    /** kernel diskstats 的扇区单位固定是 512 字节，与物理扇区大小无关。 */
    static final long SECTOR_BYTES = 512;
    static final long MIN_INTERVAL_NS = 200_000_000L;
    static final long MAX_INTERVAL_NS = 30_000_000_000L;
    private static final long SAMPLE_INTERVAL_SECONDS = 2;
    private static final Path MOUNTINFO = Path.of("/proc/self/mountinfo");
    private static final Path DISKSTATS = Path.of("/proc/diskstats");
    private static final Path NETDEV = Path.of("/proc/net/dev");
    private static final Path SYS_CLASS_NET = Path.of("/sys/class/net");
    private static final boolean LINUX = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("linux");
    /** 容器网卡和主机网卡会把同一份流量各记一遍，这些前缀不计入节点流量。 */
    private static final String[] VIRTUAL_PREFIXES = {
            "veth", "docker", "br-", "virbr", "flannel", "cni", "cali", "lxc", "dummy",
            "tun", "tap", "kube", "podman", "vboxnet", "vmnet", "ifb", "gre", "ip6tnl"
    };

    private final Object lock = new Object();
    private ScheduledExecutorService sampler;
    private Counters previous;
    private Rates rates = Rates.EMPTY;

    /** 进程启动后持续采样。重复调用无效果。非 Linux 不启动线程。 */
    public void watch(Path dataDirectory) {
        if (!LINUX) return;
        synchronized (lock) {
            if (sampler != null) return;
            Path directory = dataDirectory.toAbsolutePath();
            sampler = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "host-metrics");
                thread.setDaemon(true);
                return thread;
            });
            sampler.scheduleWithFixedDelay(() -> {
                try { accept(read(directory)); }
                catch (Exception ignored) { }
            }, 0, SAMPLE_INTERVAL_SECONDS, TimeUnit.SECONDS);
        }
    }

    public void close() {
        ScheduledExecutorService running;
        synchronized (lock) {
            running = sampler;
            sampler = null;
        }
        if (running != null) running.shutdownNow();
    }

    public Rates current() {
        synchronized (lock) { return rates; }
    }

    private Counters read(Path dataDirectory) throws IOException {
        String mounts = Files.readString(MOUNTINFO);
        String disks = Files.readString(DISKSTATS);
        String net = Files.readString(NETDEV);
        long nanos = System.nanoTime();
        String path;
        try { path = dataDirectory.toRealPath().toString(); }
        catch (IOException ex) { path = dataDirectory.toAbsolutePath().normalize().toString(); }
        return collect(mounts, disks, net, path, this::enslaved, nanos);
    }

    private boolean enslaved(String name) {
        if (name.isEmpty() || name.indexOf('/') >= 0 || name.indexOf('\\') >= 0 || name.indexOf('\0') >= 0) return true;
        return Files.isSymbolicLink(SYS_CLASS_NET.resolve(name).resolve("master"));
    }

    Rates accept(Counters now) {
        synchronized (lock) {
            Counters before = previous;
            if (before == null) {
                previous = now;
                return rates;
            }
            long elapsed = now.nanos - before.nanos;
            if (elapsed < MIN_INTERVAL_NS) return rates;
            previous = now;
            // 间隔太长时这次计数差是长时间平均，不能当成当前速度；保留新基线，等下一次短间隔。
            if (elapsed > MAX_INTERVAL_NS) {
                rates = Rates.EMPTY;
                return rates;
            }
            rates = diff(before, now);
            return rates;
        }
    }

    static Counters collect(String mountinfo, String diskstats, String netdev, String dataPath, Predicate<String> slave, long nanos) {
        Mount mount = selectMount(parseMounts(mountinfo), dataPath);
        Disk disk = mount == null ? null : parseDisks(diskstats).get(mount.major + ":" + mount.minor);
        long[] net = netBytes(parseNet(netdev), slave);
        if (disk == null) return new Counters(false, 0, 0, 0, net[0], net[1], nanos);
        return new Counters(true, disk.readSectors, disk.writeSectors, disk.ioMillis, net[0], net[1], nanos);
    }

    static Rates diff(Counters before, Counters after) {
        double seconds = (after.nanos - before.nanos) / 1_000_000_000.0;
        Double busy = null;
        Double read = null;
        Double write = null;
        if (before.disk && after.disk) {
            long io = after.ioMillis - before.ioMillis;
            long readSectors = after.readSectors - before.readSectors;
            long writeSectors = after.writeSectors - before.writeSectors;
            // io_ticks 是设备上有 I/O 未完成的毫秒数，可能略高于墙钟，页面上封顶为 100%。
            if (io >= 0) busy = Math.min(1.0, io / (seconds * 1000.0));
            if (readSectors >= 0 && readSectors <= Long.MAX_VALUE / SECTOR_BYTES) read = readSectors * SECTOR_BYTES / seconds;
            if (writeSectors >= 0 && writeSectors <= Long.MAX_VALUE / SECTOR_BYTES) write = writeSectors * SECTOR_BYTES / seconds;
        }
        Double rx = null;
        Double tx = null;
        long rxBytes = after.rxBytes - before.rxBytes;
        long txBytes = after.txBytes - before.txBytes;
        if (rxBytes >= 0) rx = rxBytes / seconds;
        if (txBytes >= 0) tx = txBytes / seconds;
        return new Rates(busy, read, write, rx, tx);
    }

    static Mount selectMount(List<Mount> mounts, String dataPath) {
        String path = dataPath.replace('\\', '/');
        Mount best = null;
        int bestLength = -1;
        for (Mount mount : mounts) {
            if (!covers(mount.mountPoint, path)) continue;
            // 同样长度时保留 mountinfo 里更靠后的挂载，后挂载的才是当前可见的文件系统。
            if (mount.mountPoint.length() >= bestLength) {
                best = mount;
                bestLength = mount.mountPoint.length();
            }
        }
        return best;
    }

    static boolean covers(String mountPoint, String path) {
        String mount = mountPoint.length() > 1 && mountPoint.endsWith("/") ? mountPoint.substring(0, mountPoint.length() - 1) : mountPoint;
        if (mount.equals("/")) return path.startsWith("/");
        return path.equals(mount) || path.startsWith(mount + "/");
    }

    static List<Mount> parseMounts(String text) {
        List<Mount> mounts = new ArrayList<>();
        for (String line : text.split("\n", -1)) {
            if (line.isBlank()) continue;
            String[] fields = line.split(" ");
            if (fields.length < 5) continue;
            String[] device = fields[2].split(":");
            if (device.length != 2) continue;
            try {
                mounts.add(new Mount(Integer.parseInt(device[0]), Integer.parseInt(device[1]), unescape(fields[4])));
            } catch (NumberFormatException ignored) { }
        }
        return mounts;
    }

    static Map<String, Disk> parseDisks(String text) {
        Map<String, Disk> disks = new HashMap<>();
        for (String line : text.split("\n", -1)) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) continue;
            String[] fields = trimmed.split("\\s+");
            // major minor name，之后第 3 个是读扇区，第 7 个是写扇区，第 10 个是 io_ticks。
            if (fields.length < 13) continue;
            try {
                disks.put(Integer.parseInt(fields[0]) + ":" + Integer.parseInt(fields[1]),
                        new Disk(Long.parseUnsignedLong(fields[5]), Long.parseUnsignedLong(fields[9]), Long.parseUnsignedLong(fields[12])));
            } catch (NumberFormatException ignored) { }
        }
        return disks;
    }

    static List<Nic> parseNet(String text) {
        List<Nic> nics = new ArrayList<>();
        for (String line : text.split("\n", -1)) {
            int colon = line.indexOf(':');
            if (colon <= 0) continue;
            String name = line.substring(0, colon).trim();
            if (name.isEmpty() || name.indexOf('|') >= 0) continue;
            String[] fields = line.substring(colon + 1).trim().split("\\s+");
            if (fields.length < 9) continue;
            try { nics.add(new Nic(name, Long.parseUnsignedLong(fields[0]), Long.parseUnsignedLong(fields[8]))); }
            catch (NumberFormatException ignored) { }
        }
        return nics;
    }

    static long[] netBytes(List<Nic> nics, Predicate<String> slave) {
        long[] physical = sum(nics, slave, true);
        if (physical != null) return physical;
        long[] any = sum(nics, slave, false);
        return any == null ? new long[] {0, 0} : any;
    }

    private static long[] sum(List<Nic> nics, Predicate<String> slave, boolean physicalOnly) {
        long rx = 0;
        long tx = 0;
        boolean any = false;
        for (Nic nic : nics) {
            if (nic.name.equals("lo") || slave.test(nic.name)) continue;
            if (physicalOnly && !physicalNic(nic.name)) continue;
            rx += nic.rxBytes;
            tx += nic.txBytes;
            any = true;
        }
        return any ? new long[] {rx, tx} : null;
    }

    static boolean physicalNic(String name) {
        String lower = name.toLowerCase(Locale.ROOT);
        for (String prefix : VIRTUAL_PREFIXES) if (lower.startsWith(prefix)) return false;
        return true;
    }

    /** mountinfo 用八进制转义空格和反斜杠，例如 {@code \040}。 */
    static String unescape(String value) {
        if (value.indexOf('\\') < 0) return value;
        StringBuilder out = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (current == '\\' && i + 3 < value.length()) {
                int code = 0;
                boolean octal = true;
                for (int shift = 1; shift <= 3; shift++) {
                    char digit = value.charAt(i + shift);
                    if (digit < '0' || digit > '7') { octal = false; break; }
                    code = code * 8 + (digit - '0');
                }
                if (octal) {
                    out.append((char) code);
                    i += 3;
                    continue;
                }
            }
            out.append(current);
        }
        return out.toString();
    }

    record Mount(int major, int minor, String mountPoint) {}
    record Disk(long readSectors, long writeSectors, long ioMillis) {}
    record Nic(String name, long rxBytes, long txBytes) {}
    record Counters(boolean disk, long readSectors, long writeSectors, long ioMillis, long rxBytes, long txBytes, long nanos) {}

    public record Rates(Double diskBusy, Double diskReadBytesPerSec, Double diskWriteBytesPerSec, Double netInBytesPerSec, Double netOutBytesPerSec) {
        static final Rates EMPTY = new Rates(null, null, null, null, null);

        public void putDisk(ObjectNode json) {
            put(json, "diskBusy", diskBusy);
            put(json, "diskReadBytesPerSec", diskReadBytesPerSec);
            put(json, "diskWriteBytesPerSec", diskWriteBytesPerSec);
        }

        public void putNet(ObjectNode json) {
            put(json, "netInBytesPerSec", netInBytesPerSec);
            put(json, "netOutBytesPerSec", netOutBytesPerSec);
        }

        private static void put(ObjectNode json, String key, Double value) {
            if (value != null && Double.isFinite(value) && value >= 0) json.put(key, value);
        }
    }
}
