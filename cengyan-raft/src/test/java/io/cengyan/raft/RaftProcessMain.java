package io.cengyan.raft;

import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import java.nio.file.Path;
import java.util.ArrayList;

/** 集成测试专用独立 JVM 启动器，不是产品 Storage 入口。 */
public final class RaftProcessMain {
    public static void main(String[] args) throws Exception {
        int local = Integer.parseInt(args[0]);
        var peers = new ArrayList<RaftPeer>();
        for (int i = 2; i < args.length; i++) {
            peers.add(RaftPeer.newBuilder().setId("p" + (i - 2)).setAddress("127.0.0.1:" + args[i]).build());
        }
        var group = RaftGroup.valueOf(GroupIds.data("process-test", 1, 0), peers);
        try (var node = new RaftNode("process-test", "p" + local, "127.0.0.1",
                Integer.parseInt(args[local + 2]), Path.of(args[1]), group,
                new RaftSettings(4L * 1024 * 1024, 1000, 10, 2))) {
            System.out.println("READY");
            System.out.flush();
            // 父进程关闭 stdin 后正常结束；故障场景由父进程强制结束。
            System.in.read();
        }
    }
}
