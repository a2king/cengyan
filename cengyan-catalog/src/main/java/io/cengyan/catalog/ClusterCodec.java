package io.cengyan.catalog;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** 集群目录值的编码。键在 {@code KeyCodec}。 */
public final class ClusterCodec {
    public static final int ROLE_STORAGE = 1;
    public static final int ROLE_COMPUTE = 2;
    public static final int STATE_ACTIVE = 1;
    public static final int STATE_DRAINING = 2;
    public static final int STATE_LEARNER = 3;
    public static final int VOTE_VOTER = 1;
    public static final int VOTE_LEARNER = 2;
    public static final int MODE_NORMAL = 1;
    public static final int MODE_SYNC = 2;
    public static final int MOVE_MOVING = 1;
    public static final int TXN_OUT = 1;
    public static final int TXN_ROLLBACK = 2;

    private ClusterCodec() {
    }

    public static byte[] node(int role, int state, String address) {
        byte[] utf8 = address.getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[2 + utf8.length];
        value[0] = (byte) role;
        value[1] = (byte) state;
        System.arraycopy(utf8, 0, value, 2, utf8.length);
        return value;
    }

    public static int nodeRole(byte[] value) {
        require(value, 2, "节点记录不完整");
        return value[0] & 0xFF;
    }

    public static int nodeState(byte[] value) {
        require(value, 2, "节点记录不完整");
        return value[1] & 0xFF;
    }

    public static String nodeAddress(byte[] value) {
        require(value, 2, "节点记录不完整");
        return new String(value, 2, value.length - 2, StandardCharsets.UTF_8);
    }

    public static byte[] vote(int kind) {
        return new byte[] {(byte) kind};
    }

    public static int voteKind(byte[] value) {
        require(value, 1, "投票记录不完整");
        return value[0] & 0xFF;
    }

    public static byte[] space(int mode, int replicas, int parts) {
        byte[] value = new byte[5];
        value[0] = (byte) mode;
        value[1] = (byte) (replicas >>> 8);
        value[2] = (byte) replicas;
        value[3] = (byte) (parts >>> 8);
        value[4] = (byte) parts;
        return value;
    }

    public static int spaceMode(byte[] value) {
        require(value, 5, "空间记录不完整");
        return value[0] & 0xFF;
    }

    public static int spaceReplicas(byte[] value) {
        require(value, 5, "空间记录不完整");
        return ((value[1] & 0xFF) << 8) | (value[2] & 0xFF);
    }

    public static int spaceParts(byte[] value) {
        require(value, 5, "空间记录不完整");
        int parts = ((value[3] & 0xFF) << 8) | (value[4] & 0xFF);
        return parts <= 0 ? 1 : parts;
    }

    public static byte[] move(int state) {
        return new byte[] {(byte) state};
    }

    public static int moveState(byte[] value) {
        require(value, 1, "搬迁记录不完整");
        return value[0] & 0xFF;
    }

    public static byte[] txn(int state, byte[] properties) {
        byte[] props = properties == null ? new byte[0] : properties;
        byte[] value = new byte[1 + props.length];
        value[0] = (byte) state;
        System.arraycopy(props, 0, value, 1, props.length);
        return value;
    }

    public static int txnState(byte[] value) {
        require(value, 1, "补偿记录不完整");
        return value[0] & 0xFF;
    }

    public static byte[] txnProperties(byte[] value) {
        require(value, 1, "补偿记录不完整");
        return Arrays.copyOfRange(value, 1, value.length);
    }

    private static void require(byte[] value, int size, String message) {
        if (value == null || value.length < size) {
            throw new IllegalArgumentException(message);
        }
    }
}
