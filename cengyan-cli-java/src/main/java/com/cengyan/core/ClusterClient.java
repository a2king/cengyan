package com.cengyan.core;

import com.cengyan.core.exception.AuthenticationException;
import com.cengyan.core.internal.ComputeTransport;
import com.cengyan.core.internal.Connection;
import com.cengyan.core.internal.ConnectionPool;
import com.cengyan.core.internal.ErrorCodes;
import com.cengyan.core.internal.LineConnection;
import com.cengyan.core.internal.Transport;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Cengyan 计算集群客户端。只面向 Compute 协议：图名随每次请求显式发送，客户端不保存服务端会话。
 *
 * <pre>{@code
 * try (ClusterClient client = ClusterClient.builder()
 *         .addresses("10.0.0.1:19669", "10.0.0.2:19669")
 *         .user("root").password("***")
 *         .build()) {
 *     ClusterGraph graph = client.graph("Company");
 *     graph.write("INSERT (n:Person {id: 1, name: 'A'})");
 *     for (Record row : graph.read("MATCH (n:Person) RETURN n.name AS name LIMIT 10")) {
 *         System.out.println(row.getString("name"));
 *     }
 * }
 * }</pre>
 *
 * <p>读写由调用方显式指定，SDK 不猜 GQL 的读写属性：
 * <ul>
 *   <li>写请求的 requestId 在这里一次定下，断线重试（换节点，最多 3 次）和令牌过期后的重发都用它，
 *       服务端在幂等窗口内按它去重；最终无法确认时抛 {@link com.cengyan.core.exception.CommitUncertainException}；</li>
 *   <li>读请求断线不重发，但下一次请求会离开失败的节点。</li>
 * </ul>
 * 首次请求时自动登录；令牌过期时自动重登一次并重发。线程安全，应在应用内复用同一个实例。
 */
public final class ClusterClient implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger("com.cengyan.core");
    /** 调用方不能通过 admin 参数覆盖的协议字段。 */
    private static final Set<String> RESERVED_ADMIN_FIELDS = Collections.unmodifiableSet(new HashSet<String>(
            Arrays.asList("op", "command", "token", "session")));

    private final Transport transport;
    private final String user;
    private final String password;
    private final Object authLock = new Object();
    private volatile String token;

    ClusterClient(Transport transport, String user, String password) {
        if (user == null || user.isEmpty()) throw new IllegalArgumentException("必须提供数据库用户名");
        if (password == null || password.isEmpty()) throw new IllegalArgumentException("必须提供数据库密码：SDK 不内置默认口令");
        this.transport = transport;
        this.user = user;
        this.password = password;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String user() {
        return user;
    }

    /** 立即登录（否则在首次请求时登录）。登录失败抛 {@link AuthenticationException}。 */
    public ClusterClient login() {
        String fresh = loginRequest();
        synchronized (authLock) {
            token = fresh;
        }
        return this;
    }

    private String loginRequest() {
        Map<String, Object> request = new LinkedHashMap<String, Object>();
        request.put("op", "login");
        request.put("user", user);
        request.put("password", password);
        Map<String, Object> response = ErrorCodes.check(transport.request(request, null, false, null), false);
        Object value = response.get("token");
        if (!(value instanceof String) || ((String) value).isEmpty()) {
            throw new AuthenticationException("登录响应缺少令牌", "UNAUTHENTICATED", response);
        }
        return (String) value;
    }

    private String currentToken() {
        String current = token;
        if (current != null) return current;
        synchronized (authLock) {
            if (token == null) token = loginRequest();
            return token;
        }
    }

    /** 令牌过期或凭据版本变更后重新登录；并发线程只让一个去换。 */
    String relogin(String stale) {
        synchronized (authLock) {
            if (token != null && !token.equals(stale)) return token;
            LOG.log(Level.FINE, "令牌失效，用户 {0} 重新登录", user);
            token = loginRequest();
            return token;
        }
    }

    /** 发送一条底层协议请求，返回 code=OK 的响应；其他码抛对应异常。 */
    public Map<String, Object> request(Map<String, ?> payload, boolean write) {
        return request(payload, write, null, null);
    }

    /**
     * 发送一条底层协议请求。
     *
     * @param timeout 总预算（含重试），null 用客户端默认超时
     * @param address 指定计算节点，null 用默认入口；指定节点断线时改走默认入口
     */
    public Map<String, Object> request(Map<String, ?> payload, boolean write, Duration timeout, Address address) {
        Map<String, Object> body = new LinkedHashMap<String, Object>(payload);
        if (write) {
            Object id = body.get("requestId");
            if (id == null || String.valueOf(id).isEmpty()) body.put("requestId", UUID.randomUUID().toString());
        }
        Long millis = timeout == null ? null : positiveMillis(timeout, "timeout");
        String used = currentToken();
        try {
            return send(body, used, millis, write, address);
        } catch (AuthenticationException expired) {
            // 过期后原地重发是安全的：写请求的 requestId 不变，服务端按幂等窗口去重。
            return send(body, relogin(used), millis, write, address);
        }
    }

    private Map<String, Object> send(Map<String, Object> body, String withToken, Long timeout, boolean write, Address address) {
        Map<String, Object> request = new LinkedHashMap<String, Object>(body);
        request.put("token", withToken);
        Map<String, Object> response = transport.request(request, timeout, write, address);
        if (write && !"OK".equals(response.get("code")) && response.get("requestId") == null) {
            // 服务端的错误响应不一定回带 requestId；补上，调用方才能凭它确认或重放。
            response = new LinkedHashMap<String, Object>(response);
            response.put("requestId", body.get("requestId"));
        }
        return ErrorCodes.check(response, write);
    }

    /** 只读执行一条不属于特定图的 GQL（如 {@code SHOW GRAPHS}）。 */
    public Result execute(String query) {
        return execute(query, null, null, false, null);
    }

    /**
     * 执行一条远程 GQL；graph 显式随请求发送，不依赖服务端会话。
     *
     * @param graph 图名，可以为 null（目录语句）
     * @param write 写语句必须传 true 才能获得断线重试与幂等保护
     */
    public Result execute(String query, String graph, Map<String, ?> params, boolean write, Duration timeout) {
        if (query == null) throw new IllegalArgumentException("query 不能为 null");
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("op", "execute");
        payload.put("query", query);
        payload.put("params", params == null ? Collections.<String, Object>emptyMap() : params);
        if (graph != null) payload.put("graph", graph);
        return new Result(request(payload, write, timeout, null));
    }

    /** 建图：16 分片、1 副本、点和边主键都是 INT64。 */
    public Map<String, Object> createGraph(String name) {
        return createGraph(name, 16, 1, IdType.INT64, IdType.INT64);
    }

    public Map<String, Object> createGraph(String name, int partitions) {
        return createGraph(name, partitions, 1, IdType.INT64, IdType.INT64);
    }

    /** 建图。分片数和主键类型建图后不能修改。 */
    public Map<String, Object> createGraph(String name, int partitions, int replicas, IdType vertexIdType, IdType edgeIdType) {
        if (name == null || name.isEmpty()) throw new IllegalArgumentException("图名不能为空");
        if (partitions < 1 || partitions > 65535) throw new IllegalArgumentException("分片数必须在 1 和 65535 之间：" + partitions);
        if (replicas < 1) throw new IllegalArgumentException("副本数必须为正：" + replicas);
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("op", "createGraph");
        payload.put("name", name);
        payload.put("partitions", partitions);
        payload.put("replicas", replicas);
        payload.put("vertexIdType", vertexIdType.name());
        payload.put("edgeIdType", edgeIdType.name());
        return request(payload, true);
    }

    /** 某个图的操作入口（不发请求，不检查图是否存在）。 */
    public ClusterGraph graph(String name) {
        return new ClusterGraph(this, name);
    }

    /**
     * 用服务端登记的计算节点地址替换本地地址表。服务端返回 IP 地址：
     * 经代理访问或需要按证书主机名校验 TLS 时不要调用。
     */
    public List<Address> refreshNodes() {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("op", "members");
        Map<String, Object> response = request(payload, false);
        Object raw = response.get("addresses");
        if (!(raw instanceof List)) throw new com.cengyan.core.exception.ProtocolException("members 响应缺少 addresses", "PROTOCOL_ERROR", response);
        List<Address> addresses = new ArrayList<Address>();
        for (Object item : (List<?>) raw) {
            if (!(item instanceof Map)) throw new com.cengyan.core.exception.ProtocolException("地址格式无效", "PROTOCOL_ERROR", response);
            Object host = ((Map<?, ?>) item).get("host");
            Object port = ((Map<?, ?>) item).get("port");
            if (!(host instanceof String) || !(port instanceof Long)) {
                throw new com.cengyan.core.exception.ProtocolException("地址格式无效：" + item, "PROTOCOL_ERROR", response);
            }
            addresses.add(new Address((String) host, ((Long) port).intValue()));
        }
        transport.updateAddresses(addresses);
        return Collections.unmodifiableList(addresses);
    }

    /** 只读管理命令，如 {@code admin("showNodes")}。 */
    public Map<String, Object> admin(String command) {
        return admin(command, false, null);
    }

    /**
     * 管理命令。会改动集群状态的命令必须显式传 {@code write=true} 才能获得断线重试；
     * SDK 不按命令名猜读写，新增命令默认按只读处理。
     *
     * @param values 命令参数，不能包含 op / command / token / session
     */
    public Map<String, Object> admin(String command, boolean write, Map<String, ?> values) {
        if (command == null || command.isEmpty()) throw new IllegalArgumentException("管理命令不能为空");
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("op", "admin");
        payload.put("command", command);
        if (values != null) {
            Set<String> reserved = new TreeSet<String>(values.keySet());
            reserved.retainAll(RESERVED_ADMIN_FIELDS);
            if (!reserved.isEmpty()) throw new IllegalArgumentException("管理命令参数不能覆盖协议字段：" + reserved);
            payload.putAll(values);
        }
        return request(payload, write);
    }

    /** 关闭全部连接。进行中的请求会以连接错误结束（写请求按提交状态未知处理）。 */
    @Override
    public void close() {
        transport.close();
    }

    static long positiveMillis(Duration duration, String name) {
        if (duration == null || duration.isNegative() || duration.isZero()) throw new IllegalArgumentException(name + " 必须为正：" + duration);
        long millis = duration.toMillis();
        return Math.max(1L, millis);
    }

    /** {@link ClusterClient} 的配置。 */
    public static final class Builder {
        private final List<Address> addresses = new ArrayList<Address>();
        private String user = "root";
        private String password;
        private Duration timeout = Duration.ofSeconds(30);
        private boolean tls;
        private SSLContext sslContext;
        private int maxIdleConnectionsPerAddress = 8;
        private Duration idleTimeout = Duration.ofSeconds(60);
        private Connection.Factory connectionFactory;

        private Builder() {
        }

        /** 计算节点地址，{@code host:port}，IPv6 写成 {@code [::1]:19669}。可多次调用累加。 */
        public Builder addresses(String... values) {
            for (String value : values) addresses.add(Address.parse(value));
            return this;
        }

        public Builder addresses(Address... values) {
            addresses.addAll(Arrays.asList(values));
            return this;
        }

        public Builder addresses(List<Address> values) {
            addresses.addAll(values);
            return this;
        }

        public Builder address(String host, int port) {
            addresses.add(new Address(host, port));
            return this;
        }

        /** 用户名，默认 root。 */
        public Builder user(String value) {
            this.user = value;
            return this;
        }

        /** 密码，必填。 */
        public Builder password(String value) {
            this.password = value;
            return this;
        }

        /** 单次请求的总预算（含写请求重试），默认 30 秒。 */
        public Builder timeout(Duration value) {
            positiveMillis(value, "timeout");
            this.timeout = value;
            return this;
        }

        /**
         * 使用 JVM 默认信任库的 TLS。Compute 端口本身不终止 TLS：需要在计算节点前部署 TLS 终止代理，
         * 地址填代理地址。
         */
        public Builder tls(boolean value) {
            this.tls = value;
            return this;
        }

        /** 自定义 TLS（自签 CA、双向 TLS）；设置后即启用 TLS。会校验证书主机名。 */
        public Builder sslContext(SSLContext value) {
            this.sslContext = value;
            return this;
        }

        /** 每个地址最多保留的空闲连接数，默认 8；并发更高时临时多开的连接用完即关。 */
        public Builder maxIdleConnectionsPerAddress(int value) {
            if (value < 0) throw new IllegalArgumentException("maxIdleConnectionsPerAddress 不能为负数");
            this.maxIdleConnectionsPerAddress = value;
            return this;
        }

        /** 空闲连接的最长保留时间，默认 60 秒。 */
        public Builder idleTimeout(Duration value) {
            positiveMillis(value, "idleTimeout");
            this.idleTimeout = value;
            return this;
        }

        /** 测试用：替换底层连接。 */
        Builder connectionFactory(Connection.Factory value) {
            this.connectionFactory = value;
            return this;
        }

        public ClusterClient build() {
            if (password == null || password.isEmpty()) throw new IllegalArgumentException("必须提供数据库密码：SDK 不内置默认口令");
            SSLContext context = sslContext;
            if (context == null && tls) {
                try {
                    context = SSLContext.getDefault();
                } catch (NoSuchAlgorithmException ex) {
                    throw new IllegalStateException("JVM 没有可用的默认 TLS 实现", ex);
                }
            }
            final SSLContext secure = context;
            Connection.Factory factory = connectionFactory != null ? connectionFactory : address -> new LineConnection(address, secure);
            ConnectionPool pool = new ConnectionPool(factory, maxIdleConnectionsPerAddress, idleTimeout.toMillis());
            Transport transport = new ComputeTransport(addresses, timeout.toMillis(), pool, new SecureRandom());
            return new ClusterClient(transport, user, password);
        }
    }
}
