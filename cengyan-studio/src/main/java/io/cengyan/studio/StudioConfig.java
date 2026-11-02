package io.cengyan.studio;

/**
 * 控制台自己的监听地址和端口，以及要转发到的图数据库地址。
 */
public record StudioConfig(
        int port,
        String host,
        String dbHost,
        int dbPort,
        int graphMaxNodes,
        int tableMaxNodes) {
    /** 保留原有构造方式，测试和嵌入式调用默认只监听本机。 */
    public StudioConfig(int port, String dbHost, int dbPort) {
        this(port, "127.0.0.1", dbHost, dbPort, 2000, 2000);
    }

    public StudioConfig(int port, String host, String dbHost, int dbPort) {
        this(port, host, dbHost, dbPort, 2000, 2000);
    }

    public StudioConfig(int port, String host, String dbHost, int dbPort, int graphMaxNodes) {
        this(port, host, dbHost, dbPort, graphMaxNodes, 2000);
    }

    public StudioConfig {
        if (port < 0 || port > 65535 || dbPort < 1 || dbPort > 65535) {
            throw new IllegalArgumentException("端口超出范围");
        }
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("控制台监听地址不能为空");
        }
        if (dbHost == null || dbHost.isBlank()) {
            throw new IllegalArgumentException("图数据库地址不能为空");
        }
        if (graphMaxNodes <= 0) {
            throw new IllegalArgumentException("关系图节点上限必须为正");
        }
        if (tableMaxNodes <= 0) {
            throw new IllegalArgumentException("表格展示行数上限必须为正");
        }
    }

    public static StudioConfig defaults() {
        return new StudioConfig(19670, "0.0.0.0", "127.0.0.1", 19669, 2000, 2000);
    }

    public static StudioConfig parse(String[] args) {
        int port = 19670;
        String listenHost = "0.0.0.0";
        String host = "127.0.0.1";
        int dbPort = 19669;
        int graphMaxNodes = 2000;
        int tableMaxNodes = 2000;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--host" -> listenHost = args[++i];
                case "--db-host" -> host = args[++i];
                case "--db-port" -> dbPort = Integer.parseInt(args[++i]);
                case "--graph-max-nodes" -> graphMaxNodes = Integer.parseInt(args[++i]);
                case "--table-max-nodes" -> tableMaxNodes = Integer.parseInt(args[++i]);
                default -> throw new IllegalArgumentException("无法识别的参数 " + args[i]);
            }
        }
        return new StudioConfig(port, listenHost, host, dbPort, graphMaxNodes, tableMaxNodes);
    }
}
