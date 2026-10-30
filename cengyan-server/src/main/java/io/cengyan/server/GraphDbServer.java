package io.cengyan.server;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.Auth;
import io.cengyan.catalog.Catalog;
import io.cengyan.catalog.LayoutMigration;
import io.cengyan.catalog.Membership;
import io.cengyan.query.QueryEngine;
import io.cengyan.store.GraphStore;
import io.cengyan.store.ShardedGraphStore;
import io.cengyan.store.StoreOptions;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 一个 JVM 进程，监听一个端口。启动顺序是打开存储、准备目录、启动超时扫描、再接受连接。
 */
public final class GraphDbServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(GraphDbServer.class);
    /** 普通 GQL 很小；批量导入请求允许更大的 JSON，但仍由批次行数和内存共同约束。 */
    private static final int MAX_LINE_BYTES = 16 * 1024 * 1024;

    private final ServerConfig config;
    private final GraphStore store;
    private final Catalog catalog;
    private final Membership membership;
    private final QueryRegistry queries;
    private final ScheduledExecutorService timeoutThread;
    private final ScheduledExecutorService compensateThread;
    private final ScheduledExecutorService cacheCompactThread;
    private final ExecutorService queryPool;
    private final EventLoopGroup boss;
    private final EventLoopGroup worker;
    private final Channel channel;
    private final HttpGateway httpGateway;
    private final AtomicBoolean closed = new AtomicBoolean();

    private GraphDbServer(
            ServerConfig config,
            GraphStore store,
            Catalog catalog,
            Membership membership,
            QueryRegistry queries,
            ScheduledExecutorService timeoutThread,
            ScheduledExecutorService compensateThread,
            ScheduledExecutorService cacheCompactThread,
            ExecutorService queryPool,
            EventLoopGroup boss,
            EventLoopGroup worker,
            Channel channel,
            HttpGateway httpGateway) {
        this.config = config;
        this.store = store;
        this.catalog = catalog;
        this.membership = membership;
        this.queries = queries;
        this.timeoutThread = timeoutThread;
        this.compensateThread = compensateThread;
        this.cacheCompactThread = cacheCompactThread;
        this.queryPool = queryPool;
        this.boss = boss;
        this.worker = worker;
        this.channel = channel;
        this.httpGateway = httpGateway;
    }

    public static void main(String[] args) throws Exception {
        Path configPath = args.length > 0 ? Path.of(args[0]) : Path.of("cengyan.properties");
        ServerConfig config = ServerConfig.load(configPath);
        GraphDbServer server = start(config);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "cengyan-shutdown"));
        log.info("层岩图数据库已监听 {}:{}", config.host(), server.port());
        server.channel.closeFuture().sync();
    }

    public static GraphDbServer start(ServerConfig config) {
        ShardedGraphStore store = null;
        Membership membership = null;
        ScheduledExecutorService timeoutThread = null;
        ScheduledExecutorService compensateThread = null;
        ScheduledExecutorService cacheCompactThread = null;
        ExecutorService queryPool = null;
        EventLoopGroup boss = null;
        EventLoopGroup worker = null;
        HttpGateway httpGateway = null;
        try {
            StoreOptions storeOptions = StoreOptions.server(
                    config.blockCacheBytes(),
                    config.rocksdbBackgroundJobs(),
                    config.rocksdbWriteBufferBytes());
            ShardedGraphStore opened = ShardedGraphStore.open(config.dataDir(), storeOptions);
            store = opened;
            LayoutMigration.migrate(config.dataDir(), opened);
            Catalog catalog = Catalog.load(store);
            log.info("已加载目录，共 {} 张图", catalog.graphs().size());
            Auth auth = new Auth(store);
            auth.ensureRoot(config.rootPassword());
            membership = Membership.open(
                    catalog, store, config.host() + ":" + config.port(), storeOptions);
            QueryRegistry queries = new QueryRegistry();
            Sessions sessions = new Sessions();
            timeoutThread = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "cengyan-timeout");
                thread.setDaemon(true);
                return thread;
            });
            ScheduledExecutorService timeout = timeoutThread;
            timeout.scheduleAtFixedRate(() -> {
                try {
                    queries.sweep();
                    sessions.sweep();
                } catch (RuntimeException ex) {
                    log.warn("超时扫描失败：{}", ex.toString());
                }
            }, 100, 100, TimeUnit.MILLISECONDS);
            compensateThread = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "cengyan-compensate");
                thread.setDaemon(true);
                return thread;
            });
            compensateThread.scheduleAtFixedRate(() -> {
                try {
                    catalog.recoverTxn();
                } catch (RuntimeException ex) {
                    log.warn("补偿扫描失败：{}", ex.toString());
                }
            }, 10, 10, TimeUnit.SECONDS);
            int cacheMinutes = config.cacheCompactIntervalMinutes();
            if (cacheMinutes > 0) {
                cacheCompactThread = Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "cengyan-cache-compact");
                    thread.setDaemon(true);
                    return thread;
                });
                ScheduledExecutorService cacheCompact = cacheCompactThread;
                cacheCompact.scheduleWithFixedDelay(() -> {
                    try {
                        log.info("开始压缩已打开目录里的写缓存");
                        opened.flush();
                        opened.compactOpen();
                        log.info("写缓存压缩完成");
                    } catch (RuntimeException ex) {
                        log.warn("写缓存压缩失败：{}", ex.toString());
                    }
                }, cacheMinutes, cacheMinutes, TimeUnit.MINUTES);
            }
            queryPool = Executors.newFixedThreadPool(config.queryThreads(), runnable -> {
                Thread thread = new Thread(runnable, "cengyan-query");
                thread.setDaemon(false);
                return thread;
            });
            boss = new NioEventLoopGroup(1, new DefaultThreadFactory("cengyan-boss"));
            worker = new NioEventLoopGroup(0, new DefaultThreadFactory("cengyan-io"));
            QueryEngine engine = new QueryEngine(
                    catalog, store, config.duckdbMemory(), config.duckdbTempDirectory(),
                    auth);
            engine.bind(membership);
            ExecuteService execute = new ExecuteService(engine, config, queries, queryPool, sessions);
            LineServer lineServer = new LineServer(execute);
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new LineBasedFrameDecoder(MAX_LINE_BYTES, true, true));
                            ch.pipeline().addLast(new StringDecoder(StandardCharsets.UTF_8));
                            ch.pipeline().addLast(new StringEncoder(StandardCharsets.UTF_8));
                            ch.pipeline().addLast(lineServer);
                        }
                    });
            Channel channel = bootstrap.bind(new InetSocketAddress(config.host(), config.port())).sync().channel();
            int tcpPort = ((InetSocketAddress) channel.localAddress()).getPort();
            int httpPort = config.httpPort() == 0 ? 0 : config.httpPort();
            httpGateway = HttpGateway.start(
                    config.host(), httpPort, "127.0.0.1", tcpPort,
                    config.httpTlsEnabled(), config.httpTlsKeystore(), config.httpTlsPassword());
            return new GraphDbServer(
                    config, store, catalog, membership, queries, timeoutThread, compensateThread,
                    cacheCompactThread, queryPool, boss, worker, channel, httpGateway);
        } catch (Exception ex) {
            if (worker != null) {
                worker.shutdownGracefully();
            }
            if (httpGateway != null) {
                httpGateway.close();
            }
            if (boss != null) {
                boss.shutdownGracefully();
            }
            if (queryPool != null) {
                queryPool.shutdownNow();
            }
            if (cacheCompactThread != null) {
                cacheCompactThread.shutdownNow();
            }
            if (compensateThread != null) {
                compensateThread.shutdownNow();
            }
            if (timeoutThread != null) {
                timeoutThread.shutdownNow();
            }
            if (membership != null) {
                membership.close();
            }
            if (store != null) {
                store.close();
            }
            if (ex instanceof CengyanException cengyan) {
                throw cengyan;
            }
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new CengyanException(ErrorCode.INTERNAL, "服务启动失败：" + ex.getMessage(), ex);
        }
    }

    public Catalog catalog() {
        return catalog;
    }

    public int port() {
        return ((InetSocketAddress) channel.localAddress()).getPort();
    }

    public int httpPort() {
        return httpGateway.port();
    }

    public ServerConfig config() {
        return config;
    }

    ExecutorService queryPool() {
        return queryPool;
    }

    QueryRegistry queries() {
        return queries;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            if (httpGateway != null) {
                httpGateway.close();
            }
            channel.close().syncUninterruptibly();
            worker.shutdownGracefully().syncUninterruptibly();
            boss.shutdownGracefully().syncUninterruptibly();
            queryPool.shutdownNow();
            if (cacheCompactThread != null) {
                cacheCompactThread.shutdownNow();
            }
            compensateThread.shutdownNow();
            timeoutThread.shutdownNow();
            membership.close();
            store.close();
            log.info("层岩图数据库已停止");
        } catch (RuntimeException ex) {
            log.warn("停止服务时出错：{}", ex.toString());
            store.close();
        }
    }
}
