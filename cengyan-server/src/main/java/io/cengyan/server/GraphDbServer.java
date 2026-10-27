package io.cengyan.server;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.Catalog;
import io.cengyan.query.QueryEngine;
import io.cengyan.store.GraphStore;
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
    private static final int MAX_LINE_BYTES = 1024 * 1024;

    private final ServerConfig config;
    private final GraphStore store;
    private final Catalog catalog;
    private final QueryRegistry queries;
    private final ScheduledExecutorService timeoutThread;
    private final ExecutorService queryPool;
    private final EventLoopGroup boss;
    private final EventLoopGroup worker;
    private final Channel channel;
    private final AtomicBoolean closed = new AtomicBoolean();

    private GraphDbServer(
            ServerConfig config,
            GraphStore store,
            Catalog catalog,
            QueryRegistry queries,
            ScheduledExecutorService timeoutThread,
            ExecutorService queryPool,
            EventLoopGroup boss,
            EventLoopGroup worker,
            Channel channel) {
        this.config = config;
        this.store = store;
        this.catalog = catalog;
        this.queries = queries;
        this.timeoutThread = timeoutThread;
        this.queryPool = queryPool;
        this.boss = boss;
        this.worker = worker;
        this.channel = channel;
    }

    public static void main(String[] args) throws Exception {
        Path configPath = args.length > 0 ? Path.of(args[0]) : Path.of("cengyan.properties");
        ServerConfig config = ServerConfig.load(configPath);
        GraphDbServer server = start(config);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "cengyan-shutdown"));
        log.info("层岩图数据库已监听端口 {}", server.port());
        server.channel.closeFuture().sync();
    }

    public static GraphDbServer start(ServerConfig config) {
        GraphStore store = null;
        ScheduledExecutorService timeoutThread = null;
        ExecutorService queryPool = null;
        EventLoopGroup boss = null;
        EventLoopGroup worker = null;
        try {
            store = GraphStore.open(config.dataDir(), StoreOptions.server(config.blockCacheBytes()));
            Catalog catalog = Catalog.load(store);
            log.info("已加载目录，共 {} 张图", catalog.graphs().size());
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
            queryPool = Executors.newFixedThreadPool(config.queryThreads(), runnable -> {
                Thread thread = new Thread(runnable, "cengyan-query");
                thread.setDaemon(false);
                return thread;
            });
            boss = new NioEventLoopGroup(1, new DefaultThreadFactory("cengyan-boss"));
            worker = new NioEventLoopGroup(0, new DefaultThreadFactory("cengyan-io"));
            QueryEngine engine = new QueryEngine(catalog, store);
            ExecuteService execute = new ExecuteService(engine, config, queries, queryPool, sessions);
            LineServer lineServer = new LineServer(queries, execute);
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
            Channel channel = bootstrap.bind(config.port()).sync().channel();
            return new GraphDbServer(config, store, catalog, queries, timeoutThread, queryPool, boss, worker, channel);
        } catch (Exception ex) {
            if (worker != null) {
                worker.shutdownGracefully();
            }
            if (boss != null) {
                boss.shutdownGracefully();
            }
            if (queryPool != null) {
                queryPool.shutdownNow();
            }
            if (timeoutThread != null) {
                timeoutThread.shutdownNow();
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
            channel.close().syncUninterruptibly();
            worker.shutdownGracefully().syncUninterruptibly();
            boss.shutdownGracefully().syncUninterruptibly();
            queryPool.shutdownNow();
            timeoutThread.shutdownNow();
            store.close();
            log.info("层岩图数据库已停止");
        } catch (RuntimeException ex) {
            log.warn("停止服务时出错：{}", ex.toString());
            store.close();
        }
    }
}
