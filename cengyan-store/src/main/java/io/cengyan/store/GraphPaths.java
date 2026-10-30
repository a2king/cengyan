package io.cengyan.store;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/**
 * 一个图的文件都放在 {@code <data>/<图名>/} 下面。
 * 每个目录是独立的 RocksDB，WAL 和刷盘文件也留在这个目录里。
 */
public final class GraphPaths {
    public static final String SYSTEM_DIRECTORY = "_system";
    /** 和 DuckDB 临时目录 {@code data/cache} 冲突，不能当作图名。 */
    public static final String RESERVED_GRAPH_NAME = "cache";

    public static final Set<Cf> CATALOG_FAMILIES = Set.of(Cf.DEFAULT, Cf.CATALOG);
    public static final Set<Cf> NODE_FAMILIES = Set.of(Cf.DEFAULT, Cf.VERTEX);
    public static final Set<Cf> EDGE_FAMILIES = Set.of(Cf.DEFAULT, Cf.EDGE, Cf.PROP, Cf.TXN);
    public static final Set<Cf> INDEX_FAMILIES = Set.of(Cf.DEFAULT, Cf.INDEX);

    private GraphPaths() {
    }

    public static Path system(Path dataDir) {
        return dataDir.resolve(SYSTEM_DIRECTORY);
    }

    public static Path graph(Path dataDir, String graphName) {
        return dataDir.resolve(graphName);
    }

    public static Path meta(Path dataDir, String graphName) {
        return graph(dataDir, graphName).resolve("meta");
    }

    public static Path node(Path dataDir, String graphName, String typeName, int part) {
        return graph(dataDir, graphName).resolve("data").resolve("node")
                .resolve(typeName).resolve(Integer.toString(part));
    }

    public static Path edge(Path dataDir, String graphName, String typeName, int part) {
        return graph(dataDir, graphName).resolve("data").resolve("edge")
                .resolve(typeName).resolve(Integer.toString(part));
    }

    /** 索引目录名是 {@code 节点名或边名-字段}，多个字段继续用 {@code -} 接上。 */
    public static String indexFolder(String owner, List<String> fields) {
        StringBuilder folder = new StringBuilder(owner);
        if (fields != null) {
            for (String field : fields) {
                folder.append('-').append(field);
            }
        }
        return folder.toString();
    }

    public static Path index(Path dataDir, String graphName, String indexFolder, int part) {
        return graph(dataDir, graphName).resolve("index").resolve(indexFolder).resolve(Integer.toString(part));
    }

    public static Set<Cf> families(Path path) {
        String text = path.toString().replace('\\', '/');
        if (text.endsWith("/" + SYSTEM_DIRECTORY) || text.endsWith("/meta")) {
            return CATALOG_FAMILIES;
        }
        if (text.contains("/data/node/")) {
            return NODE_FAMILIES;
        }
        if (text.contains("/data/edge/")) {
            return EDGE_FAMILIES;
        }
        if (text.contains("/index/")) {
            return INDEX_FAMILIES;
        }
        return Set.of(Cf.values());
    }

    /** 同一批写入的提交顺序：系统目录、图元数据、点边、索引。 */
    public static int writeOrder(Path path) {
        String text = path.toString().replace('\\', '/');
        if (text.endsWith("/" + SYSTEM_DIRECTORY)) {
            return 0;
        }
        if (text.endsWith("/meta")) {
            return 1;
        }
        if (text.contains("/index/")) {
            return 3;
        }
        return 2;
    }
}
