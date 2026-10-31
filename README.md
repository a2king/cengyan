# 层岩图数据库

单机图数据库。数据在本机 RocksDB 里，客户端用一行一条 JSON 连到默认端口 `19669`。

当前做到第 9 期：建图、插入、单跳和有界多跳、`SET` / `DELETE`、排序聚合、最短路、穿透持股、属性索引、登录和只读角色，以及分片、副本、计算节点。语法是手写解析，不是完整的 ISO GQL。新建的图默认 1 个分片、1 个副本，已有数据的分片号仍是 0。

每一类语句怎么写、和标准相比缺了什么，在 `docs/gql/`。先看 `docs/gql/00-和标准相比.md`。

## 先跑起来

需要 JDK 21。这台机器上的 JDK 21 在 `D:\workspace\code_env\jdk21`。系统默认的 `java` 如果是 1.8，先换过来，再在仓库根目录打包：

```powershell
chcp 65001
$env:JAVA_HOME = "D:\workspace\code_env\jdk21"
$env:Path = "$env:JAVA_HOME\bin;" + $env:Path
mvn "-DskipTests" package
```

如果要生成给新用户直接解压部署的发布包，仓库根目录执行：

```powershell
# Windows
$env:JAVA_HOME = "D:\workspace\code_env\jdk21"
$env:Path = "$env:JAVA_HOME\bin;" + $env:Path
.\build-dist.ps1
```

```bash
# Linux
export JAVA_HOME=/path/to/jdk-21
export PATH="$JAVA_HOME/bin:$PATH"
bash ./build-dist.sh
```

脚本只构建并打包 `cengyan-server` 和 `cengyan-studio`，会在 `dist/` 下同时生成发布目录和压缩包。发布包内的 JAR、`bin/` 启动命令、`config/` 配置样例、数据目录和日志目录已经准备好，解压后运行 `bin/start-all.bat`（Windows）或 `./bin/start-all.sh`（Linux）即可。所有打包和启动命令都会检查 Java 主版本，检测到不是 Java 21 时会中断并说明原因。

Linux 服务器如果没有把 JDK 21 配到系统环境变量，可以直接编辑发布包里的 `bin/java.env`：

```bash
export JAVA_HOME=/opt/jdk-21
```

启动脚本会自动读取这个文件。Shell 中设置环境变量时写成 `export JAVA_HOME=/opt/jdk-21`，不要写成 `export $JAVA_HOME=...`。

开一个终端启动服务。配置文件要在当前目录，所以就在仓库根目录执行：

```powershell
java -jar cengyan-server\target\cengyan-server-1.0.0.jar cengyan.properties
```

看到日志里的 `0.0.0.0:19669` 就可以连。内网另一台机器用这台电脑的局域网 IP，不要写 `0.0.0.0`。数据写在 `data\`，日志打在这个终端。`Ctrl+C` 会关库。Windows 防火墙若拦截入站，需要放行这个端口。

再开一个终端启动客户端：

```powershell
java -jar cengyan-cli\target\cengyan-cli-1.0.0.jar
```

`--host` 和 `--port` 可改连接地址，默认 `127.0.0.1:19669`。交互模式里输入 `exit` 或 `quit` 退出。不要用「启动一次只发一条命令」的方式连续操作：每次新进程都不记得上次的登录。

浏览器控制台另开一个终端：

```powershell
java -jar cengyan-studio\target\cengyan-studio-1.0.0.jar
```

打开 `http://127.0.0.1:19670`。控制台把请求转到本机图库 `19669`。`--host`、`--port`、`--db-host`、`--db-port` 可改；部署包的 `config/studio.properties` 默认使用 `host=0.0.0.0`，内网用户可以通过服务器 IP 访问。登录用图库用户（仓库默认 `root` / `cengyan`），不是独立 Studio 账号。

Studio 前端源码在 `cengyan-studio-web`，和 Java 转发服务分开维护。开发前端时：

```powershell
cd cengyan-studio-web
npm install
npm run dev
```

生产构建会把静态资源写回 `cengyan-studio/src/main/resources/studio`，随后重新打包 `cengyan-studio` 即可随 Java Studio 一起部署：

```powershell
npm run typecheck
npm run build
```

页面使用 Ant Design 组织工作台控件，Monaco Editor 承载 GQL 编辑器，React Flow 承载查询结果的点边关系图。图模式先进入图空间列表，打开空间后再切换点类型、边类型、索引和统计信息；GQL 页右上角可以独立切换当前图空间。

## 第一段查询

空库第一次启动会创建读写用户 `root`，密码是配置里的 `root_password`，仓库自带配置是 `cengyan`。先登录，再写语句。一次一行，不要用分号把多条拼在一起。

```text
:login root cengyan
CREATE GRAPH Company
USE Company
CREATE NODE TYPE Company ID INT (name STRING NULL)
CREATE REL TYPE holder (ratio DOUBLE NULL)
INSERT (a:Company {id:1, name:'root'})-[:holder {ratio:0.5}]->(b:Company {id:2, name:'sub'})
MATCH (a:Company {id:1})-[e:holder]->(b) RETURN b.id, e.ratio
```

最后一行应返回 `b.id = 2`、`e.ratio = 0.5`。

名字只能是英文字母、数字、下划线，不能以数字或下划线开头，不能用中文，最长 128 字节。关键字不区分大小写，图名、类型名、属性名区分大小写。`id` 是点的编号，不是属性列。

节点 ID 可以按类型定义为非负整数或字符串。字符串 ID 需要指定 UTF-8 最大字节长度；旧的 `CREATE NODE TYPE Company (...)` 写法等同于 `ID INT`。

```text
CREATE NODE TYPE Company ID INT (name STRING NULL)
CREATE NODE TYPE Person ID STRING(64) (name STRING NULL)
```

字符串 ID 写入和查询时使用引号，例如 `id:'person-001'`。`SHOW NODE TYPES` 会返回 `idType` 和 `idLength` 两列。

## 点和边怎么写

带类型的点模式会把点写进去。只写 `({id:1})` 时只写边，不写点。

```text
INSERT (a:Company {id:3, name:'alone'})
INSERT (a {id:1})-[:holder {ratio:0.2}]->(b {id:3})
```

箭头是逻辑方向。`(a)-[:holder]->(b)` 里 a 是起点、b 是终点。`(a)<-[:holder]-(b)` 则起点是 b、终点是 a。插入边必须有方向，也不能一次插入多跳。

同一对点可以有多条边，用 `@` 后面的 rank 区分，不写就是 0：

```text
INSERT (a {id:1})-[:holder@3 {ratio:0.1}]->(b {id:2})
```

改和删都从 `MATCH` 开头，并且只支持一跳。删点可以不写边：

```text
MATCH (a:Company {id:1})-[e:holder]->(b {id:2}) SET e.ratio = 0.8, b.name = 'child'
MATCH (a {id:3}) DELETE a
```

`RETURN b` 返回整个点，形如 `{"id":2,"type":"Company","properties":{...}}`。`RETURN e` 返回整条边，形如 `{"type":"holder","rank":0,"src":1,"dst":2,"properties":{...}}`。

可以只查点，不写边。边上不写类型表示任何关系；用 `|` 列出几种类型，每个名字前面的冒号可以省略：

```text
MATCH (a:Company {id:1}) RETURN a
MATCH (a:Company {id:1})-[e]->(b) RETURN e
MATCH (a:Company {id:1})-[e:holder|:works]->(b) RETURN b.id
```

不写类型或一次写多种类型时，这个点上没被选中的边也算进每点上限。插入边仍然只能写一个类型。

属性类型是 `BOOL`、`INT64`、`DOUBLE`、`STRING`、`DATETIME`。建类型后只能在末尾加可空列：

```text
ALTER NODE TYPE Company ADD founded DATETIME NULL
```

时间字面量写成 `DATETIME '2024-01-31T12:00:00Z'`。不带时区的 `DATETIME '2024-01-31 12:00:00'` 按 UTC。返回值是 ISO 字符串。

## Spark 初始化脚本

`dev-scripts/data_init_node.py` 和 `dev-scripts/data_init_edge.py` 都在 `main` 中创建 `SparkSession`，执行 `spark.sql`，再用 `foreachPartition` 复用一个 Cengyan 会话逐条写入。脚本只依赖随目录提供的 `cengyan.py`，不需要安装 Nebula 客户端。

节点导入示例：

```bash
spark-submit --py-files dev-scripts/cengyan.py dev-scripts/data_init_node.py \
  --sql "select id, name from company_source" \
  --host 127.0.0.1 --port 19669 --user root --password cengyan \
  --space Company --node-type Company --id-field id \
  --id-type STRING --id-length 64 --property-fields name --batch 512
```

边导入示例：

```bash
spark-submit --py-files dev-scripts/cengyan.py dev-scripts/data_init_edge.py \
  --sql "select src_id, dst_id, ratio from holding_source" \
  --host 127.0.0.1 --port 19669 --user root --password cengyan \
  --space Company --edge-type holder --src-field src_id --dst-field dst_id \
  --id-type STRING --id-length 64 --property-fields ratio --batch 512
```

`--id-type INT` 是默认值；使用 `STRING` 时脚本会为 ID 自动加引号并转义单引号，`--id-length` 按 UTF-8 字节校验，需与节点类型定义一致。

当前初始化脚本使用服务端批量接口。`--batch` 表示一次 `bulkInsert` 的行数，默认 512，服务端单批最多 10000 行、请求体最多 16 MiB；它不再为每行发一个 GQL 请求。

批量接口的 JSON 形状如下：

```json
{
  "op": "bulkInsert",
  "session": "...",
  "kind": "node",
  "type": "Person",
  "rows": [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}
  ]
}
```

边批次使用 `src`、`dst`、可选 `rank` 和属性列：

```json
{
  "op": "bulkInsert",
  "kind": "edge",
  "type": "knows",
  "rows": [
    {"src": 1, "dst": 2, "rank": 0, "score": 0.5}
  ]
}
```

节点批次在服务端对应一次 RocksDB `WriteBatch`。字符串 ID 映射、节点属性和索引也在同一个批次中提交；任意一行校验失败时整批失败。同一分片的边同样一次提交。跨分片的边先写出边、属性和补偿标记，再写入边，最后删除标记。

10 亿级数据不能放进一个请求，应该由 Spark 分区或外部导入器切成 512～10000 行的批次，并通过多个分区并行提交。批量边可以连接不同分片上的端点。

## 目录

```text
SHOW GRAPHS
SHOW ALIASES
SHOW NODE TYPES
SHOW REL TYPES
DESCRIBE NODE TYPE Company
DESCRIBE REL TYPE holder
CREATE ALIAS corp FOR GRAPH Company
DROP ALIAS corp
DROP GRAPH Company
```

`USE` 之后的语句都落在当前图。别名指向图，不能指向另一个别名。`DROP GRAPH` 只能删图，不能拿来删别名。

## 多跳、最短路、子图

变长跳写在箭头后面。`{3}` 是正好第 3 跳的点，`{1,3}` 是第 1 到第 3 跳新到达的点，每个点只出现一次。

```text
MATCH (a:Company {id:1})-[:holder]->{1,3}(b) RETURN b.id
CALL path.shortest({start: 1, end: 2, rel: "holder", hops: 10}) YIELD length, vids
CALL subgraph.bounded({start: 1, rel: "holder", hops: 3}) YIELD vid
CALL ownership.largest_holder({start: 1, rel: "holder", ratioProp: "ratio", hops: 10}) YIELD vid, ratio
```

多跳 `MATCH` 的 `WHERE` 目前只能比较边的 `rank` 和终点 `id`。`*` 表示所有路径，会直接报语法错误。

子图从起点沿这种边的出边和入边一起扩，结果包含起点，只返回 `vid`。`hops: 0` 只返回起点。


## 索引

```text
CREATE INDEX by_name ON NODE TYPE Company (name)
CREATE UNIQUE INDEX by_ratio ON REL TYPE holder (ratio)
SHOW INDEXES
DROP INDEX by_name
:rebuildIndex by_name
:stats
```

`CREATE INDEX` 只登记目录，此时查询还不会用它。执行 `:rebuildIndex 索引名`，`SHOW INDEXES` 里 `ready` 变成 true 之后，等值和数值范围才会走索引。索引建好之前的新写入仍会维护索引键。

字符串只支持等值，范围条件不会走索引。`INT64`、`DOUBLE`、`DATETIME` 可以做范围。`NULL` 不进索引，多条 `NULL` 不算冲突。唯一索引冲突时报 `UNIQUE_VIOLATION`。`:stats` 需要先 `USE`，返回每种点、每种边的数量和最大度数。

## 用户和角色

用户管理不写进 GQL，用冒号命令。角色只有 `read` 和 `write`，作用范围是整个进程，不是单张图。

```text
:createUser reader secret read
:setPassword reader next
:setRole reader write
:dropUser reader
```

只读用户可以 `USE`、`SHOW`、`DESCRIBE`、`MATCH`、`CALL` 和 `:stats`。`INSERT`、`SET`、`DELETE`、`CREATE`、`DROP`、`ALTER`，以及重建索引、刷盘、压缩和用户管理，会返回 `FORBIDDEN`。最后一个读写用户不能删除，也不能改成只读。密码不以明文保存。会话只在内存里，空闲 8 小时删除，重启服务后要重新登录，图数据还在。

## 运维命令

| 输入 | 作用 |
|---|---|
| `ping` 或 `:ping` | 看连接是否通，不用登录 |
| `:login 用户 密码` | 登录，并记住这次会话 |
| `:flush` | 把内存表刷到磁盘 |
| `:compact` | 压缩全部列族。查询可以继续按快照读 |
| `:rebuildIndex 索引名` | 重建索引并标记为可用 |
| `:stats` | 按类型看点数、边数和最大度数 |
| `:createUser 用户 密码 read\|write` | 建用户 |
| `:setPassword 用户 密码` | 改密码 |
| `:setRole 用户 read\|write` | 改角色 |
| `:dropUser 用户` | 删用户 |
| `:setPartCount 图 分片数` | 按新的分片数重写已有键。1 个分片时分片号仍是 0 |
| `:addStorage 主机:端口` | 加入空的存储节点，并开始搬分片号最小的那一个 |
| `:drainStorage 节点号` | 存储节点开始下线。它若是某个分片的唯一投票成员，返回 `NODE_NOT_DRAINED` |
| `:removeStorage 节点号` | 已经下线且不再持有分片时移除 |
| `:addReplica 图 副本数` | 把图加到这么多投票副本。同步期间写入返回 `SPACE_READONLY`，查询继续 |
| `:addCompute 主机:端口` | 健康检查通过后加入计算节点。计算节点不存点和边 |
| `:drainCompute 节点号` | 不再把新会话分到这台 |
| `:removeCompute 节点号` | 没有绑定会话、也没有正在执行的查询时才能移除 |
| `:showMembers 图` | 只列出投票成员。学习者不显示 |

创建图时副本数只能是 1。先把数据灌完，再 `:addReplica`。跨分片的边会先写出边再写入边；补偿还没做完时，查询可能只看见出边。有 3 个投票成员以后，写入要多数派确认才返回成功，一台机器坏掉仍然可以写。计算节点只负责承接会话：新会话去正在执行的查询更少的那台，同一会话的下一条语句还留在那里。

## 自己写客户端

TCP，一行一条 JSON，UTF-8，以换行结束。成功和失败都看 `code`。失败响应没有半截 `rows`。`message` 是中文说明。

```json
{"op":"ping"}
{"op":"login","user":"root","password":"cengyan"}
{"op":"execute","session":"上一次返回的 session","query":"RETURN 1","params":{}}
{"op":"admin","session":"...","command":"rebuildIndex","index":"by_name"}
{"op":"admin","session":"...","command":"createUser","user":"reader","password":"secret","role":"read"}
{"op":"cancel","queryId":"要取消的查询"}
```

`login`、`execute`、`admin` 成功时大致是：

```json
{"code":"OK","session":"...","queryId":"...","columns":["user","role"],"rows":[["root","write"]],"rowsAffected":0}
```

没登录就执行，会得到 `{"code":"FORBIDDEN","message":"请先登录",...}`。把响应里的 `session` 原样带回去。客户端自带的交互模式会自动保存。

查询参数放在 `params` 里，语句里用 `$名字`。交互客户端发出的 `params` 是空对象，所以在客户端里直接写字面量。

## 配置

`cengyan.properties` 在启动时读完，运行中不热更新。仓库里的块缓存是开发机的 256MB。32GB 机器按计划书把 `block_cache_bytes` 改成 `8589934592`。

| 项 | 仓库里的值 | 含义 |
|---|---|---|
| `host` | `0.0.0.0` | 监听地址。`0.0.0.0` 是所有网卡，`127.0.0.1` 只本机 |
| `port` | `19669` | 监听端口 |
| `data_dir` | `data` | 数据目录，相对启动时的工作目录 |
| `root_password` | `cengyan` | 只在还没有任何用户时，用来创建 `root` |
| `query_threads` | `16` | 交互查询线程数。登录、SHOW、USE、DESCRIBE 和普通语句走这里 |
| `interactive_reserved` | `2` | 上面的名额里留给登录和元数据的条数，必须小于 `query_threads` |
| `heavy_query_threads` | `2` | 统计、存储扫描、重建索引、批量导入使用的线程数，不占用交互线程 |
| `query_queue_ms` | `30000` | 等一条空闲线程的最长时间，超时返回 `BUSY` |
| `heavy_max_execution_time_ms` | `0` | 重查询的硬超时。`0` 表示不再额外限制 |
| `max_execution_time_ms` | `0` | 一条普通语句的服务端超时，`0` 表示不限 |
| `max_edges_per_vertex` | `0` | 每点扫描边数预算，`0` 表示不限 |
| `max_frontier_per_hop` | `0` | 每跳新点预算，`0` 表示不限 |
| `max_hops` | `0` | 多跳、最短路、子图的跳数预算，`0` 表示不限 |
| `max_paths` | `0` | 路径结果预算，`0` 表示不限 |
| `query_memory_bytes` | `67108864` | 单条查询的内存预算 |
| `block_cache_bytes` | `268435456` | RocksDB 块缓存 |
| `duckdb_memory` | `1GB` | 排序和聚合使用的 DuckDB 内存 |
| `duckdb_temp_directory` | `tmp/duck` | DuckDB 临时目录 |
| ocksdb_background_jobs | 6 | RocksDB background flush/compaction jobs |
| ocksdb_write_buffer_bytes | 134217728 | MemTable write buffer per Column Family, in bytes |

这些预算是执行策略，不是 GQL 语义。默认值为 `0`，不会因为固定的 64 条边、10 跳或 100 条路径拒绝查询；不受信任的部署可以显式设置正数。

## 测试

```powershell
mvn test
```

性能测试记在 `docs/perf-report.md`。
