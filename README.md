# 层岩图数据库

单机图数据库。数据在本机 RocksDB 里，客户端用一行一条 JSON 连到默认端口 `19669`。

当前做到第 8 期：建图、插入、单跳和有界多跳、`SET` / `DELETE`、排序聚合、最短路、穿透持股、属性索引、登录和只读角色。分片、副本、计算节点还没做。语法是手写解析，不是完整的 ISO GQL。

## 先跑起来

需要 JDK 21。这台机器上的 JDK 21 在 `D:\workspace\code_env\jdk21`。系统默认的 `java` 如果是 1.8，先换过来，再在仓库根目录打包：

```powershell
chcp 65001
$env:JAVA_HOME = "D:\workspace\code_env\jdk21"
$env:Path = "$env:JAVA_HOME\bin;" + $env:Path
mvn "-DskipTests" package
```

开一个终端启动服务。配置文件要在当前目录，所以就在仓库根目录执行：

```powershell
java -jar cengyan-server\target\cengyan-server-1.0.0-SNAPSHOT.jar cengyan.properties
```

看到日志里的端口 `19669` 就可以连。数据写在 `data\`，日志打在这个终端。`Ctrl+C` 会关库。

再开一个终端启动客户端：

```powershell
java -jar cengyan-cli\target\cengyan-cli-1.0.0-SNAPSHOT.jar
```

`--host` 和 `--port` 可改连接地址，默认 `127.0.0.1:19669`。交互模式里输入 `exit` 或 `quit` 退出。不要用「启动一次只发一条命令」的方式连续操作：每次新进程都不记得上次的登录。

## 第一段查询

空库第一次启动会创建读写用户 `root`，密码是配置里的 `root_password`，仓库自带配置是 `cengyan`。先登录，再写语句。一次一行，不要用分号把多条拼在一起。

```text
:login root cengyan
CREATE GRAPH Company
USE Company
CREATE NODE TYPE Company (name STRING NULL)
CREATE REL TYPE holder (ratio DOUBLE NULL)
INSERT (a:Company {id:1, name:'root'})-[:holder {ratio:0.5}]->(b:Company {id:2, name:'sub'})
MATCH (a:Company {id:1})-[e:holder]->(b) RETURN b.id, e.ratio
```

最后一行应返回 `b.id = 2`、`e.ratio = 0.5`。

名字只能是英文字母、数字、下划线，不能以数字或下划线开头，不能用中文，最长 128 字节。关键字不区分大小写，图名、类型名、属性名区分大小写。`id` 是点的编号，不是属性列。

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

当前的 `MATCH ... RETURN` 必须带一条边。只插入、没有边的点，这条语句读不回来。

属性类型是 `BOOL`、`INT64`、`DOUBLE`、`STRING`、`DATETIME`。建类型后只能在末尾加可空列：

```text
ALTER NODE TYPE Company ADD founded DATETIME NULL
```

时间字面量写成 `DATETIME '2024-01-31T12:00:00Z'`。不带时区的 `DATETIME '2024-01-31 12:00:00'` 按 UTC。返回值是 ISO 字符串。

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

穿透持股沿出边走。单条比例低于 `min_edge_ratio`（默认 `0.01`），或累乘后低于 `min_accum_ratio`（默认 `0.001`），就不再往下走。环不会把同一段比例再乘一次。

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
| `port` | `19669` | 监听端口 |
| `data_dir` | `data` | 数据目录，相对启动时的工作目录 |
| `root_password` | `cengyan` | 只在还没有任何用户时，用来创建 `root` |
| `query_threads` | `16` | 查询线程数 |
| `max_execution_time_ms` | `1000` | 一条语句默认最多跑 1 秒 |
| `max_edges_per_vertex` | `64` | 从一个点扫边的上限，超过返回 `TRUNCATED` |
| `max_frontier_per_hop` | `64` | 一跳里新点的上限，超过返回 `FRONTIER_EXCEEDED` |
| `max_hops` | `10` | 多跳、最短路、子图的跳数上限 |
| `max_paths` | `100` | 路径条数上限 |
| `query_memory_bytes` | `67108864` | 单条查询的内存预算 |
| `block_cache_bytes` | `268435456` | RocksDB 块缓存 |
| `duckdb_memory` | `1GB` | 排序和聚合使用的 DuckDB 内存 |
| `duckdb_temp_directory` | `tmp/duck` | DuckDB 临时目录 |
| `min_edge_ratio` | `0.01` | 穿透持股单条边的最低比例 |
| `min_accum_ratio` | `0.001` | 穿透持股累乘后的最低比例 |

一个点上写出超过 64 条边可以成功，但默认配置下再去 `MATCH` 这个点会因为扫边超过上限而失败，不是悄悄少返回几行。

## 测试

```powershell
mvn test
```

性能测试记在 `docs/perf-report.md`。分片、副本和计算节点还没做。
