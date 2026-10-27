# 图数据库重构计划书

对照这份 Nebula 仓库的存储、计算、查询，在 Java 21 上重做。查询语言用 ISO/IEC 39075:2024 GQL 的核心子集，不移植 nGQL 语法。存储用嵌入式 RocksDB。第 1 期只在单机上测试。分片、副本和计算节点的规则写在第 7 节第 2 期，单机测试不实现。

已经定下的约束：

- 一跳、热缓存、返回大约 100 条以内，接口 p99 目标 10 毫秒。
- 常见查询是 10 跳，每个点大约 10 到 40 条边。全部扩出去在第 3 到第 4 跳就会超过 10 毫秒，C++ 也一样。10 跳要进毫秒，每一跳留下的边必须收在几十条。
- 超时、断连、手动终止走同一个取消函数，并停掉正在进行的前缀扫描。
- 过滤、聚合、排序交给进程内 DuckDB，并设置内存上限和临时目录。多跳扩展仍由自己的执行器做。
- 块缓存放在 RocksDB 堆外。Java 堆不拿来堆路径对象。

本文前半是和 Nebula 的对照。拿到新仓库里从零写代码时，以第 10 节为准，不要再打开这份源码。第 9 节的路径只是可选对照。

图空间别名如果还要补在正在跑的 Nebula 上，按第 4.6 节和第 7 节的「生产先行」。新项目自己的别名按第 10.5 节，放在第 2 期。

---

## 1. Nebula 现在怎么分层

三个进程，Thrift RPC 互相调用。

| 进程 | 代码 | 职责 |
|---|---|---|
| metad | `src/meta` | 集群唯一的目录：图空间、分片、schema、索引、用户、会话、配置、Job。自己也是一套 Raft + RocksDB。 |
| storaged | `src/storage` + `src/kvstore` | 按分片存点边。Raft 复制。查询时在本机扫 RocksDB，把结果装成 DataSet 再返回。 |
| graphd | `src/graph` + `src/parser` | 解析 nGQL，生成执行计划，按分片把请求发到 storaged，在内存里做跳、路径、Join、聚合。 |

一次 `GO` / `MATCH` 的路径：

1. graphd 解析、校验、计划、可选优化。
2. 按起点的 vid 算出 partId，RPC 到该 part 的 leader。
3. storaged 用前缀迭代器扫边，属性在扫描时就解码进行。
4. 全部 vid 扫完才把 DataSet 放进响应。
5. graphd 等齐所有 storaged，把邻接放进哈希表，再决定下一跳。变长路径要等所有跳结束才拼路径。

单机嵌入式要把第 2 步到第 4 步收成进程内方法调用。计算层不再等一份完整 DataSet 才开始下一条边的过滤。

---

## 2. 存储：Nebula 有什么

### 2.1 键

实现在 `src/common/utils/NebulaKeyUtils.h`。vid 在 2.0 之后是定长字节，长度由图空间决定。partId 占 3 字节，用来把键散到分片。

| 数据 | 键 |
|---|---|
| 点 | `type(1) + partId(3) + vid + tagId(4)` |
| 边 | `type(1) + partId(3) + src + edgeType(4) + rank(8) + dst + placeholder(1)` |
| 系统 | part 提交、balance、kv 名字空间，另有前缀 |

`edgeType` 为正表示出边，为负表示入边。同一个点的出边是一次前缀扫描。`rank` 允许同一对点之间有多条边。

值是 schema 版本加上按列编码的属性（`src/codec`）。扫一条边就会把属性值读出来。这是超级点和大属性查询费内存的直接原因。

索引键在 `IndexKeyUtils`：索引号、属性编码、点或边的身份。索引与数据不是同一条键。

### 2.2 storaged 上的请求

| 能力 | 代码 | 行为 |
|---|---|---|
| 取邻居 | `GetNeighborsProcessor` | 每个起点一行，边上的属性收进嵌套 List。`limit < 0` 时用 `max_edge_returned_per_vertex`，默认 `INT_MAX`。 |
| 只取终点 | `GetDstBySrcProcessor` | 简单 GO 的轻量路径，仍然受上面的旗标限制。 |
| 取点属性 / 边属性 | `GetPropProcessor` | 已知身份后取属性。 |
| 扫描点 / 边 | `ScanVertexProcessor`、`ScanEdgeProcessor` | 带 cursor 的范围扫，一次 RPC 仍把本批行放进内存。 |
| 索引查找 | `LookupProcessor` | 按索引前缀或范围找身份，再回表。 |
| 写点 | `AddVerticesProcessor` | 按 tag 写点键。 |
| 写边 | `AddEdgesProcessor`，以及 `storage/transaction` 下整套 Chain | 一条逻辑边要写出边和入边。两边落在不同分片时用 TOSS：本地、远端、prime、double prime，失败再补偿。 |
| 改点 / 改边 | `UpdateVertexProcessor`、`UpdateEdgeProcessor` | 读旧值、改列、写回。 |
| 删点 / 删 tag / 删边 | `DeleteVerticesProcessor`、`DeleteTagsProcessor`、`DeleteEdgesProcessor` | 删点可以带边，实现上是先把边查回来再删。 |
| 管理 | checkpoint、清 space、admin task、leader | 给备份、Job、Raft 用。 |
| 通用 KV | `kv/PutProcessor` 等 | 内部使用，不是图查询接口。 |

`query_concurrently` 为真时，一个 GetNeighbors 按 part 并行，每 part 一份 DataSet，合并前同时占内存。

### 2.3 复制与分片

`src/kvstore/raftex`：选主、日志、快照、成员变更。一个 part 一个 Raft 组。写入走 leader。读在 Nebula 里多数也走 leader。

分片由 metad 分配。vid 哈希到 partId，再编码进键。迁移是 `DATA_BALANCE` Job：把 part 从一台 storaged 拷到另一台。

单机没有这部分。键里仍建议预留分区号，避免以后加分片时改键格式。

### 2.4 目录（metad）

| 领域 | 处理器目录 | 用户能做的事 |
|---|---|---|
| 图空间 | `processors/parts` | 创建、克隆、删除、修改空间；vid 类型和长度、副本数、分片数。 |
| Schema | `processors/schema` | Tag、Edge 的创建、修改、删除、列出。列有类型、默认值、可空、TTL。 |
| 索引 | `processors/index` | Tag 索引、Edge 索引的创建、删除、查看。 |
| 全文 | `processors/index/FTIndexProcessor` | Elasticsearch 外部全文索引。 |
| 主机与 Zone | `processors/zone` | 加减主机、Zone 合并切分、按 Zone 放置副本。 |
| 用户 | `processors/user` | 用户、密码、角色、授权。 |
| 会话 | `processors/session` | 会话登记、`KILL` 状态。 |
| 配置 | `processors/config` | 集群可改的 gflag。 |
| Job | `processors/job` | 见下表。 |
| 快照备份 | `processors/admin` | snapshot、backup、restore。 |
| 心跳 | `HBProcessor` | storaged / graphd 上报，顺带拉 schema 和 killed plan。 |

Job 类型（`src/interface/meta.thrift` 的 `JobType`）：

| Job | 作用 | 单机是否还需要 |
|---|---|---|
| COMPACT | RocksDB 压缩 | 要，做成管理命令 |
| FLUSH | memtable 落盘 | 要 |
| REBUILD_TAG_INDEX / REBUILD_EDGE_INDEX | 全量重建索引 | 要 |
| STATS | 点数、边数、大致度数 | 要，超级点阈值依赖它 |
| REBUILD_FULLTEXT_INDEX | 重建全文 | 第一期不做 |
| DATA_BALANCE / LEADER_BALANCE / ZONE_BALANCE | 搬分片、平衡 leader | 有集群再做 |
| DOWNLOAD / INGEST | 把外部 SST 灌进来 | 有集群再做 |

### 2.5 存储层在新系统里怎么落

进程内一个 `GraphStore`，不要先写 Raft。

列族：

| 列族 | 键 | 值 |
|---|---|---|
| `meta` | schema 版本、类型、序列 | 小记录 |
| `vertex` | `graphId + partId + vid` | 标签位图；很小的属性可以内联 |
| `edge` | `graphId + partId + src + typeId + direction + rank + dst` | 空，或只有拓扑需要的几个字节 |
| `prop` | 点或边的身份 + schema 版本 | 编码后的属性 |
| `index` | `indexId + 属性编码 + 身份` | 空 |

`direction` 只有出、入两值。插入一条边写两条 `edge` 键，一次 `WriteBatch` 提交。扩展任意一侧都是前缀扫描。这和 Nebula 用正负 `edgeType` 是同一件事，只是方向显式放在键里。

`partId` 第一期恒为 0，占位 2 字节。`vid` 用内部 8 字节整数。业务主键走唯一索引，不把变长字符串直接当扫描键。列族名和每个字节的位置以第 10.2 节为准：目录列族叫 `catalog`，边属性列族叫 `prop`。

扫边接口必须带上限和取消令牌：

```text
scanEdges(graph, src, type, direction, limit, cancelToken) -> iterator
multiGetProps(identities) -> values
```

迭代器每 64 条边看一次 `cancelToken`。达到 `limit` 就停，并在结果上标记「被截断」，不要静默当成完整邻居。Nebula 现在是静默成功。

### 2.6 双向查询从第 1 期就保留

RocksDB 不能把一条键倒着扫。Nebula 的做法是插入时写两条边键，查询时两个方向都做前缀扫描。这个能力保留，不改成「只存出边、入边靠全表过滤」。

插入 A 到 B、类型 `hold`、rank 0 时，`edge` 列族里有两条键：

| 方向 | 键里的 src | direction | dst | 用来回答 |
|---|---|---|---|---|
| 出边 | A | 出 | B | 从 A 沿 `hold` 往外走 |
| 入边 | B | 入 | A | 从 B 沿 `hold` 往里找股东 |

`scanEdges(A, hold, 出)` 和 `scanEdges(B, hold, 入)` 各是一次 `Seek` 加向后扫。出边前缀不得匹配到入边，第 1 期的往返测试要锁住这一点。

和 Nebula 的差别只有属性放哪。Nebula 把同一份边属性编码进两条边的值里。这里两条键只表示拓扑，属性在 `prop` 里存一份，键用边的逻辑身份（类型、rank、两端点、与方向无关）。从入边扫到终点后，再用这个身份 `multiGetProps`。不要为了双向再复制一份持股比例。

第 3 期的 GQL 要同时接受正向、反向和无向的一步模式：`(a)-[r:hold]->(b)`、`(a)<-[r:hold]-(b)`、`(a)-[r:hold]-(b)`。反向模式走入边前缀，不能先查出边再在内存里反转。第 4 期每一跳都带方向，反向多跳同样受每跳上限约束。

删除和更新必须两条拓扑键一起动，否则一边还能扫到、另一边已经没了。这和第 4.2 节的同一个 `WriteBatch` 是一件事。

---

## 3. 计算：Nebula 有什么

计算几乎全在 graphd。storaged 只做「一个请求里的扫描 + 过滤下推 + 收成 DataSet」。

### 3.1 一次查询的对象

| 对象 | 代码 | 存活时间 |
|---|---|---|
| `QueryInstance` | `src/graph/service/QueryInstance.cpp` | 堆上单独 new。跑完才 `delete this`。客户端断开不会删它。 |
| `QueryContext` | `src/graph/context` | 表达式、计划、`killed_` 标志。 |
| `ExecutionContext` | 同目录 | 每个变量一份 `Result` / `DataSet`。 |
| 执行器 | `src/graph/executor` | 多跳的邻接表是执行器成员，活过每一跳 RPC。 |

调度器 `AsyncMsgNotifyBasedScheduler`：算子 `open()` 时看 `killed_` 和内存水位，然后 `execute()`。已经在等 RPC 的算子不会被打断。

### 3.2 执行器

查询和算法：

| 执行器 | 作用 | 新系统 |
|---|---|---|
| `GetNeighbors` / `GetVertices` / `GetEdges` | 向存储取数 | 收成 `GraphStore` 的方法 |
| `Expand` / `ExpandAll` | GO 的多跳。没写 LIMIT 时，发给存储的 limit 是 `int64` 最大值 | 删除 GO。多跳只留一种 Expand |
| `Traverse` | MATCH 变长路径。中间跳 limit 为 -1，最后才拼路径 | 改成按跳产出，并带每跳上限 |
| `AllPaths` / 各类 `ShortestPath` | 全路径、单对、多对、BFS 最短路 | 第一期只做有上限的最短路。全路径默认关闭 |
| `Subgraph` | 子图，点只扩一次，边仍全取 | 第二期，且每点有边数上限 |
| `IndexScan` / `FulltextIndexScan` | LOOKUP、全文 | 属性索引第二期。全文不做 |
| `Filter` `Project` `Aggregate` `Sort` `TopN` `Limit` `Dedup` | 关系算子，输入输出都是整份 DataSet | 交给 DuckDB |
| `InnerJoin` `LeftJoin` `Intersect` `Minus` `Union` | 集合和连接 | DuckDB |
| `Unwind` `PatternApply` `RollUpApply` | 列表展开、模式相关子查询 | GQL 子集稳定后再做 |
| `Sample` | 蓄水池。存储侧仍会扫完该点的边 | 保留语法能力，但截断优先于抽样 |
| `Loop` `Select` | 多跳的控制流 | Expand 内部循环，不暴露给计划 |

管理类执行器（`executor/admin`、`maintain`）对应第 4 节的语句，多数第一期不做。

### 3.3 优化器

`src/graph/optimizer/rule`，大约 50 条规则。`enable_optimizer` 在代码里默认关闭，配置文件里默认打开。和正确性、性能相关、新系统必须有的只有几类：

- 过滤条件下推到扫描和扩展（`PushFilterDownGetNbrs`、`PushEFilterDown`、`PushFilterDownTraverse`）。Nebula 对变长路径的边上条件经常推不进存储。
- LIMIT 下推（`PushLimitDownGetNeighbors`、`PushLimitDownExpandAll`）。很多计划形状推不下去。
- 索引选择（`IndexScanRule`、`OptimizeTagIndexScanByFilter`）。
- 无用 Project、AppendVertices 的消除。

地理索引、全文 LIMIT 合并，第一期不移植。

新系统的计划重写就做这四件：谓词能下推就下推、每跳条数写成存储 limit、能用索引就不扫全图、投影掉用不到的属性。不做通用代价优化器。

### 3.4 内存

`src/common/memory/MemoryTracker.h` 是进程级计数，不是按查询。超过约 80% 物理内存，当前线程抛 `bad_alloc`，这条查询失败。已经分配的表要等查询结束才释放。没有算子落盘。

`Value` 的字符串、List、Path 是深拷贝。点、边对象引用计数。多跳时 `adjList_` 保留每一跳的边，`buildPath` 再按「已有路径 × 本跳边」拷贝。

新系统：

- 扩展阶段只保留「本跳的 vid 列表」和取消令牌。不在 Java 里攒完整路径表，除非 `RETURN` 要求路径值，并且条数已经在上限内。
- DuckDB 连接设置 `memory_limit` 和 `temp_directory`。聚合和排序超出后落盘。
- 单条查询还有自己的字节预算。超预算即取消，不等到整机 80%。

### 3.5 现在的取消，为什么不够

| 机制 | 实际效果 |
|---|---|
| 客户端超时 | 服务端不知道。`QueryInstance` 继续跑。 |
| `storage_client_timeout_ms`（默认 60 秒） | 只让 graphd 不再等。storaged 继续扫完。 |
| `KILL QUERY` | 把 `killed_` 设上，并经 meta 心跳告诉 storaged。storaged 默认每 256 条边才看一次，而且缓存可能是旧的。 |
| 算子 `open()` | 只在算子开始时看 `killed_`。`TraverseExecutor` 的多跳循环中间不看。 |
| 会话空闲 8 小时 | 跟单条查询无关。 |

新系统一个 `QueryHandle.cancel(reason)`：

1. 置上 `cancelled`。
2. 客户端还在等，就用「超时」或「被终止」结束本次调用。
3. `scanEdges` 在迭代中看到令牌就返回。
4. DuckDB 的 statement 同时 interrupt。
5. `KILL` 语句、连接断开、服务端 `max_execution_time_ms`、请求里带上的 `timeout_ms`，都只调这个函数。

不要 `Thread.stop`，也不要杀掉进程。RocksDB 迭代器要正常关闭。

---

## 4. 查询：Nebula 有什么

语句种类在 `src/parser/Sentence.h`。语法在 `parser.yy`（约 4100 行）和 `scanner.lex`。

### 4.1 图上的读

| nGQL | 行为 | GQL 里对应什么 | 期次 |
|---|---|---|---|
| `MATCH` | 模式匹配，变长路径走 `Traverse` | `MATCH` | 3 起，变长在 4 |
| `GO m TO n STEPS` | 从一批起点按边类型走，管道后接 `WHERE` / `YIELD` | 用 `MATCH` 的量化路径表达。不实现 GO | 4，且每跳有上限 |
| `FETCH PROP ON` | 已知 vid 或边键取属性 | `MATCH (n)` 带主键过滤 | 3 |
| `LOOKUP` | 走索引 | `MATCH` 的 `WHERE` 能下推到索引时走索引 | 7 |
| `FIND PATH` / `FIND SHORTEST PATH` | 全路径或最短路。取邻居时 limit 仍是 -1，LIMIT 只裁最终路径条数 | 最短路单独算子。全路径不做，除非每跳有上限并且用户写了路径条数上限 | 最短路 6，全路径不做 |
| `GET SUBGRAPH` | 若干跳内的点和边，点去重 | 有上限的子图过程 | 8 |
| `FIND ALL PATH` | 邻接上做笛卡尔积 | 默认拒绝。10 跳、度数 10 时路径数是 10^10 | 不做 |

### 4.2 图上的写

| nGQL | 行为 | 期次 |
|---|---|---|
| `INSERT VERTEX` / `INSERT EDGE` | 按 schema 写入。边要写两个方向 | 2，GQL `INSERT` |
| `UPDATE VERTEX` / `UPDATE EDGE` | 改已有属性，可带 `WHEN` | 5，GQL `SET` |
| `DELETE VERTEX` / `DELETE TAG` / `DELETE EDGE` | 删点时可带边 | 5，GQL `DELETE` |
| `UPSERT` | 没有就插入 | 5 |

一条 `INSERT` / `SET` / `DELETE` 所写的键必须原子提交。没有 `BEGIN` / `COMMIT`，也不把多条语句合成一个事务。这和 Nebula 现在的边界一致，这个边界要保留。

单机只有一个分片，一条边的出边、入边、属性、索引放进同一个 `WriteBatch`，对应 `RocksEngine::commitBatchWrite`。提交前进程退出，这批键都不可见；提交后由 RocksDB 的 WAL 恢复。`SET` 和 `DELETE` 在同一把写锁里读旧值、改批次、再提交，避免两次更新互相盖掉。删点时，该点、它的出边和入边、对应属性在同一批里删除；超过 `max_edges_per_vertex` 就整批拒绝，不能删了一半。

出边和入边落到不同分片时，一个 `WriteBatch` 盖不住。这个能力放到第 9 期，语义按 `storage/transaction` 的补偿链保留，不把 C++ 类照搬过来：

1. 起点所在分片先写下 `prime` 标记和这一侧的边。
2. 再写终点所在分片的反向边。
3. 成功后删掉 `prime`。失败由后台扫残留的 `prime` 继续补，或按标记回退。
4. 语句在两侧都完成之前不能对外报成功。补偿期间允许短暂只有一侧可见，这一点写进接口说明。

不保留的是用户自己圈定的多语句事务，以及 Zone、Listener 那些管理功能。补偿链本身要保留。

### 4.3 Schema 和管理

第一期要有的：

- 创建图、删除图。对应 `CREATE SPACE`，但没有副本因子和分片数。
- 节点类型、边类型的创建、修改、删除、描述。对应 Tag / Edge。列类型先做布尔、整数、浮点、字符串、日期时间。列表和映射放到类型系统稳定之后。
- 属性索引的创建、删除、重建。
- `USE` 对应 GQL 的会话当前图。空间名和别名都能 `USE`，解析后会话里保存的是空间 id。
- 图空间别名。语义见第 4.6 节。新系统的 catalog 从第 2 期就带上，不放到管理功能的后期。
- 用户、密码、只读 / 读写两种角色。对应 `GRANT` 的一个子集。

明确往后放的：Zone、Listener、全文索引、地理类型、字符集、快照集群、Balance、配置中心、`CREATE SPACE AS`、服务注册（`SIGN IN SERVICE`）。

### 4.4 表达式和函数

类型在 `Value.h`：空、布尔、整数、浮点、字符串、日期、时间、日期时间、点、边、路径、列表、映射、集合、DataSet、地理、时长，以及多种空值（类型错误、除零、未知属性等）。

图上常用函数：`id`、`labels`、`properties`、`startNode`、`endNode`、`head`、`last`、`nodes`、`relationships`、`length`、`size`、`coalesce`、`type`。

标量和字符串：`abs`、`round`、取整、对数、三角、`left` / `right`、`substring`、大小写。地理函数一整包在 `src/common/geo`。

第一期函数：`id`、`labels`、`properties`、`startNode`、`endNode`、`length`、`coalesce`、`type`，加比较、算术、`AND` / `OR` / `NOT`、`IN`、`IS NULL`。其余按测试补，不先搬完整函数表。

### 4.5 会话和协议

`GraphService::future_executeWithParameter`：sessionId + 查询字符串 + 参数表。没有超时字段。会话空闲默认 8 小时。

新协议从第一天带上：

```text
execute(sessionId, query, parameters, timeoutMs) -> response
cancel(sessionId, queryId)
```

`timeoutMs` 缺省时用服务端 `max_execution_time_ms`。

### 4.6 图空间别名

这不是 MySQL 那种把查询存成视图。别名是空间 id 的另一个名字。点、边、schema 仍然只按空间 id 存一份。删除别名只删目录里的名字，不删数据。

全系统一个名字空间。空间名和别名之间、别名和别名之间都不能重复。一个别名只指向一个空间。一个空间可以有多个别名。别名不能再指向别名，解析只查一次。

目录里存的是空间 id，不是空间名。空间改名后，已有别名仍然指向原来的数据。

语句：

```text
CREATE ALIAS alias_name FOR SPACE space_name
DROP ALIAS alias_name
SHOW ALIASES
USE alias_name
```

`USE` 先按空间名查，没有再按别名查，得到同一个 `spaceId`。权限按这个空间原来的权限，不给别名单独授权。会话里同时记下用户输入的名字、空间的正式名字和 `spaceId`。随后的读写只用 `spaceId`。

删除规则：

- `DROP ALIAS` 只删除别名记录。已经 `USE` 进该空间的会话继续可用。之后再用这个名字 `USE` 会失败。
- `DROP SPACE` 只接受空间的正式名字。用别名调用 `DROP SPACE` 必须失败，避免把别名当成空间删掉数据。删除空间时，指向它的别名一并删掉。
- `CREATE SPACE` 的名字若已被别名占用，失败。`CREATE ALIAS` 的名字若已是空间名或别名，失败。这两项检查和写入放在同一次 meta 提交里。

现有代码上，空间名索引在 `CreateSpaceProcessor` 写入的 `indexSpaceKey`。`USE` 在 `SwitchSpaceExecutor` 里调用 `getSpace(空间名)`。别名用单独的 meta 前缀，不要写进空间索引。`getSpace` 在空间名未命中时再查别名，然后用空间 id 取空间描述。graphd 的 meta 缓存要在创建和删除别名后失效，规则与空间缓存相同。

---

## 5. 对照结论

照着实现的是这些语义，不是这些 C++ 类：

- 定长 vid、出边和入边各自一条键、两个方向都是前缀扫描。rank 表示重边。属性只存一份，不跟着方向复制。
- 点有标签，边有类型，属性有类型和可空。
- 模式匹配、过滤、投影、聚合、排序、LIMIT。
- 索引查找，以及索引与数据一起提交。
- 一条 `INSERT` / `SET` / `DELETE` 原子生效。单机用一个 `WriteBatch`。跨分片的边用补偿链，保证两个方向最终一致。没有多语句 `BEGIN` / `COMMIT`。
- 多跳扩展、访问去重、最短路。
- 用户和会话。
- 图空间别名：全系统名字不重复，`USE` 别名进入同一个空间 id，删除别名不删数据。
- COMPACT、FLUSH、重建索引、统计度数。

改掉的实现：

- 三进程收成单进程。存储调用是方法，不是 RPC。
- 属性不放在边的值里。扫描拓扑和读取属性分开。
- 每跳有条数上限，截断对客户端可见。
- 查询有截止时间，取消能停迭代器。
- 关系算子进 DuckDB，不在 Java 堆里放完整 DataSet。
- 路径只在条数已经受限之后才物化。

不移植的：

- nGQL 的 `GO`、管道、`YIELD` 语法。
- Raft 的具体实现、Zone、Listener、全文、地理。分片和副本按第 7 节第 2 期，不把 Nebula 的 Balance 任务搬过来。跨分片补偿链的语义要留，类不照搬。
- 无上限的全路径。
- 进程级 80% 内存才失败、按查询不记账的模型。

---

## 6. 代码怎么拆

Maven 模块，Java 21。包名按项目定，下面用 `graphdb`。

```text
graphdb-store      RocksDB、键、WriteBatch、扫描、索引
graphdb-catalog    图、标签、边类型、属性、用户
graphdb-parser     ANTLR 生成的 GQL 语法树
graphdb-query      语义、逻辑计划、Expand、取消、DuckDB
graphdb-server     会话、execute / cancel、管理命令
```

模块之间只允许靠下面这几个接口说话。查询包不引用 RocksDB 的类。

```text
GraphStore
  mutate(Batch)
  getVertex(graph, vid)
  scanEdges(graph, src, type, direction, limit, cancel) -> EdgeCursor
  multiGetProps(keys) -> values
  scanIndex(index, from, to, limit, cancel) -> identities

Catalog
  graph(name) -> GraphId
  nodeType(name) -> TypeId, properties
  relType(name) -> TypeId, properties

QueryHandle
  id, deadline, cancel(reason)
  boolean cancelled()

Planner
  plan(statement, catalog, limits) -> PhysicalPlan
```

`limits` 来自配置，第一期写成常数，不要散落在执行器里：

| 参数 | 建议初值 | 原因 |
|---|---|---|
| `max_execution_time_ms` | 1000 | 接口目标是毫秒级。长任务必须显式加大 |
| `max_edges_per_vertex` | 64 | 略高于常见度数 40，挡住意外大点 |
| `max_frontier_per_hop` | 64 | 保住 10 跳仍在几百条边 |
| `max_hops` | 10 | 和现有查询深度一致 |
| `max_paths` | 100 | 需要路径值时的上限 |
| `query_memory_bytes` | 64MB | 单查询。DuckDB 另有自己的 limit |

超过 `max_edges_per_vertex` 或 `max_frontier_per_hop` 时，普通 `MATCH` 失败并说明截断点。不做静默少返回。用户写的 `LIMIT` 是成功提前结束，不是这个失败上限。穿透持股不用这两个点数上限，用第 10.9 节的比例阈值。

---

## 7. 分期：每一期写什么、怎样算做完

每一期都要有测试再进入下一期。测试用 JUnit。存储用临时目录里的 RocksDB，测完删除。

### 生产先行：图空间别名

这一项只在现有 C++ 服务上打补丁时才做，关键字仍是 `SPACE`。新仓库不要先改旧代码，按第 10.5 节在第 2 期做，关键字用 `GRAPH`。语义以第 4.6 节为准。

改动范围：

- meta：别名键、`CREATE ALIAS` / `DROP ALIAS` / `SHOW ALIASES`。创建空间、创建别名时互查名字。删除空间时删掉它的全部别名。
- graphd：`getSpace` 在空间名失败后按别名解析。`USE` 不另写一套切换逻辑。
- 存储的点边键、RocksDB、查询执行器不动。

完成：

- 空间 `s` 已有数据。`CREATE ALIAS a FOR SPACE s` 之后，`USE a` 能读到这些数据。
- `DROP ALIAS a` 之后，`USE s` 的数据还在，`USE a` 失败。
- `CREATE SPACE a`、再建一个同名别名、`DROP SPACE a` 这三步都失败，且 `s` 的数据还在。
- 两个会话同时用同一个新名字创建空间和别名，只能成功一个。

### 第 0 期：工程能编过

- Java 21、Maven、`rocksdbjni`、DuckDB JDBC 的依赖能解析。
- 空的 `GraphStore.open(path)` 能打开并关闭。
- 不写查询。

完成：`mvn test` 通过，测试里开关一次数据库。

### 第 1 期：拓扑读写

这一期只在单机上测试：一个进程、一个数据目录。`partId` 恒为 0。不建副本，不加存储节点，也不做计算节点调度。

类：`KeyCodec`、`RocksGraphStore`、`EdgeCursor`、`WriteBatch`。

- 内部 vid 的分配。
- 插入一条边得到出边键和入边键。
- `scanEdges` 按起点、类型、方向返回，出和入都要实现，遵守 `limit`。插入只写一条方向的键视为失败。
- `cancel` 在扫描中途置上后，游标在 64 条内结束。
- 属性列族先留空。

完成：

- 插入 A→B 之后，从 A 查出边得到 B，从 B 查入边得到 A。只写出边或只写入边的数据，两个测试都必须失败。
- 1 万个点、每点 40 条出边，取一个点的 40 条出边，以及从终点取对应的 40 条入边，热缓存 p99 都低于 10 毫秒（本机测试，写入报告里的机器型号）。
- 从一个点做 10 跳、每跳最多 40 条、访问过的点不再进入下一跳，总时间低于 10 毫秒。这一条证明「收住边界」的循环是对的，此时还没有 GQL。
- 不设上限地 10 跳全展开的测试应当在开发时被预算直接拒绝，而不是跑完。

### 第 2 期：Schema 和属性

类：`Catalog`、`NodeType`、`RelType`、`PropCodec`。

- 创建图、节点类型、边类型。列类型：布尔、长整数、双精度、字符串、日期时间。可空。
- 属性写入 `prop`，不写入 `edge` 的值。
- 修改类型只允许加可空列。删列、改类型放到有数据迁移测试之后。
- 插入点、插入边。边、反向边、属性、点记录在一个 `WriteBatch`。
- 按 vid 读点。按边的身份 `multiGetProps`。
- 空间别名和正式名字放在同一张目录里，规则与第 4.6 节相同。`USE` 别名得到的是空间 id。

完成：

- 重启进程后 schema 和数据还在。读 40 条边的拓扑时，属性字节没有被解码（可以用计数器断言 `prop` 列族的 get 次数为 0）。
- 插入一条边时，在 `commit` 之前注入失败。出边、入边、属性三条都不能被读到。
- 提交成功后杀掉进程再打开，出边和入边都在。两侧不能只剩一条。

#### 分片、副本和计算节点

规则先写在本期。第 1 期的单机测试不实现，也不跑下面的验收。编码放在第 9 期，单机各期稳定之后，按这里做。

三件事分开：

| 操作 | 目的 | 数据怎么动 |
|---|---|---|
| 加副本 | 同一批分片多一份，用来扛机器故障 | 检查点拷贝，追上之后才有投票权 |
| 加存储节点 | 把已有分片搬到新机器，扩大容量 | 按分片搬，只停正在搬的分片 |
| 加减计算节点 | 查询进程无状态 | 不拷数据，只改谁可以接新查询 |

创建图时副本数只能是 1。灌数用大批 `WriteBatch`，要更快就生成 SST 再灌入。灌完执行 `FLUSH`，再做 RocksDB 检查点。这之前不加副本。每条写入都等多数派确认，是 Nebula 在多副本上初始化慢的原因，导入阶段不走那条路径。

目录里用这些记录，页面不另存一份节点表：

```text
0x50 + nodeId(2)
value: role(1) + state(1) + utf8 地址
role:  1 存储, 2 计算
state: 1 在役, 2 排空中, 3 学习者

0x51 + graphId(2) + partId(2) + nodeId(2)
value: vote(1)
vote:  1 投票成员, 2 学习者

0x52 + graphId(2)
value: mode(1) + replicaFactor(2) + partCount(2)
mode:  1 正常, 2 副本同步中

0x53 + graphId(2) + partId(2)
value: moveState(1)
moveState: 0 无, 1 正在搬迁
```

页面只发管理命令，成员以这些键为准。命令走第 10.16 节的同一条连接，不进 GQL：

```text
{"op":"admin","command":"addStorage","addr":"host:port"}
{"op":"admin","command":"drainStorage","nodeId":1}
{"op":"admin","command":"removeStorage","nodeId":1}
{"op":"admin","command":"addReplica","graph":"g","replicas":3}
{"op":"admin","command":"addCompute","addr":"host:port"}
{"op":"admin","command":"drainCompute","nodeId":2}
{"op":"admin","command":"removeCompute","nodeId":2}
```

加副本的第一版让整个空间进入副本同步。`mode = 2`。查询继续。`INSERT`、`SET`、`DELETE` 以及会改目录的语句返回 `SPACE_READONLY`。然后 `FLUSH`，做检查点，把文件拷到新存储节点。新节点先写成学习者，`vote = 2`，不参与投票，页面显示的副本数只计投票成员。拷贝期间源上仍然只有一份投票副本。双方键范围核对一致后，把学习者改成投票成员，`replicaFactor` 更新，`mode` 回到 1。源进程在提升前退出，数据仍只在源磁盘上，空间保持单副本。

只读拷贝跑通之后，再做日志追赶：停写只留在打检查点的那一下，拷贝期间的新写进入该分片的提交日志，学习者补齐到相同序号后再提升。有 3 个投票成员之后，一条写入要多数派确认才返回 `OK`，一个分片可以少一台机器。

加存储节点时，新机器以空的在役存储节点加入，先不持有分片。搬迁按 `partId` 从小到大。被搬的分片写 `moveState = 1`，哈希到这个分片的写入返回 `PART_READONLY`，同一张图的其它分片继续写。检查点放到新节点并成为该分片的投票成员之后，删掉旧节点上的这一条 `0x51`。目标投票成员数不够时，拒绝开始搬迁。

删除不是立刻摘掉进程。`drainStorage` 把节点标成排空中，不再把新分片放到它上面，并把它现有的分片按上面的搬迁迁走。某个分片若只剩这一张投票票，拒绝排空。`removeStorage` 只接受排空中、且该 `nodeId` 已经没有任何 `0x51` 记录的节点，否则返回 `NODE_NOT_DRAINED`。

`partId` 在第 1 期恒为 0。开始实现本节时改为 `Integer.floorMod(vid, partCount)`。已有数据要按新的 `partId` 重写键，不能只改计算函数。出边用 `outSrc` 算分片，入边用 `outDst`。两个分片不同的边走第 10.18 节的补偿链。

计算节点不存放点边。`addCompute` 在健康检查通过后把节点标为在役。接入层把新会话交给一台在役计算节点，顺序是：正在执行的查询数少，并列时已计入的查询内存少，再并列时最近一次上报的 CPU 使用率低。会话第一次 `execute` 时把选中的 `nodeId` 记在会话上，之后的语句都到这台机器。一条查询开始后不再换节点，多跳就在这台机器上跑完。`drainCompute` 不再接新会话，已经绑定的会话继续到查询结束或会话空闲超时。`removeCompute` 只接受已经没有绑定会话、也没有进行中查询的节点。

第 9 期验收：

- 单副本灌入一批边，再加到 3 副本。同步期间写入得到 `SPACE_READONLY`，读取仍能看见灌入的边。提升完成后两个新节点都能按前缀读到同一条边。
- 提升前关掉源进程，页面上的投票副本数仍是 1。
- 搬一个分片时，另一个分片的插入成功。
- 对仍持有唯一投票副本的存储节点执行 `drainStorage`，得到 `NODE_NOT_DRAINED`。
- 两台计算节点都在役时，新会话落到正在执行的查询更少的那台。同一会话的第二条语句仍到这台。

### 第 3 期：GQL 的一跳

- 引入 ANTLR 4。语法从 opengql 的 GQL 语法裁剪，引入前核对许可证。不把 ISO 正文抄进仓库。
- 语义分析：名字要在 catalog 里，类型要匹配。
- 语句：`INSERT`、`MATCH` 定长一步、`WHERE`、`RETURN`、`LIMIT`、`ORDER BY`。一步 `MATCH` 要覆盖 `->`、`<-` 和没有方向的 `-`。`<-` 只能走入边前缀。
- `WHERE` 里只涉及边类型、rank、终点 vid 的条件，在 `scanEdges` 时判断，不先收齐再滤。
- 需要属性时，对已经限于 `LIMIT` 的那些边做一次 `multiGetProps`。
- 会话有当前图。参数用占位符，不拼字符串。

完成：

- 固定的一组 GQL 语句有语法树快照测试和结果测试。
- 「一跳、LIMIT 100、热缓存」p99 低于 10 毫秒。
- 度数 1000 的点，`LIMIT 10` 时扫描在第 10 条符合条件的边停止，而不是扫完 1000 条再丢。

### 第 4 期：多跳和预算

这是和现有查询最相关的一期。

- `MATCH` 的定长跳和 `{1,10}` 这种上界。上界不得超过 `max_hops`。
- 每一跳：上一跳的终点做起点，跳过已访问 vid，每点最多 `max_edges_per_vertex`，本跳新 vid 最多 `max_frontier_per_hop`。
- 任一步达到上限，整条语句失败，错误里带上跳数和起点 vid。
- 路径值只在最终行数不超过 `max_paths` 时构造。
- 执行循环每一跳开始前看 `cancelled`。

完成：

- 度数 40、10 跳、每跳过滤后仍可能超过 64 个新点的图，语句在超限那一跳失败，耗时是毫秒级，而不是把图扫完。
- 每跳过滤后不超过 40 个新点的 10 跳，p99 低于 10 毫秒。
- 这两组测试用同一套生成图，只改过滤条件。防止以后有人把上限拿掉。

### 第 5 期：改、删、聚合

- `SET`、`DELETE` 走和第 2 期同一条原子写路径。`SET` 在写锁里读旧值再写回，属性、出边、入边同一批提交。
- 删点时按该点的出边和入边前缀收集要删的键，仍放进同一个 `WriteBatch`。边数超过 `max_edges_per_vertex` 则整句失败，不留下半删状态。
- `RETURN` 上的聚合、`DISTINCT`、`ORDER BY`、`SKIP` 进 DuckDB。输入是已经截断的本跳行，不是全图。
- DuckDB 设置内存上限。超出后能落盘而不是把 JVM 打满。可以用一个故意大于堆的 `ORDER BY` 测试观察临时文件，而不是看进程被系统杀掉。

### 第 6 期：取消和最短路

- `timeout_ms`、服务端默认时限、连接关闭、`cancel` 接口，都进入 `QueryHandle.cancel`。
- 扫描中、DuckDB 执行中、跳与跳之间都能停。
- 两点之间的最短路：从两端交替扩展，每侧受同一套每跳上限。跳数到 `max_hops` 仍没相遇就返回空，不改为全图 BFS。
- 穿透持股是单独算子，不走路径拼接。算法、阈值和循环处理见第 10.9 节。
- 不实现全路径。语句如果表达的是「所有路径」，语义分析直接拒绝，并提示用最短路或加上边界。

完成：时限 50 毫秒、图足够大时，调用在 100 毫秒内返回超时错误，且该查询的扫描计数不再增加。

### 第 7 期：索引

- 创建属性索引。写入时索引键和数据在同一个 `WriteBatch`。
- `WHERE` 是索引列上的等值或范围时，先 `scanIndex` 再取点，不扫全部边。
- `REBUILD INDEX` 扫 `vertex` / `edge` 列族重建。重建过程可取消。
- 统计 Job：点数、边数、按类型的最大度数。最大度数只作为观测，不代替第 4 期的硬上限。

### 第 8 期：会话、权限、子图、运维

- 用户、密码、读写角色。
- 有上限的子图读取。
- `COMPACT`、`FLUSH`。
- 管理命令与 GQL 分开，不塞进 GQL 语法。

### 第 9 期：实现第 2 期的分片规则

前置条件：第 1 期到第 8 期的单机测试稳定。成员、只读窗口、加副本、搬分片、计算节点调度都按第 2 期已经写好的规则编码，不另起一套。

- `partId` 改为按 vid 计算。出边跟起点走，入边跟终点走。已有单机数据要重写键。
- 同一分片内的键仍然是一个 `WriteBatch`。跨分片的边走第 10.18 节。补偿未完成时查询可能只看见一个方向。远端写入失败后重启，后台能补齐或回退。
- 投票成员达到 3 之后，写入先进入该分片的提交日志，多数派确认后再写进本地 RocksDB。不把 `src/kvstore/raftex` 翻译成 Java。
- 验收条目就是第 2 期列出的那五条。

---

## 8. 建议的实现顺序（打开工程之后）

1. 新仓库从第 10.15 节的 `KeyCodec` 开始。只有现网等不及、还在改这份 C++ 代码时，才先做上面的生产先行别名。
2. `KeyCodec` 的往返测试：同一输入编码再解码，字段相等。出边前缀不得匹配到入边。
3. `RocksGraphStore.scanEdges` 的 limit 和 cancel。
4. 10 跳、每跳 40 的预算循环，用 Java API 而不是 GQL 先跑通第 1 期的性能测试。
5. 再接 catalog 和 ANTLR。语法没裁剪之前不要开始执行器。
6. DuckDB 只在第 5 期接入。更早接入会把「扩展」错误地写成递归 SQL。

第 1 期的测试只启动一个进程、一个数据目录。第 2 期里的分片、副本和计算节点留到第 9 期再编码。

性能测试每次记录：数据量、是否热缓存、块缓存大小、堆大小、p50、p99。没有这些数字的「毫秒级」不作为完成。

---

## 9. 附录：可选的源码对照

新项目编码不要依赖这一节。行为已经写在第 10 节。下面的路径只用于核对这份仓库当时的实现。

| 要确认的行为 | 位置 |
|---|---|
| 点边键布局 | `src/common/utils/NebulaKeyUtils.h` |
| 扫边并按 limit 截断 | `src/storage/exec/GetNeighborsNode.h` |
| limit 如何盖过旗标 | `src/storage/query/GetNeighborsProcessor.cpp` |
| 图查询入口 | `src/graph/service/GraphService.cpp` |
| 查询何时释放 | `src/graph/service/QueryInstance.cpp` |
| 多跳 MATCH | `src/graph/executor/query/TraverseExecutor.cpp` |
| GO 的 limit | `src/graph/executor/query/ExpandAllExecutor.cpp` |
| 手动 KILL | `src/graph/executor/admin/KillQueryExecutor.cpp` |
| storage 侧多久看一次 KILL | `src/clients/meta/MetaClient.cpp` 的 `checkIsPlanKilled` |
| 语句清单 | `src/parser/Sentence.h` |
| Job 类型 | `src/interface/meta.thrift` |
| 值类型 | `src/common/datatypes/Value.h` |
| 优化规则 | `src/graph/optimizer/rule` |
| 空间名索引与 `USE` | `src/meta/processors/parts/CreateSpaceProcessor.cpp`，`src/graph/executor/admin/SwitchSpaceExecutor.cpp` |

## 10. 可独立编码的规格

新仓库只看这一节就能开工。数字、字节序和错误码都是本系统的约定，写代码时按这里实现，再拿第 7 节的测试验收。

### 10.1 进程与线程

一个 JVM 进程，监听一个端口。启动顺序：

1. 打开 RocksDB，列族不存在就创建。
2. 读 catalog 列族，建内存名字表。
3. 启动超时扫描线程，每 100 ms 看一次运行中的查询。
4. 开始接受连接。

线程：

| 线程 | 做什么 |
|---|---|
| 网络线程 | 读请求，提交到查询池，不在这里扫 RocksDB |
| 查询池 | 固定大小，默认等于 CPU 核数。一条查询占一个线程直到结束或取消 |
| 超时线程 | 到期调用 `QueryHandle.cancel()` |
| 阶段 9 的补偿线程 | 每 10 秒扫一次未完成的跨分片边 |

同一张图的写用一把 `ReentrantLock`。读不用这把锁，用 RocksDB 的 `Snapshot`。阶段 1 只有一张图、一个分片，锁是进程级的。

### 10.2 键布局

所有多字节整数用大端。RocksDB 按字节比较，大端才能让同一个前缀里的整数有序。

七个列族，建库时一次建好。阶段 1 用不到的列族也先建出来，避免以后改列族名单。

| 列族 | 内容 |
|---|---|
| `default` | 不用，保持打开 |
| `vertex` | 点键。值是点属性，编码见第 10.3 节 |
| `edge` | 出边键、入边键。值是空的 0 字节 |
| `prop` | 边属性，一条逻辑边一条。点属性不进这个列族 |
| `catalog` | 空间、别名、类型、属性定义、自增号 |
| `index` | 阶段 7 以前保持空 |
| `txn` | 阶段 9 以前保持空。跨分片补偿标记 |

边键，固定 42 字节：

| 偏移 | 长度 | 字段 |
|---|---|---|
| 0 | 2 | graphId，uint16，从 1 开始 |
| 2 | 2 | partId，uint16，阶段 1 恒为 0 |
| 4 | 8 | src，int64 |
| 12 | 4 | edgeTypeId，uint32，从 1 开始 |
| 16 | 1 | direction，0 是出边，1 是入边 |
| 17 | 8 | rank，int64，默认 0 |
| 25 | 8 | dst，int64 |
| 33 | 9 | 保留，填 0。前缀、比较、解码都不读这 9 字节 |

出边前缀是前 17 字节：`graphId + partId + src + edgeTypeId + direction=0`。入边前缀同样 17 字节，只是 src 换成要查的点、direction 换成 1。

逻辑边身份，用来做属性键，不随方向对调：

`edgeTypeId + rank + outSrc + outDst`

出边键里 src 就是 outSrc。入边键要把两端反过来写，属性键仍然用出边那对端点。这样两个方向共用一条属性。

边属性键：

| 偏移 | 长度 | 字段 |
|---|---|---|
| 0 | 1 | 固定 `0x01` |
| 1 | 2 | graphId |
| 3 | 4 | edgeTypeId |
| 7 | 8 | rank |
| 15 | 8 | outSrc |
| 23 | 8 | outDst |

属性键里不放 schemaVersion。版本写在值里，这样按身份 `multiGet` 时不用先猜版本。点键固定 16 字节：`graphId(2) + partId(2) + vid(8) + tagId(4)`。值同样是第 10.3 节的属性编码。一个点可以有多条点键，每种节点类型一条。阶段 3 只有一个点类型时 tagId 用 1。

`id` 不是普通属性。阶段 7 之前，插入语句里的 `id` 必须是 INT64，这个数就是 vid，不再另分配内部号，也不把 `id` 写进属性字节。字符串业务键等到阶段 7 的唯一索引。自环允许：outSrc 和 outDst 相同，出边键和入边键仍然各写一条。

一个点的全部关联边共用 12 字节前缀：`graphId + partId + vid`。这个前缀下面既有出边也有入边，每条逻辑边在该点上只出现一次。删点时扫这 12 字节，而不是扫 17 字节的单类型前缀。

### 10.3 属性编码

值的布局：

1. `uint8` 编码版本，第一版是 `1`。
2. `uint32` schemaVersion。写入时用该类型当前版本。
3. `uint16` 属性个数，等于这个 schemaVersion 的属性个数。
4. 空值位图，每 8 个属性 1 字节，位 0 对第 0 个属性，1 表示空。
5. 按属性下标从小到大写非空值，空值不占位。

读的时候如果值里的 schemaVersion 比目录里的小，只允许差在尾部新加的可空列上，这些列读成空。差在别的地方，或者值的版本比目录新，返回 `SCHEMA_MISMATCH`。改类型只能 `ADD` 可空列，schemaVersion 加 1，不改已经写下的值。

单个值：

| 类型 | 字节 |
|---|---|
| BOOL | 1 字节，0 或 1 |
| INT64 | 8 字节，大端 |
| DOUBLE | 8 字节，IEEE754 大端 |
| STRING | `uint32` 字节长度 + UTF-8，长度上限 64 KiB |
| DATETIME | 8 字节 int64，UTC 毫秒 |

持股比例用 DOUBLE。写入前把 NaN、无穷大拒绝掉。比例允许大于 1，查询时如果一个点的出边比例之和大于 1.000001，记一条数据质量告警，计算仍继续。

未知属性名在语义分析阶段报错，不留到执行期。

### 10.4 打开 RocksDB

```text
create_if_missing = true
create_missing_column_families = true
WAL 打开
压缩 = LZ4
prefix_extractor = 固定 17 字节，只挂在 edge 列族
edge 列族开 prefix bloom
block_cache = 机器内存的 25%，32 GB 机器取 8 GB
write_buffer_size = 64 MB
max_background_jobs = 4
```

扫边用 `seek(prefix)`，上界按下面的函数计算。迭代器读到 `key >= upper` 就停。`upper` 为空表示扫到列族末尾。不要全表扫描再过滤。

```text
upperBound(prefix):
    b = copy(prefix)
    for i from b.length - 1 down to 0:
        if b[i] != 0xFF:
            b[i] = b[i] + 1
            return b
        b[i] = 0
    return 空
```

出边前缀的最后一字节是 0，加 1 之后正好是同一点、同一类型的入边前缀。所以出边扫描的上界就是入边前缀，不会把入边算进去。

### 10.5 catalog 记录

名字最长 128 字节，UTF-8，中间不能有 `0x00`。比较名字时区分大小写，不把大小写折成同一种。

名字主键，空间名和别名共用，全局一张：

```text
key:   0x10 + utf8(name)
value: kind(1) + graphId(2)
kind:  1 = 空间, 2 = 别名
```

下一个空间号：

```text
key:   0x01
value: nextGraphId，uint16，初始 1
```

空间记录：

```text
key:   0x11 + graphId(2)
value: utf8 名字长度 uint16 + 名字
```

别名记录：

```text
key:   0x12 + graphId(2) + utf8(alias)
value: 空
```

节点类型和边类型各有一个自增号，都从 1 开始：

```text
key:   0x21 + graphId    value: nextNodeTypeId，uint32
key:   0x22 + graphId    value: nextRelTypeId，uint32
```

类型记录：

```text
key: 0x20 + graphId(2) + kind(1) + typeId(4)
kind: 1 = 节点类型, 2 = 边类型
value:
  schemaVersion uint32，新建为 1
  utf8 名字
  uint16 属性个数
  重复：utf8 名字、类型号 uint8、可空 uint8
类型号：1 BOOL，2 INT64，3 DOUBLE，4 STRING，5 DATETIME
```

`Catalog.graph(name)` 只查一次 `0x10`。kind 是 1 或 2 都返回同一个 graphId，再读 `0x11` 拿规范名。别名不能指向别名。会话记下 graphId、规范名、用户输入的名字、kind。之后的读写只用 graphId。

同一个 `WriteBatch` 里写名字主键和空间或别名记录。创建前先 `get` 名字主键，存在就返回 `NAME_CONFLICT`，不覆盖。两个会话抢同一个名字时，后写入的批次会看到已存在的键，必须失败。RocksDB 的 `WriteBatch` 本身不检查键冲突，所以创建名字要在图目录锁里完成「先读再写」。阶段 1 这把锁就是进程里的那把写锁。

`DROP GRAPH name` 只接受 kind=1。用别名调用返回 `DROP_ALIAS_AS_GRAPH`。删除空间记录、该 graphId 下全部 `0x12` 前缀、这些别名的 `0x10` 名字键、类型记录和自增号。阶段 9 之前同时删数据：`vertex`、`edge`、`txn` 的前缀是 graphId 这两个字节，`prop` 的前缀是 `0x01 + graphId`。`index` 的键以 indexId 开头，要先读该图的 `0x30` 记录，再按每个 indexId 删。阶段 9 改成异步任务，接口先返回已接受。

`DROP ALIAS name` 只接受 kind=2，删名字键和 `0x12` 记录。已经 `USE` 进该空间的会话继续用原来的 graphId。

`SHOW ALIASES` 扫 `0x10`，留下 kind=2，再读 `0x11` 得到规范名。结果三列：别名、规范名、graphId。

节点类型名、边类型名只在本图内唯一，不进 `0x10`。创建类型时扫本图的 `0x20` 前缀看名字。`ALTER NODE TYPE` / `ALTER REL TYPE` 只允许在末尾加可空列，schemaVersion 加 1，和类型记录放在同一个批次。

### 10.6 写入算法

插入一条有向边 `outSrc -> outDst`。不要求两端已经有点记录，只写边。默认覆盖同一条逻辑边的属性。

1. 拿图写锁。
2. 解析类型名，拿到 edgeTypeId 和当前 schemaVersion。缺列、未知列、类型不合、NaN、无穷大都在这里失败，不写批次。
3. `partOf(vid)`。阶段 9 之前恒为 0。阶段 9 起为 `Integer.floorMod(vid, partCount)`，已有数据要重写键，不能只改函数。
4. 若出边 partId 和入边 partId 相同，编码属性，`WriteBatch` 放三条：出边键、入边键、`prop` 键，然后 `db.write`。
5. 两个 partId 不同时走第 10.18 节，阶段 9 之前直接返回 `CROSS_PART_UNSUPPORTED`。
6. 释放写锁。`write` 抛错时调用方看到失败。提交前进程退出，三条键都不可见；提交后靠 WAL 恢复。两侧不能只剩一条。

`IF NOT EXISTS` 先读 `prop` 键。存在就返回 `OK`，`rowsAffected = 0`，批次不写。不要返回错误，也别覆盖旧属性。

`SET` 在同一把写锁里读旧属性，按第 10.3 节解出来，改指定列，再用当前 schemaVersion 编码写回。出边键和入边键不变。旧值和新值放在同一个批次里提交。第一版没有单独的 `WHEN`。条件写在 `MATCH` 的 `WHERE` 里，在这把锁内检查，不符合就返回 `OK`，`rowsAffected = 0`。

删除一条边：同一个批次删出边键、入边键、`prop` 键。

删除一个点：

1. 扫 `vertex` 上 `graphId + partId + vid` 的 12 字节前缀，收集点键。
2. 扫 `edge` 上同样 12 字节前缀。每条键解码出逻辑端点，把这一侧的键、另一侧的键、`prop` 键放进集合。自环的两条键会各被看到一次，集合按字节去重。
3. 逻辑边数大于 `max_edges_per_vertex` 时整句失败，错误码 `VERTEX_DEGREE_EXCEEDED`，批次不提交。
4. 否则一个批次删掉集合里的全部键。

一条语句里的多行插入、修改、删除合成一个批次。没有 `BEGIN` / `COMMIT`，也不把多条语句合成一个事务。

读查询在开始时取 RocksDB `Snapshot`，在 `finally` 里释放。写不使用这条快照。快照期间提交的写，这条查询看不见。

### 10.7 扫描算法

这里有两个不同的数。`max_edges_per_vertex` 是安全上限，超过就失败。语句里的 `LIMIT` 是已经找到足够的匹配后成功返回。

```text
scanEdges(graphId, vid, edgeTypeId, direction, queryLimit, cancel):
    prefix = 前 17 字节
    it = edgeCf.newIterator(snapshot)
    it.seek(prefix)
    upper = upperBound(prefix)
    examined = 0
    matched = 0
    while it.isValid() and (upper 为空 or it.key() < upper):
        每 64 条看一次 cancel，已取消就关迭代器并抛 QUERY_CANCELED
        解出 rank 和键里的 dst，换算成逻辑端点
        examined += 1
        if examined > max_edges_per_vertex:
            关迭代器，抛 TRUNCATED
        if 端点谓词不通过：继续
        matched += 1
        产出 EdgeRef(type, rank, outSrc, outDst)
        if queryLimit >= 0 and matched == queryLimit:
            关迭代器，正常返回
    关闭迭代器
```

`direction=0` 时键里的 src 是 outSrc，dst 是 outDst。`direction=1` 时键里的 src 是原来的 outDst，dst 是原来的 outSrc，产出前换回来。调用方拿到的始终是逻辑端点。

无向一步 `-` 对同一点和同一类型扫 direction 0 和 1，按逻辑边身份去重后再计数。安全上限按去重后的条数算。

属性在扫描之后按需 `multiGet`。只判断类型、rank、端点的谓词不要读 `prop`。需要属性才能判断的谓词，先受安全上限收口，再批量取属性。`LIMIT 10` 且前 10 条边已经满足谓词时，扫描停在第 10 条，`prop` 的 get 次数不超过 10。度数 1000 但前 64 条里凑不齐 `LIMIT` 所要的匹配时，返回 `TRUNCATED`，不继续扫完。

第 10.9 节的持股算子是唯一把 `max_edges_per_vertex` 关掉的扫描。它仍然每 64 条检查取消。

### 10.8 查询执行

一条语句的管道：

```text
字节 -> 语法树 -> 绑定 graphId 和类型 -> 逻辑计划 -> 执行
```

单跳 `MATCH` 的物理步骤：

1. 用起点、类型、方向调用 `scanEdges`。
2. 只用 rank 和端点就能判断的条件在扫描循环里丢边。
3. 需要属性的条件跟着扫描逐批 `multiGet`。凑够语句里的 `LIMIT` 就停。没写 `LIMIT` 时最多处理到 `max_edges_per_vertex`，再多一条就 `TRUNCATED`。
4. `RETURN` 的行是最终行，此时才构造。
5. 有 `ORDER BY`、`COUNT`、`SUM` 时把最终行交给 DuckDB 的内存表，并设置 DuckDB `memory_limit`。

多跳普通 `MATCH` 用全局 visited。点进过 frontier 就不再进。这是第 1 期和第 4 期要的预算循环，菱形图上第二条走到同一个点的路径不会出现。要把多条路径的比例加起来，必须走第 10.9 节，不能用这个循环。

```text
visited = {起点}
frontier = {起点}
for hop in 1..hops:
    取消检查
    if frontier.size() > max_frontier_per_hop: 失败 FRONTIER_EXCEEDED
    next = 空集合
    for vid in frontier:
        refs = scanEdges(...)
        扫描内部若抛 TRUNCATED，整句失败
        for dst in refs 的对端:
            if visited 含 dst: 跳过
            visited.add(dst)
            next.add(dst)
            if next.size() > max_frontier_per_hop: 失败 FRONTIER_EXCEEDED
    frontier = next
返回 frontier，或最后一跳已经受限的边
```

`RETURN` 要求路径值时，只在最终行数不超过 `max_paths` 之后才逐行构造路径。构造之前发现会超过，整句失败 `PATH_LIMIT_EXCEEDED`，不返回半截。没写路径值的查询只保留 vid 集合。

两个上限都是失败，不把截断结果当成成功。用户写的 `LIMIT` 只裁最终成功的行，不能把 `TRUNCATED` 变成 `OK`。

### 10.9 穿透持股算子

语法在第 10.11 节。它不走 `max_frontier_per_hop` 和 `max_edges_per_vertex`。

输入：起点 `s`，边类型，最多 `H` 跳，`minEdgeRatio` 默认 0.01，`minAccumRatio` 默认 0.001。

调用 `dfs(s, 1.0, 0, [s])`。`best` 初值是空 map。

```text
dfs(vid, accum, depth, stack):
    取消检查
    if depth == H: return
    for edge in scanEdges(vid, holder 类型, direction=0, 关闭每点上限):
        ratio = multiGet 这一条逻辑边的 ratioProp
        if ratio < minEdgeRatio: continue
        nxt = accum * ratio
        if nxt < minAccumRatio: continue
        if stack 含 edge.outDst: continue
        best[edge.outDst] += nxt
        stack.push(edge.outDst)
        dfs(edge.outDst, nxt, depth + 1, stack)
        stack.pop()

best 里去掉 s
答案 = best 中值最大的 vid，并列时取 vid 较小的一个
```

`stack` 只含当前路径，防止环。另一条路径仍可进入同一个点，比例要加进 `best`。`best` 是 `HashMap<Long, Double>`，一条查询一份。这个扫描不使用 `max_edges_per_vertex`，也不使用 `max_frontier_per_hop`。

精确算完 10 跳、途经几千个点，时间是秒级。调用方要把 `timeout_ms` 加大。到点就按第 10.10 节取消，不返回算到一半的最大股东。

5000 个点的 map 是几十 KB。内存告警来自把路径当行保存，这个算子禁止保存路径。

若业务要“只看最终自然人”，在类型上加终点过滤；没过滤时最大的是累计比例最大的企业或人。

验收用这张图，阈值都放低到 0，让短路径不会被剪掉。S→A 0.5，S→B 0.5，A→C 0.4，B→C 0.4。算完 `best[A]=0.5`，`best[B]=0.5`，`best[C]=0.4`。并列时 A 的 vid 更小，答案是 A。C 被加了两次，说明两条路径都进了 map。把 A→C 改成指向 A，这条环不能让 A 的比例再乘一遍。

### 10.10 取消

`QueryHandle` 持有 `AtomicBoolean canceled`、`deadlineNanos`、DuckDB 的 `interrupt`、当前 RocksDB 迭代器引用。

四个入口都只调用 `cancel()`：

| 来源 | 何时 |
|---|---|
| `timeout_ms` | 查询开始时算出 deadline |
| 服务默认 | `max_execution_time_ms`，请求没传 timeout 时使用 |
| 连接断开 | 该连接上所有未完成句柄 |
| `CANCEL QUERY id` | 按查询 id |

`cancel()` 把标志置位、调用 DuckDB interrupt、关闭当前迭代器。查询线程在下一处检查点抛 `QUERY_CANCELED` 并释放 snapshot。不使用 `Thread.stop`。

### 10.11 第一版语法

足够让 ANTLR 文法独立写完。标识符区分大小写，最长 128 字节。

```text
CREATE GRAPH name
USE name
CREATE ALIAS name FOR GRAPH name
DROP ALIAS name
DROP GRAPH name
SHOW ALIASES

CREATE NODE TYPE name ( prop type [NULL], ... )
CREATE REL TYPE name ( prop type [NULL], ... )
ALTER NODE TYPE name ADD prop type NULL
ALTER REL TYPE name ADD prop type NULL

INSERT (a:NodeType {id: 1, ...})
INSERT (a)-[:RelType {prop: value}]->(b)
INSERT (a)-[:RelType@3 {prop: value}]->(b)

MATCH (a:NodeType {id: 1})-[:RelType]->(b)
  WHERE a.prop = $x
  RETURN b.id
  ORDER BY b.id
  LIMIT 10

MATCH (a:NodeType {id: 1})<-[:RelType]-(b) RETURN b.id
MATCH (a:NodeType {id: 1})-[:RelType]-(b) RETURN b.id

CALL ownership.largest_holder({
  start: 1,
  rel: "holder",
  ratioProp: "ratio",
  hops: 10
}) YIELD vid, ratio
```

`@3` 是 rank，不写就是 0。方向有 `->`、`<-` 和无向 `-`。阶段 4 再加 `-{1,10}`，上界大于 `max_hops` 时语义分析直接失败。`SET`、`DELETE` 放到阶段 5，形式是：

```text
MATCH (a {id:1})-[e:RelType]->(b {id:2})
SET e.ratio = 0.2

MATCH (a {id:1}) DELETE a
```

参数只允许标量，用 `$name`。不把参数拼进查询字符串。

阶段 6 的最短路不另造一套模式语法，用：

```text
CALL path.shortest({start: 1, end: 2, rel: "holder", hops: 10})
YIELD length, vids
```

从两端交替扩展。每一侧都用第 10.8 节的 visited 和两个点数上限。跳数到 `hops` 仍没在同一点相遇就返回空行，不改成全图搜索。

表达式在语义分析之后求值，第一版只做这些：

| 形式 | 规则 |
|---|---|
| 比较 `=` `<>` `<` `<=` `>` `>=` | 整数和浮点混比时先把整数转成双精度。字符串按 UTF-8 字节比 |
| `AND` `OR` `NOT` | 三值逻辑。`WHERE` 只留下真，未知当假丢掉 |
| `IS NULL` / `IS NOT NULL` | 唯一判断空值的写法。`= NULL` 的结果是未知 |
| `IN (标量, ...)` | 列表里出现空值时，那一项是未知 |
| `+` `-` `*` `/` | 除零返回 `DIVIDE_BY_ZERO`，不写成空值 |
| `id(n)` | 返回 vid |
| `labels(n)` `type(e)` | 返回类型名。阶段 3 一个点只有一个类型 |
| `properties(n)` | 返回当前 schema 下的全部属性，缺的可空列是空 |
| `startNode(e)` `endNode(e)` | 返回逻辑出边的起点和终点 vid |
| `length(path)` `size(list)` | 跳数或元素个数 |
| `coalesce(a, b, ...)` | 第一个非空 |

`head`、`last`、`nodes`、`relationships`、字符串函数、三角函数、列表和映射，有测试再加。地理函数不做。语义分析发现未知函数名就失败，不留到执行期。

### 10.12 错误码

| 码 | 含义 | 客户端 |
|---|---|---|
| `OK` | 成功 | 读结果 |
| `NAME_CONFLICT` | 空间名或别名已占用 | 不要重试 |
| `UNIQUE_VIOLATION` | 唯一索引撞上已有值 | 不要重试 |
| `GRAPH_NOT_FOUND` | USE 或别名目标不存在 | 不要重试 |
| `DROP_ALIAS_AS_GRAPH` | 用别名调用了 DROP GRAPH | 不要重试 |
| `TYPE_NOT_FOUND` | 点或边类型不存在 | 不要重试 |
| `VERTEX_DEGREE_EXCEEDED` | 删点时边太多 | 不要当成功 |
| `TRUNCATED` | 扫描超过每点上限 | 不要当成功 |
| `FRONTIER_EXCEEDED` | 普通 MATCH 下一跳点太多 | 缩小范围或改走持股算子 |
| `PATH_LIMIT_EXCEEDED` | 路径行数会超过 `max_paths` | 不要当成功 |
| `SCHEMA_MISMATCH` | 属性版本对不上目录 | 不要重试 |
| `CROSS_PART_UNSUPPORTED` | 阶段 9 之前边的两端落在不同分片 | 不要重试 |
| `DIVIDE_BY_ZERO` | 表达式除零 | 不要重试 |
| `QUERY_CANCELED` | 超时、断连或手动取消 | 可以重试 |
| `QUERY_MEMORY_EXCEEDED` | 这条查询超过 64 MB | 缩小查询 |
| `BUSY` | 查询池满，等待 50 ms 仍无空位 | 稍后重试 |
| `SYNTAX` | 文法不接受 | 不要重试 |
| `TYPE_MISMATCH` | 属性类型或参数类型不合 | 不要重试 |
| `FORBIDDEN` | 只读会话执行了写语句 | 不要重试 |
| `SPACE_READONLY` | 空间正在做副本同步 | 同步结束后再写 |
| `PART_READONLY` | 这条数据所在分片正在搬迁 | 稍后重试 |
| `NODE_NOT_DRAINED` | 节点还持有分片、会话或查询 | 先排空 |

### 10.13 内存预算

32 GB 机器的默认值：

| 项目 | 大小 |
|---|---|
| 操作系统和 JVM 元数据 | 4 GB |
| RocksDB block cache | 8 GB |
| 全部查询合计 `query_memory_bytes` | 4 GB |
| 单查询上限 | 64 MB |
| DuckDB `memory_limit` | 1 GB，只给最终聚合和排序 |
| 其余 | 留给堆、网络缓冲和页缓存 |

不配置操作系统 swap 来接查询。查询内存用每个 `QueryHandle` 上的计数器，分配路径、行、map 时增加，结束时减去。

### 10.14 类和阶段

| 阶段 | 先写这些类型 | 做完的标志 |
|---|---|---|
| 0 | `GraphDbServer`、`QueryHandle`、`ErrorCode`、`LineServer` | 第 10.16 节的 `ping` 返回 OK |
| 1 | `KeyCodec`、`RocksGraphStore`、`EdgeScanner` | 单机、一个数据目录，第 7 节阶段 1 的测试 |
| 2 | `Catalog`、`PropCodec`、`InsertExecutor` | 崩溃测试、别名测试、属性 get 次数为 0。分片规则只记录，不编码 |
| 3 | `GqlLexer`、`GqlParser`、`Binder`、`MatchExecutor` | 单跳 `->`、`<-`、`-` |
| 4 | `HopLoop` | 普通 10 跳在上限处失败 |
| 5 | `MutateExecutor`、`DuckResult` | `SET` / `DELETE` 原子提交，最终 `ORDER BY` 在 DuckDB |
| 6 | `CancelToken`、`OwnershipExecutor`、`ShortestPath` | 第 10.9 节的比例用例，以及 50 毫秒时限能停扫描 |
| 7 | `IndexCodec`、`IndexScan` | 等值和范围走 `index` 列族 |
| 8 | `SessionStore`、`Auth` | 用户、角色、带上限的子图 |
| 9 | `CompensationQueue`、`Membership` | 按第 7 节第 2 期的五条验收。跨分片边最终两个方向都在，或回退后两侧都没有 |

公开接口保持第 6 节的四个。`scanEdges` 的游标类型在第 6 节叫 `EdgeCursor`，实现类可以叫 `EdgeScanner`。方法签名里的字节数组都由 `KeyCodec` 产生，其它类不自己拼字节。`Catalog.graph(name)` 按第 10.5 节同时接受空间名和别名。

### 10.15 开工顺序

1. 写 `KeyCodec` 的往返测试：出边、入边、属性键、前缀、上界。
2. 写 `EdgeScanner`，用临时目录的 RocksDB。
3. 写第 10.9 节算子，输入先用内存里的假边，再换成扫描器。
4. 写 catalog 名字事务和第 10.11 节的文法。
5. 最后接网络和 DuckDB。

第 9 节的源码路径只在结果对不上时打开。对不上以本节的测试为准，不把旧实现的静默截断带进来。

### 10.16 连接协议和配置

阶段 0 就固定对外字节，后面只加字段，不换端口协议。TCP 上按行收 JSON，一行一条，UTF-8，换行结尾。默认端口 `19669`，避免和现成的 9669 抢端口。

```text
{"op":"ping"}
{"op":"execute","session":"","query":"RETURN 1","params":{},"timeoutMs":1000}
{"op":"cancel","session":"...","queryId":"..."}
```

`session` 为空时服务端分配一个，放进响应。阶段 8 之前不校验用户。会话放在内存里，空闲 8 小时删除。进程退出后会话消失，数据还在。

```text
{"code":"OK","session":"...","queryId":"...","columns":["id"],"rows":[[1]],"rowsAffected":0}
{"code":"TRUNCATED","session":"...","queryId":"...","message":"hop 4 vid 18 examined 65"}
```

失败响应不带半截 `rows`。`message` 给人看，程序看 `code`。

配置用一个 properties 文件，启动时读完，运行中不热更新。

```text
port=19669
data_dir=data
query_threads=16
max_execution_time_ms=1000
max_edges_per_vertex=64
max_frontier_per_hop=64
max_hops=10
max_paths=100
query_memory_bytes=67108864
block_cache_bytes=8589934592
duckdb_memory=1GB
duckdb_temp_directory=tmp/duck
min_edge_ratio=0.01
min_accum_ratio=0.001
```

32 GB 机器按第 10.13 节填 `block_cache_bytes`。开发机把块缓存降到 256 MB，不把开发机的缓存大小写进性能测试结论。

`FLUSH` 和 `COMPACT` 不进 GQL。用同一条连接发：

```text
{"op":"admin","command":"flush"}
{"op":"admin","command":"compact"}
```

这两条调用 RocksDB 对应列族的 flush 和 compact，期间查询仍可按快照读。加副本、搬分片和计算节点的命令写在第 7 节第 2 期，第 1 期不实现。

### 10.17 索引

索引记录在 catalog：

```text
key: 0x30 + graphId(2) + indexId(4)
value: 名字、目标 kind、typeId、属性下标列表、唯一标记 uint8
```

`index` 列族的键：

```text
indexId(4) + 属性编码 + graphId(2) + 身份
```

身份：点是 `tagId + vid`，边是 `edgeTypeId + rank + outSrc + outDst`。值是空字节。

属性编码要能按字节排序。INT64 先把符号位取反再按大端写。DOUBLE 按可排序的 8 字节写：负数取反全部位，非负数只取反符号位。STRING 第一版只做等值，键是 `uint32 长度 + UTF-8`，不做字符串范围。NULL 不写入索引。唯一索引在同一个写批次里先查键，已存在则整批失败，错误码 `UNIQUE_VIOLATION`。

写入点或边时，索引键和数据键在同一个 `WriteBatch`。`WHERE` 的等值或数值范围能覆盖索引前缀时走 `scanIndex`，否则才扫点或边。重建索引扫 `vertex` 或 `prop`，按第 10.10 节取消，建完前查询仍走原来的扫描。

### 10.18 跨分片补偿

阶段 9 之前不要写这一节的键。出边所在分片是 `partOf(outSrc)`，入边所在分片是 `partOf(outDst)`。相同就用第 10.6 节的一个批次，不写 `txn`。

不同分片时，一条逻辑边的状态放在 `txn` 列族，键是：

```text
graphId(2) + outPartId(2) + edgeTypeId(4) + rank(8) + outSrc(8) + outDst(8)
value: state(1) + 第 10.3 节的属性字节
state: 1 = 出边已写、入边未确认，2 = 正在回退
```

步骤：

1. 在出边分片的一个批次里写出边键、`prop` 键、`state = 1` 的 txn 键。提交成功才继续。
2. 在入边分片的一个批次里写入边键。
3. 回到出边分片，删掉 txn 键。这一步提交之后，语句才返回 `OK`。
4. 第 2 步失败就把 txn 改成 `state = 2`，再删出边键、`prop` 键和 txn 键。回退完成返回失败，两侧都不能剩下键。

后台每 10 秒扫 `txn`。`state = 1` 的重做第 2 步和第 3 步。`state = 2` 的重做第 4 步。重做是幂等的：键已经在就当作这一步成功。查询在第 3 步之前可能只看见出边。接口说明里写上这句，测试要覆盖「第 2 步之后进程退出，重启后入边被补上」和「第 2 步失败，重启后两侧都没有」。

有 3 个投票成员以后，每个分片上的那个批次先进入提交日志，多数派确认后再写进本地 RocksDB。成员、只读窗口和搬迁按第 7 节第 2 期。不把 Nebula 的 Raft 实现翻译过来。

### 10.19 编码时不要自己补的行为

第 5 节已经列出不移植的范围。写到对应阶段之前，下面这些直接拒绝，不要做静默兼容：

- `BEGIN`、`COMMIT`、跨语句事务。
- `GO`、管道、`YIELD` 查询语法。`CALL ... YIELD` 只用于第 10.11 节和第 10.20 节列出的过程。
- 所有路径、无上限的变长展开、抽样。
- 全文、地理、Zone、Listener，以及 Nebula 那种整空间 `DATA_BALANCE`。分片搬迁只按第 7 节第 2 期，第 1 期不实现。
- 达到 `max_edges_per_vertex` 或 `max_frontier_per_hop` 时返回少一些行并报成功。
- 把穿透持股写成「先收集路径再聚合」。
- 用 `Thread.stop`、杀进程或操作系统 swap 来处理超时和超内存。

### 10.20 阶段 8 的用户和子图

阶段 8 之前，`execute` 不校验用户。阶段 8 起，先登录再执行。

```text
key: 0x40 + utf8(user)
value: 盐 16 字节 + PBKDF2-HMAC-SHA256 哈希 32 字节 + role uint8
```

迭代次数 600000。密码不明文落盘。role 1 是只读，role 2 是读写。只读会话遇到 `INSERT`、`SET`、`DELETE`、`CREATE`、`DROP`、`ALTER` 时返回 `FORBIDDEN`。

```text
{"op":"login","user":"u","password":"p"}
```

成功后会话绑定这个用户。`DROP GRAPH` 只允许读写角色。别名不单独授权，权限看 graphId 上的角色。第一版只有这两种角色，授权粒度是整张图。

有上限的子图：

```text
CALL subgraph.bounded({start: 1, rel: "holder", hops: 3}) YIELD vid
```

执行用第 10.8 节的 visited 和两个点数上限。结果只有 vid。超过上限返回 `TRUNCATED` 或 `FRONTIER_EXCEEDED`。

---

## 11. 开工前的需求澄清记录

2026-10-27 开工前逐条确认。与前文冲突时以本节为准。

### 11.1 工程坐标

- 仓库 `cengyan`（层岩图数据库），远程 `https://github.com/a2king/cengyan.git`。推送走代理 `http://127.0.0.1:7890`（`HTTP_PROXY` / `HTTPS_PROXY`）。
- Maven `groupId=io.cengyan`，根包 `io.cengyan`。
- 模块：`cengyan-store`、`cengyan-catalog`、`cengyan-parser`、`cengyan-query`、`cengyan-server`，另加 `cengyan-cli`（第 11.6 条）。第 6 节的 `graphdb-*` 一律按此替换。
- JDK 用本机 `D:\workspace\code_env\jdk21`，Maven 3.9.16。

### 11.2 推进方式

- 从第 0 期连续做到第 8 期，中途不停下等确认。第 9 期（分片、副本、计算节点）不编码。
- 每完成一期提交一次 git，commit message 用中文，并推送到远程。
- 每期的 JUnit 测试通过才进入下一期。

### 11.3 GQL 文法

- 不引入 opengql 等外部文法文件。按第 10.11 节手写一份小的 ANTLR4 文法，关键字和模式写法对齐 ISO GQL。
- 关键字不区分大小写；标识符区分大小写。
- 标识符只允许英文字母、数字、下划线，不能以数字或下划线开头，不允许中文。最长 128 字节。第 10.9、10.11、10.20 节示例里的边类型名「股东」据此改为 `holder`。

### 11.4 语句语义补充

- 插入边时端点的 id 写在点模式里：`INSERT (a:Company {id:1})-[:holder {ratio:0.5}]->(b:Company {id:2})`。a 是起点，b 是终点，中括号里是边属性。点模式带类型时顺带写点记录；只写 `({id:1})` 时只写边不写点。不采用「先 MATCH 再 INSERT」。
- 第 7 期之前起点不带 id 的 `MATCH (a:Company) RETURN a.id LIMIT 10` 允许执行：按 `graphId + partId` 前缀扫 `vertex` 列族，受语句 `LIMIT` 和 `max_edges_per_vertex` 同一个安全上限约束，超过返回 `TRUNCATED`。
- 变长跳 `{m,n}` 的结果是第 m 到第 n 跳每一跳新到达的点的并集，每个点只出现一次，按首次到达的跳数计；定长 `{n}` 就是第 n 跳的 frontier。与 Cypher 语义一致。
- DATETIME 字面量按 ISO GQL 写 `DATETIME '2024-01-31T12:00:00Z'`；不带时区的 `'2024-01-31 12:00:00'` 按 UTC 解释。返回时输出 ISO 字符串。
- 补上目录查看语句，第 2 期一起做：`SHOW GRAPHS`、`SHOW NODE TYPES`、`SHOW REL TYPES`、`DESCRIBE NODE TYPE x`、`DESCRIBE REL TYPE x`。
- 第 7 期索引语句：`CREATE [UNIQUE] INDEX idx ON NODE TYPE Company (name)`、`CREATE [UNIQUE] INDEX idx ON REL TYPE holder (ratio)`、`DROP INDEX idx`、`SHOW INDEXES`。重建是运维动作，走 admin 命令 `{"op":"admin","command":"rebuildIndex","index":"idx"}`，不进 GQL。
- `RETURN b` 返回整个点时，JSON 形如 `{"id":1,"type":"Company","properties":{...}}`；返回整条边形如 `{"type":"holder","rank":0,"src":1,"dst":2,"properties":{...}}`。

### 11.5 网络、序列化、日志

- 网络层用 Netty，按行解码 JSON；查询仍提交到第 10.1 节的固定大小查询池，不在 Netty 的 I/O 线程里扫 RocksDB。
- JSON 用 Jackson（流式 API，`ObjectMapper` 全局复用）。
- 日志用 SLF4J + Logback。
- 代码注释、日志、响应 `message` 用中文；类名、方法名、错误码保持英文。

### 11.6 命令行客户端

- 增加 `cengyan-cli` 模块：读一行 GQL，包成 `execute` JSON 发到服务端，表格打印结果；以 `:` 开头的行当作 admin 命令（如 `:flush`、`:compact`）。
- `mvn package` 产出 `cengyan-server` 和 `cengyan-cli` 两个可直接运行的 jar。

### 11.7 性能测试

- JUnit 测试里自己计时并断言 p99，用 `@Tag("perf")` 标记，默认 `mvn test` 一起跑。
- 每次结果追加写入 `docs/perf-report.md`，记录：机器型号、数据量、是否热缓存、块缓存大小、堆大小、p50、p99。
- 测试用的 RocksDB 块缓存按开发机设置（256 MB 以内），不把开发机数值写成生产结论。

### 11.8 第 8 期用户

- 空库第一次启动若没有任何用户，按配置 `root_password`（缺省 `cengyan`）创建读写用户 `root`。
- 建用户、改密码、设角色走 admin 命令（`createUser`、`setPassword`、`setRole`、`dropUser`），不进 GQL。

### 11.9 本次确认的问题清单

| # | 问题 | 结论 |
|---|---|---|
| 1 | groupId、根包、模块名 | `io.cengyan`，`cengyan-*` |
| 2 | 推进方式 | 连续做到第 8 期 |
| 3 | 版本管理 | 远程 `a2king/cengyan`，走代理，按期提交推送 |
| 4 | 文法来源 | 手写小文法，不引外部文件 |
| 5 | INSERT 边的端点 | id 写在点模式里，a 起点 b 终点 |
| 6 | MATCH 起点无 id | 允许，受安全上限约束 |
| 7 | 网络与 JSON | 高并发框架，性能优先：Netty + Jackson |
| 8 | 语言 | 注释、日志、message 中文；标识符英文 |
| 9 | 性能测试 | JUnit 计时 + `@Tag("perf")`，报告写 `docs/perf-report.md` |
| 10 | DATETIME 字面量 | ISO GQL 形式，返回 ISO 字符串 |
| 11 | 变长跳结果 | 各跳新点并集，Cypher 语义 |
| 12 | CLI | 做 `cengyan-cli` |
| 13 | 初始用户 | 启动自动建 `root`，用户管理走 admin |
| 14 | 索引语句 | `CREATE [UNIQUE] INDEX ... ON NODE/REL TYPE`，重建走 admin |
| 15 | 目录查看 | 补 `SHOW GRAPHS` / `SHOW NODE TYPES` / `SHOW REL TYPES` / `DESCRIBE` |
| 16 | 标识符 | 仅英文、数字、下划线，不以数字和下划线开头 |
