# 与 Neo4j 执行器的查询优化差异和改造计划

对照 Neo4j Cypher 25 的算子体系（官方执行计划文档），梳理层岩现有执行器的差距。
本文只做差异分析和计划，不改代码。现状以当前仓库代码为准。

阅读前提：`docs/gql/04-查询.md`（语义）、`docs/gql/10-改造路线图.md`（功能路线）。
本文是执行器层面的补充，聚焦「同一句 GQL 的执行代价」。

## 0. 两边的执行模型先对齐

| | Neo4j | 层岩 |
|---|---|---|
| 计划形态 | 逻辑计划 → 物理计划（算子树，EXPLAIN/PROFILE 可见） | 无计划对象，`MatchExecutor`/`MutateExecutor` 按语句形状直接执行 |
| 优化入口 | COST planner，多规则改写 + 基数估计选计划 | 语义分析期固定几条：`constantId`、`push()`、`IndexLookup` |
| 执行方式 | 算子流水线，惰性拉取，支持并行分区 | 递归收集 `List<Hop>`，全量收集后再交给 DuckDB 排序聚合 |
| 可观测 | `PROFILE` 给 DB Hits / Rows / 内存 | 无等价物。`:stats` 只有全库计数 |

关键差别：Neo4j 把「怎么执行」显式建模成计划，规则改写才有落点；层岩没有计划对象，所有优化都发生在「语义分析 + 执行循环开头」这两个地方。**做谓词下推之前不必先造完整优化器，但每加一条下推规则都要有明确的落点。**

## 1. 定位算子：从「模式属性里抠 id」到多种 seek

Neo4j 叶子算子一系，按定位强度从高到低：

| Neo4j 算子 | 触发条件 | 层岩现状 |
|---|---|---|
| NodeByIdSeek / NodeByElementIdSeek | `WHERE id(n) = 0` 或 `elementId(n) = '4:uuid'` | ❌ 无。内联 `{id:...}` 走点查；WHERE 里的 `id()` 条件落残差 |
| NodeIndexSeek | `WHERE n.prop = 值` 有索引 | ✅ 部分等价。`IndexLookup.vertexIds` 支持等值+范围，但只接边模式的起点；只查点不查 IndexLookup |
| NodeIndexSeekByRange / Contains / EndsWith | STARTS WITH、范围 | 部分。范围有，前缀/包含无（且字符串范围本来不走索引） |
| NodeByLabelScan | `MATCH (n:Label)` 无索引无 id | ✅ 等价。`scanVertices` 按 tagId 过滤 |
| AllNodesScan | `MATCH (n)` | ✅ 等价，且起点无类型时直接报错，比 Neo4j 更保守 |

**差距 1（本次的核心）：WHERE 里的 `id(v) = '...'` 不转点查。**
现状链路：`nodes()`/`run()` → `constantId()` 只看模式属性 `{id:...}`；WHERE 的合取项进 `push()`，`ref()` 只认 `target.id`/`edge.rank` 两种形状且限 long 字面量。结果 `where id(v)="..."` 落到「全类型扫 + 每点属性解码 + 每点一次外部 ID 反查」。
Neo4j 对应：`NodeByIdSeek`，1 次 DB hit。
改法落点：在 `nodes()` 和 `run()` 里，进扫描前从 WHERE 合取项里提取 `id(起点变量) op 常量`（先支持 `=`，可扩展 `IN` 列表）。等价重写：
- `op` 为 `=`：调 `findVertexId` 得内部 vid，当作 `startId`，与现有 `constantId` 通道汇合；
- 剩余条件（含 `IN` 展开后的）留作残差，不影响正确性。
- 同理多跳模式的终点也可以带 `id(target)=常量`（把 `ref()` 认 `Expr.Call("id", Variable)` 且比较对象是常量）。

**差距 2：属性索引只在边模式的起点上被考虑。**
`nodes()`（只查点）根本不调 `IndexLookup`。`MATCH (a:org {regcap > 1e8}) RETURN a` 依然全类型扫。
改法落点：`nodes()` 里把 `filterStarts` 之前插一段 `IndexLookup.vertexIds`（带窗口语义：取满 window 即停，见差距 5），并保证该列在索引里 `ready`。

**差距 3：属性过滤先全量加载后过滤。**
Neo4j 有专门的 `Cache Properties`（对已在计划里的行批量取属性）和 `Filter` 协同；层岩在 `nodes()` 里对每个起点先 `vertexProps`（27 列全解码）再判 WHERE。1000w 点 × 27 列解码是主要成本。
改法落点：属性过滤分两级——能下推到 `scanVertices` 的谓词（等值/范围，值已物化在点键值里）在扫描器里判；不能下推的才在收集后判。扫描器里需要拿到 `PropDef` 和解码函数，这会把 store 层 API 加宽（值比较发生在 `RocksReadSession`），换取扫描量从 N 降到命中行。Neo4j 的 Filter 虽然也是独立算子，但它的属性缓存让重复属性读只发生一次；层岩的一次 get 对应一次 RocksDB get，成本口径不同，不可直接照搬。

## 2. 边扫描：已具备的与缺的

先说已经对齐的，避免重复建设：

- **方向语义**：出边/入边双向键，`-` 无向去重（`names.seen`）——等价 Expand(All)，且边扫描带 `remaining` 窗口（MatchExecutor.java:133/157），LIMIT 对边是生效的。
- **边上谓词下推**：rank 与终点 id 过滤在扫描器内判断（`push()` → `scanEdges` 的 predicate 参数）——等价「Filter 与 Expand 融合」。
- **edge 索引**：`IndexLookup.edges` 存在，当边属性有索引时先索引定位边（差距 2 的边侧版本）。

缺的：

**差距 4：多跳模式的起点收集没有窗口。**
`multi()` 同样以 `scanVertices(..., -1, ...)` 收全部起点（`starts()` 复用同一段）。一跳里传下去的 `remaining` 在多跳第一跳之前就丢了。
改法落点：多跳起点收集阶段对 window 的处理与 `nodes()` 一致（见差距 5）。

**差距 5：点扫描不传 LIMIT/SKIP 窗口（上次已确认）。**
`scanVertices(graphId, tagId, -1, maxExamined, cancel)` 的 `queryLimit=-1` 写死。存储层已支持 `queryLimit` 找满即停（RocksReadSession.java:159-161）。
改法落点：`nodes()` 里当点模式**无属性过滤**时，把 `window()` 作为 queryLimit 传下去；有属性过滤时保持 -1（语义是先过滤再 LIMIT，传下去会少行）。
风险点：`filterStarts` 在窗口之后执行，把窗口传下去后，与「先收满 SKIP+LIMIT 再过滤」的现行为相比结果可能不同（过滤掉的行也计窗口）。需要明确语义：窗口内过滤掉的行是否占窗口。Neo4j 的语义是 Limit 算子只裁输出行数，过滤不占窗口。建议按 Neo4j 语义执行：**窗口只数「过滤后」的行**——存储层扫到 window 个候选就停，但保留现行的收集后二次过滤，即存储传回 ≥ window 行、执行层裁到 window。更简单的口径：无属性过滤时才传窗口，此时两者无差别；有过滤时保持全扫。**本差距先按「无属性过滤才下推」实现，语义零风险。**

**差距 6：无 Cartesion/Apply/Join 一系的连接算子。**
Neo4j 用 Apply/NodeHashJoin/Triadic 系列解决多模式连接；层岩一条语句只支持一个点或一条边（04-查询.md 明确「一条语句里不能写两段边」）。这是 GQL 功能面（路线图第二阶段「多模式连接」）的事，不在执行器优化范围内，但做第二阶段时这些落点要预留：起点收集的抽象要能接「多个起点源」。

## 3. 执行方式：流式 vs 全量收集

**差距 7：结果收集是全量的，LIMIT 只裁输出。**
`hops` 列表收集全部匹配（除边扫描带 remaining 外），收集完才进 `finish()` 排序/去重/分页。1000w 点的 `MATCH (a:org) RETURN a.id` 没写 LIMIT 时堆里是千万级 Hop + List。
Neo4j 管线里 ProduceResults 是边产出边送客户端，Limit 算子找满即停（还有 Top 算子）。
层岩的约束：排序/去重要等全部行（DuckDB 处理），所以「无 ORDER BY + 有 LIMIT」才能安全早停——语义上 LIMIT 就是截断输出，找满 window 行即可停扫描。这与差距 5 是同一个改动的两个表述：有窗口就提前停，无窗口才全量收。
注：现行为「无 ORDER BY 有 LIMIT 时一跳先收满 SKIP+LIMIT 行再排序去重」是 `04-查询.md` 里写明的一跳语义，保留不变；实现上把「收满即停」做成扫描提前终止即可。

**差距 7 附带一个内存改善**：`starts()` 返回的 `List<VertexId>` → `List<Long>` 再生成 Hop，全程 3 个千万级列表。提前终止后这些列表天然变小。

## 4. 可观测性：差距 8

Neo4j 有 EXPLAIN/PROFILE（DB Hits、Rows、估算行数、索引使用），层岩没有等价物。`docs/gql/04-查询.md` 里「扫描是否有上限由预算决定，返回 TRUNCATED」是仅有的执行信息。
做下推规则时最大的工程风险是「用户不知道自己的谓词到底推没推下去」。建议随差距 1/2/5 一起交付一个轻量 `EXPLAIN`：
- 输出当前语句的执行决策：起点定位方式（点查/索引/全类型扫）、扫描行数预算、下推的谓词列表、残差谓词列表；
- 一行一条 JSON 的协议不变，结果就是普通 QueryResult（一列 decision 文本），客户端零改动；
- 不做基数估计（Neo4j 的 Estimated Rows 依赖统计，层岩没有统计基础设施），只报**决策**，不报**代价**。
这个对调试和验证下推规则本身也是必需的。

## 5. 实施顺序

按「收益 / 风险 / 依赖」排：

| 序 | 项 | 对应差距 | 收益 | 风险 | 依赖 |
|---|---|---|---|---|---|
| P0 | WHERE `id(v)=常量` 转点查 | 1 | 已知 ID 查询从全扫变 O(1)；Nebula 式 `IN` 列表顺带支持 | 低，等价重写 | 无 |
| P0 | 点扫描带窗口（无属性过滤时） | 5/7 | LIMIT 生效于扫描，抽样查询不再全图遍历 | 低，语义口径已定 | 无 |
| P1 | 只查点走属性索引 | 2 | `MATCH (a:org {regcap:...})` 类查询走索引 | 中，需处理索引 not ready | 差距 5 的窗口口径 |
| P1 | 轻量 EXPLAIN | 8 | 下推可见、可调试 | 低 | 随 P0 交付 |
| P2 | 点属性过滤下推到扫描器 | 3 | 全属性列过滤不再全量解码 | 高，动 store API | 差距 2、5 |
| P2 | 多跳起点窗口 | 4 | 多跳抽样查询降级为有界扫描 | 中 | 差距 5 |

P0 两项都是「语义零变化、只改执行决策」的等价重写，且互相独立，可以并行做。做完后：

- `match (v:org {id:'...'}) return v` 与 `match (v:org) where id(v)='...' return v` 走同一条点查路——两种写法只差一次函数调用解析，不再差一次全图扫描；
- `match (a:org) return a limit 10` 真正扫描 10 个键。

P1/P2 涉及索引可用性与 store API 加宽，等 P0 验收后单独立项。差距 6（连接算子）不进本计划，归入路线图第二阶段。

## 6. 与文档的同步

这些差距修完后 `docs/gql/` 需要同步：

- `04-查询.md`：「只写类型不写 id 按类型扫描」一节补充 LIMIT/SKIP 何时生效于扫描（现文档只说 LIMIT 是结果窗口，未提扫描提前终止——修完 P0 后这句要改）；
- `00-和标准相比.md`：若 EXPLAIN 交付，补一节「扩展：EXPLAIN」；
- `04-查询.md` 的「从全图按关系取 100 条」一节，在 P1/P2 之后重写（边模式起点索引定位）。
