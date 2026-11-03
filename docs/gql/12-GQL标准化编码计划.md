# GQL 标准化编码计划

状态：六个阶段的编码已经落地。验收时对照本文第 4 节的破坏性变更，并跑 `mvn -pl cengyan-query -am test`。

对照 ISO/IEC 39075（GQL）、Neo4j Cypher 与 NebulaGraph nGQL 做完差距分析后制定。差距矩阵见本次对照 canvas；功能路线见 [10-改造路线图](10-改造路线图.md)；执行器差距见 [11-执行器优化差距](11-执行器优化差距-与Neo4j对照.md)。本文是可执行的编码计划，按阶段推进，每阶段独立可编译、可测试、可提交，阶段结束停下验收。

## 0. 已确认的决策

| # | 决策 | 结论 |
|---|---|---|
| 1 | 语法路线 | 只跟 ISO GQL 标准。不接受 Cypher / nGQL 方言（`*1..3`、`!=`、`WITH`、`STARTS WITH` 等一律报错） |
| 2 | 本轮范围 | 缺陷修复、模式层、组合层、表达式层、写入与 DDL。类型层（DATE/TIME/DURATION、列表属性、多标签存储）留下一轮 |
| 3 | ORDER BY + LIMIT | 改成标准语义：有 ORDER BY 时全量收集后排序再截断；无 ORDER BY 时保留收满即停 |
| 4 | 删点 | 按标准：默认 `NODETACH`，点上有边报错；`DETACH DELETE` 连边删 |
| 5 | 标识符 | 只保留标准保留字，其余关键字按上下文识别；支持反引号分隔标识符，反引号内允许中文和任意字符 |
| 6 | 变长路径行语义 | 每条路径一行；默认 `WALK`；支持 `TRAIL` / `SIMPLE` / `ACYCLIC` 关键字；`SIMPLE` 允许回到起点闭环但不再继续；去重由用户写 `DISTINCT` |
| 7 | 无上界与 LIMIT | 允许 `*` / `+` / `{m,}`，上界由 `max_hops` 预算兜底（预算为 0 时必须显式写上界）；路径查询的 `LIMIT` 改回截断语义、不再必填，`max_paths` 超出才报错 |
| 8 | 过程调用 | 位置参数和单个映射参数都接受；`YIELD` 之后可接 `FILTER` / `RETURN` 等 |
| 9 | 多语句 | 服务端一次请求接受分号分隔的多条语句，顺序执行，返回最后一条的结果；不做跨语句事务 |
| 10 | 无类型起点 | 允许 `MATCH (a)`，按全部点类型依次扫；支持 `:A\|B`、`:!A`、`:%` 标签表达式；LIMIT 窗口提前停，预算兜底 |
| 11 | 执行器架构 | 逻辑计划 + 算子树流式执行；图遍历在 Java 算子里 |
| 12 | DuckDB 分工 | 排序/OFFSET/LIMIT、分组聚合与 DISTINCT、集合运算、无共享变量的连接与大哈希连接、`NEXT` 段间缓冲全部交给 DuckDB（可落盘），以性能换稳定 |
| 13 | 交付物 | parser / query 代码、`docs/gql/*.md` 全部重写、`Gql.g4` 同步、Studio 与 Python CLI 跟进、单元测试 + 端到端语法回归。`dev-scripts` 和 README 示例不在本轮 |
| 14 | 节奏 | 6 个阶段，每阶段可独立提交，阶段结束等验收 |
| 15 | 标准缺失但常用的能力 | 以层岩扩展函数提供（`starts_with`、`contains`、`regex_match`、`nodes`、`relationships`、`labels`、`properties`、`type`），不加关键字，文档单独标注 |

## 1. 目标语法清单

写法以 GQL 标准为准。标注「扩展」的是层岩自有。

### 1.1 词法

- 保留字只含标准保留字：`MATCH OPTIONAL RETURN FILTER LET FOR IN ORDER BY OFFSET SKIP LIMIT NEXT UNION EXCEPT INTERSECT INSERT SET REMOVE DELETE DETACH NODETACH CALL YIELD WHERE AND OR NOT XOR IS NULL TRUE FALSE DISTINCT ALL ANY SHORTEST WALK TRAIL SIMPLE ACYCLIC CASE WHEN THEN ELSE END EXISTS CAST AS ASC DESC GROUP USE CREATE DROP ALTER GRAPH NODE REL TYPE INDEX ALIAS SHOW DESCRIBE EXPLAIN`。
- 其余（`comment`、`parts`、`add`、`on`、`for`、`unique`、`types`、`indexes`、`id`、`bool`、`int`、`string`、`double`、`datetime` 等）降为上下文关键字，可做属性名、变量名、类型名。
- 反引号标识符 `` `持股比例` ``、`` `order` ``；内部双写反引号转义。
- 字符串转义：`\\`、`\'`、`\"`、`\n`、`\t`、`\r`、`\uXXXX`；保留双写引号。
- 数字：`1e8`、`1.5E-3`、`0x1F`、`0o17`、`0b101`、`1_000_000`。
- 多语句：`statement (';' statement)* ';'?`。

### 1.2 模式层

```text
MATCH (a:org)-[e:inv]->(b)-[:inv]->(c)                       -- 多段
MATCH (a:org {id:1}), (b:org {id:2})                         -- 多模式（逗号）
MATCH (a:org) MATCH (a)-[:inv]->(b)                          -- 多个 MATCH
OPTIONAL MATCH (a)-[:inv]->(b)
MATCH (a:org|person) / (a:!org) / (a:%)                      -- 标签表达式
MATCH (a:org WHERE a.regcap > 1e8)                           -- 内联 WHERE
MATCH (a)-[e:inv]->{1,3}(b) / {2,} / * / +                   -- 量词
MATCH p = TRAIL (a)-[:inv]->*(b)                             -- 路径模式
MATCH p = ANY SHORTEST (a)-[:inv]->*(b)                      -- 路径搜索前缀
MATCH p = ALL SHORTEST / SHORTEST 3 / ANY 5 (a)-[:inv]->*(b)
MATCH ((a)-[e:inv]->(b) WHERE e.ratio > 0.1){1,3}            -- 量化路径模式，内部可过滤
MATCH (a)-[e:inv]->{1,3}(b) RETURN e                         -- e 绑定为边列表
```

### 1.3 组合层

```text
MATCH ... FILTER expr LET x = expr FOR y IN list RETURN ... GROUP BY ... ORDER BY ... OFFSET n LIMIT m
RETURN ... NEXT MATCH ... RETURN ...                          -- 线性复合
query UNION [ALL|DISTINCT] query / EXCEPT / INTERSECT
RETURN *  /  RETURN DISTINCT  /  count(DISTINCT x)
聚合：count sum avg min max collect_list stddev_samp stddev_pop percentile_cont percentile_disc
```

隐式分组：`RETURN a.name, count(*)` 按非聚合列分组，与显式 `GROUP BY` 等价。

### 1.4 表达式层

```text
CASE x WHEN 1 THEN 'a' ELSE 'b' END  /  CASE WHEN c THEN ... END
[1, 2, 3]  {k: v}  list[0]  list[1:3]  x IN [1,2]  x IN $ids
s || t  CAST(x AS INT)  COALESCE  NULLIF
CHAR_LENGTH UPPER LOWER TRIM LTRIM RTRIM BTRIM LEFT RIGHT
ABS MOD FLOOR CEIL SQRT POWER EXP LN LOG LOG10
SIZE(list) PATH_LENGTH(p) ELEMENT_ID(x)
x IS [NOT] NULL  x IS [NOT] TYPED INT  n IS [NOT] LABELED org
n IS SOURCE OF e  n IS DESTINATION OF e  SAME(a, b)  ALL_DIFFERENT(a, b, c)  PROPERTY_EXISTS(n, p)
EXISTS { MATCH (a)-[:inv]->(b) }
扩展函数：starts_with contains ends_with regex_match nodes relationships labels properties type id
         startNode endNode（改为返回节点）
```

### 1.5 写入与 DDL

```text
INSERT (a:org {id:1}), (b:org {id:2}), (a)-[:inv {ratio:0.5}]->(b)   -- 多元素
MATCH (a:org {id:1}) INSERT (a)-[:inv]->(b:org {id:3, name:'x'})     -- 引用匹配变量
MATCH (a)-[e]->{1,3}(b) SET b.flag = true                            -- 多跳后修改
SET a = {name:'x', note:NULL}                                        -- 整体赋值
REMOVE a.note
MATCH (a {id:3}) DELETE a                                            -- NODETACH，有边报错
MATCH (a {id:3}) DETACH DELETE a
DROP NODE TYPE org / DROP REL TYPE inv                               -- 扩展
CREATE GRAPH g IF NOT EXISTS / DROP INDEX i IF EXISTS / CREATE NODE TYPE t IF NOT EXISTS ...
ALTER NODE TYPE org DROP note / ALTER NODE TYPE org RENAME note TO memo   -- 扩展
```

## 2. 架构

```text
GQL 文本 ──GqlReader──▶ Stmt/Expr AST ──Planner──▶ LogicalPlan ──Executor──▶ 行流 ──▶ QueryResult
                                                       │
                                                       └─ 排序/聚合/集合/大连接/NEXT 缓冲 ──▶ DuckDB
```

### 2.1 包与文件

| 包 | 新增 / 改动 | 职责 |
|---|---|---|
| `io.cengyan.parser` | 改 `GqlReader`、`Stmt`、`Expr`、`Gql.g4`；新增 `Lexer`（从 `GqlReader.tokenize` 拆出） | 词法、语法、AST |
| `io.cengyan.query.plan` | 新增 `LogicalPlan`、`PlanNode`（密封接口）、`Planner`、`PlanPrinter` | AST → 算子树，EXPLAIN 打印 |
| `io.cengyan.query.exec` | 新增 `Row`、`RowSchema`、`Operator`（迭代器接口）、各算子类、`Values`（Node / Edge / Path / List / Map 值对象） | 流式执行 |
| `io.cengyan.query.exec.duck` | 新增 `DuckSink`、`DuckSort`、`DuckAggregate`、`DuckSetOp`、`DuckJoin`、`DuckBuffer`；改造现有 `DuckResult` 为公共连接与类型映射 | DuckDB 卸载 |
| `io.cengyan.query` | `QueryEngine` 改为走 Planner；`MatchExecutor` 拆解为叶子算子后删除；`MutateExecutor` 改为消费算子树输出；`ExprEval` 扩展；`ProcedureExecutor` 变成 `ProcedureCall` 算子的实现表 | 入口与写入 |
| `io.cengyan.catalog` | `Catalog` 增加 `dropType`、`dropProperty`、`renameProperty`、`IF EXISTS` 判断 | DDL |
| `io.cengyan.store` | `ReadSession` 增加 `scanAllVertices(graphId, queryLimit, maxExamined, cancel)`（跨类型）、`countEdges(graphId, vid)`（NODETACH 校验）；`GraphStore` 增加按类型前缀删除 | 存储 API |
| `io.cengyan.server` | `ExecuteService` 支持多语句；`QueryEngine.execute` 返回最后一条结果 | 协议 |

### 2.2 行模型与值

- `Row`：`Object[]`，列位置由 `RowSchema`（变量名 → 下标，含类型提示）决定。所有算子按 `RowSchema` 读写，不再按「起点/边/终点」三元组硬编码。
- 值类型：`NodeValue(vid, typeId, props 延迟加载)`、`EdgeValue(src, dst, typeId, rank, props 延迟加载)`、`PathValue(nodes, edges)`、`ListValue`、`MapValue`、标量。属性延迟到第一次访问时解码，解码结果缓存在值对象上（对应 Neo4j 的 Cache Properties）。
- NULL 传播与三值逻辑沿用 `ExprEval`。

### 2.3 算子

| 算子 | 来源 | 说明 |
|---|---|---|
| `NodeByIdSeek` | 现 `constantId` / WHERE `id(v)=常量` | 点查 |
| `NodeIndexSeek` | 现 `IndexLookup.vertexIds` | 等值与范围 |
| `NodeByTypeScan` | 现 `scanVertices` | 带 LIMIT 窗口 |
| `AllNodesScan` | 新 | 按全部类型依次扫，标签表达式在这里过滤类型集合 |
| `Expand` | 现 `scanEdges` 一跳 | 方向、类型集合、rank/终点 id 下推 |
| `VarExpand` | 现 `HopLoop` / `PathAll` | 量词、路径模式（WALK/TRAIL/SIMPLE/ACYCLIC）、每条路径一行、边列表绑定、量化模式内部 WHERE；`max_hops` / `max_frontier_per_hop` / `max_paths` 预算 |
| `ShortestPath` | 现 `ShortestPath` | `ANY SHORTEST` / `ALL SHORTEST` / `SHORTEST k` / `ANY k`；BFS 分层 |
| `Filter` | 新 | 残差谓词；`EXISTS {}` 子查询在此调用子计划 |
| `Project` | 新 | RETURN 项、LET |
| `Unwind` | 新 | FOR |
| `Apply` | 新 | 左行驱动右子树（多个 MATCH 共享变量、OPTIONAL MATCH、带相关变量的 EXISTS）；右侧为空时 OPTIONAL 补 NULL 行 |
| `ProcedureCall` | 现 `ProcedureExecutor` | YIELD 列进入 `RowSchema` |
| `Limit` | 新 | 无 ORDER BY 时的早停 |
| `DuckSort` | 新 | ORDER BY / OFFSET / LIMIT；Top-N 由 DuckDB 自己做 |
| `DuckAggregate` | 新 | GROUP BY、聚合函数、DISTINCT、`count(DISTINCT)` |
| `DuckSetOp` | 新 | UNION / EXCEPT / INTERSECT |
| `DuckJoin` | 新 | 笛卡尔积、无法用 Apply 的哈希连接 |
| `DuckBuffer` | 新 | NEXT 段间物化；超过 `spill_threshold_rows` 时落 DuckDB 临时表，否则留内存 |
| `Mutate` | 现 `MutateExecutor` | 消费上游行，执行 SET / REMOVE / DELETE / INSERT；一条语句一个 WriteBatch |

DuckDB 列类型映射：节点、边、路径、列表、映射在送入 DuckDB 时序列化为 JSON 列保留顺序，排序键和聚合键只允许标量；出来后再反序列化为值对象。这一点沿用现有 `DuckResult.canonical` 的做法扩展。

### 2.4 配置

新增 `spill_threshold_rows`（默认 100000）：`DuckBuffer` 和 `Apply` 右侧物化超过该行数时落 DuckDB。其余预算键不变。

## 3. 分阶段

### 阶段 1：词法与缺陷修复

改动集中在 `cengyan-parser` 和少量 query / catalog / store。不引入算子树。

1. 从 `GqlReader` 拆出 `Lexer`：反引号标识符、字符串转义、数字字面量（指数、进制、下划线）、`!=` 不加。
2. 关键字分两级：`RESERVED` 集合与 `CONTEXTUAL` 集合。`ident()` 放行 IDENT 与所有上下文关键字；语句分发仍按首词判断。
3. `script` 规则改为多语句；`Gql.parse` 返回 `List<Stmt>`；`QueryEngine.execute` 顺序执行返回最后一条；`ExecuteService` 不变协议。
4. `SKIP` / `OFFSET` / `LIMIT` 后接表达式，执行时求值并校验非负整数。
5. 一跳查询：删除「有 LIMIT 时先收满窗口再排序」的分支；有 ORDER BY 走全量收集 + 现有 `DuckResult`；无 ORDER BY 保留早停。
6. `DELETE`：`DETACH` / `NODETACH` 关键字；`ReadSession.countEdges`；NODETACH 有边报 `CONSTRAINT_VIOLATION`；DETACH 把出边入边两侧键一起放进 WriteBatch。
7. 函数返回形状：`labels()` 返回列表，`startNode()` / `endNode()` 返回节点值。
8. 路径查询：`LIMIT` 改回截断、不再必填；`*` / `+` / `{m,}` 解析通过，上界取 `max_hops`，预算为 0 时报「请写上界或配置 max_hops」。
9. DDL：`DROP NODE TYPE` / `DROP REL TYPE`（删目录项 + 按类型前缀删数据 + 删索引）；`IF NOT EXISTS` / `IF EXISTS` 覆盖 GRAPH / ALIAS / TYPE / INDEX。
10. 过程调用接受位置参数：`ProcedureExecutor` 为每个过程声明参数顺序表。
11. `Gql.g4` 同步。

测试：`GqlSyntaxTest` 扩充（每条词法规则正反例）；`QueryEngineTest` 新增 ORDER BY+LIMIT 正确性、NODETACH 报错、DETACH 连删、多语句、DROP TYPE。

验收：现有全部测试通过；`CREATE REL TYPE inv (comment STRING NULL)`、`WHERE a.regcap > 1e8`、`MATCH ... ORDER BY e.ratio DESC LIMIT 10` 返回真实 Top 10。

### 阶段 2：算子树骨架

1. 新增 `plan` 与 `exec` 包，定义 `PlanNode`、`Operator`、`Row`、`RowSchema`、值对象。
2. `Planner` 先只接受现有语法形态（单点、一跳、有界多跳、路径变量、过程调用），生成算子树。
3. 把 `MatchExecutor` 的点查 / 索引 / 类型扫 / 边扫 / 多跳 / 路径逻辑拆成叶子算子，`MatchExecutor` 退化为「Planner + 执行 + 收集」的壳，然后删除。
4. `DuckSort`、`DuckAggregate` 包装现有 `DuckResult`。
5. `EXPLAIN` 改为打印算子树（`PlanPrinter`），每个算子一行，含下推谓词与残差谓词。
6. `ProcedureCall` 算子接入，`YIELD` 后允许 `FILTER` / `RETURN`。

测试：现有 `HopMatchTest`、`PathAllTest`、`Phase6/7/8Test`、`QueryEngineTest` 全部不改断言跑通（除阶段 1 已改语义的用例）；新增 `PlannerTest` 对比 EXPLAIN 输出。

验收：结果与阶段 1 完全一致，性能回归不超过 10%（用 `HopPerfTest`）。

### 阶段 3：模式层

1. AST：`PathPattern`（元素序列）、`GraphPattern`（多路径 + WHERE）、`LabelExpr`（`|` `&` `!` `%`）、`Quantifier`（含无上界）、`PathMode`、`PathSearchPrefix`、`ParenthesizedPath`（量化路径模式），`MatchStmt` 持 `optional` 标志，一条查询持多个 `MatchStmt`。
2. Planner：
   - 单条路径多段 → `Expand` 链；
   - 逗号多路径与多个 MATCH → 有共享变量用 `Apply`，无共享变量用 `DuckJoin`；
   - `OPTIONAL MATCH` → `Apply(optional=true)`；
   - 标签表达式 → 类型集合，落到 `NodeByTypeScan` / `AllNodesScan` / 残差 `Filter`；
   - 内联 WHERE 与量化模式内部 WHERE → 下推进 `Expand` / `VarExpand` 谓词；
   - 变长绑定边变量为列表，路径变量可绑任意模式；
   - 路径搜索前缀 → `ShortestPath` 算子。
3. `VarExpand` 重写：每条路径一行、四种路径模式、无上界受 `max_hops`、中间点与边属性过滤（属性延迟加载）。
4. `AllNodesScan` 与 `ReadSession.scanAllVertices`。
5. `path.shortest` / `path.all` / `subgraph.bounded` 保留为过程，内部改调同一算子。

测试：新增 `PatternMatchTest`（多段、多模式、OPTIONAL、标签表达式、四种路径模式各自的行数断言、无上界+预算、路径前缀、量化模式内部过滤）。

验收：`07-多跳和过程.md` 里现有示例按新语义给出正确行数；多跳 `WHERE e.ratio > 0.1` 可用。

### 阶段 4：组合层

1. AST：`LinearQuery`（语句序列：MATCH / OPTIONAL MATCH / FILTER / LET / FOR / CALL / ORDER BY / OFFSET / LIMIT / RETURN）、`CompositeQuery`（`NEXT` 链、集合运算树）、`ReturnStmt` 持 `GROUP BY` 与 `*`。
2. Planner：`FILTER` → `Filter`；`LET` → `Project`；`FOR` → `Unwind`；`RETURN` 含聚合 → `DuckAggregate`（隐式分组键 = 非聚合项）；`ORDER BY/OFFSET/LIMIT` → `DuckSort`（无 ORDER BY 仅 LIMIT 时用 `Limit` 早停）；`NEXT` → `DuckBuffer`；集合运算 → `DuckSetOp`。
3. 聚合函数：avg / min / max / collect_list / stddev_samp / stddev_pop / percentile_cont / percentile_disc；`count(DISTINCT x)`；`RETURN *`。
4. `DuckBuffer` 落盘阈值与 DuckDB 临时表生命周期（随 `QueryHandle` 关闭）。

测试：新增 `CompositeQueryTest`（NEXT 两段、UNION/EXCEPT/INTERSECT 各 ALL/DISTINCT、GROUP BY 显式与隐式等价、聚合后 ORDER BY、FOR 展开、LET 引用、大结果落盘）。

验收：`MATCH (a:org)-[e:inv]->(b) RETURN a.name, count(*), avg(e.ratio) ORDER BY count(*) DESC LIMIT 10` 可用。

### 阶段 5：表达式层

1. AST：`Case`、`ListLiteral`、`MapLiteral`、`Index`、`Slice`、`Cast`、`Exists`（子查询）、新谓词节点、`Concat`。
2. `ExprEval`：函数注册表（名字 → 参数个数校验 + 实现），标准函数与扩展函数分两张表；`IN` 右侧接受列表值与参数；`||`；`IS TYPED` / `IS LABELED` / `IS SOURCE OF` / `IS DESTINATION OF` / `SAME` / `ALL_DIFFERENT` / `PROPERTY_EXISTS`。
3. `EXISTS { }`：Planner 生成子计划，`Filter` 调用，相关变量通过 `Apply` 传入。
4. 扩展函数：`starts_with`、`ends_with`、`contains`、`regex_match`、`nodes`、`relationships`、`labels`、`properties`、`type`、`id`。
5. 模式里的 `{k: v}` 允许任意表达式（已支持），`IN [..]` 列表在 `IndexLookup` 里展开为多次点查。

测试：新增 `ExprTest`（每个函数正反例与 NULL 传播、CASE 两种形式、列表索引越界为 NULL、CAST 失败报 TYPE_MISMATCH、EXISTS 相关与非相关）。

### 阶段 6：写入与 DDL

1. AST：`InsertStmt` 持 `GraphPattern`（多元素、多路径），可出现在 MATCH 之后；`SetStmt` 新增 `SetAll`；`RemoveStmt`；`AlterType` 新增 `DROP` / `RENAME`。
2. `Mutate` 算子消费上游行：插入时模式里的已绑定变量取行值，未绑定的新建；多跳后 SET / DELETE。
3. `Catalog.dropProperty`（列标记删除，schemaVersion +1，读旧版本数据时跳过）、`renameProperty`。
4. 一条语句一个 WriteBatch，跨分片沿用现有补偿标记。

测试：新增 `MutateTest`（多元素 INSERT、引用匹配变量 INSERT、多跳 SET、SET 整体赋值含 NULL、REMOVE、ALTER DROP 后读旧数据、RENAME 后索引仍可用）。

### 贯穿各阶段

- **`Gql.g4`**：每阶段结束同步到与手写解析器一致。
- **文档**：每阶段结束重写对应 `docs/gql/0x-*.md`；阶段 6 后重写 `00-和标准相比.md` 与 `09-标准与扩展边界.md`。
- **Studio（`cengyan-studio-web/src`）**：阶段 1 把 `statements.ts` 的分号拆分改为整段提交；`monaco.ts` 关键字表随每阶段更新；阶段 2 后 EXPLAIN 结果渲染成缩进树。
- **Python CLI（`cengyan-cli-python`）**：`file` 命令改为整段提交；`result.py` 识别新值形状（节点列表、路径、列表、映射）。
- **端到端回归**：`cengyan-query/src/test/resources/gql-cases/*.gql` + 期望 JSON，一条用例一文件，`GqlCasesTest` 逐条跑；用例按文档章节编号，文档里的每个示例都要有对应用例。

## 4. 破坏性变更清单

验收时请对照这张表检查依赖方。

| 变更 | 旧行为 | 新行为 | 阶段 |
|---|---|---|---|
| ORDER BY + LIMIT | 先收窗口再排 | 先排后截 | 1 |
| `DELETE a`（有边） | 静默留悬挂边 | 报错；需 `DETACH DELETE` | 1 |
| `labels(a)` | 字符串 | 列表 | 1 |
| `startNode(e)` / `endNode(e)` | 编号 | 节点对象 | 1 |
| 路径查询 `LIMIT` | 必填、超出报错 | 可选、截断 | 1 |
| `MATCH ... {1,3}` 不绑路径变量 | 每个新到达点一行 | 每条路径一行（WALK） | 3 |
| 多跳 `RETURN e` | 报错 | 返回边列表 | 3 |
| `EXPLAIN` 输出 | operator/detail 两列 | 算子树缩进文本 | 2 |
| 一次请求多语句 | 客户端拆分 | 服务端执行，返回最后一条 | 1 |

## 5. 风险

- **阶段 2 是最大的一次重构**：`MatchExecutor` 71KB 逻辑要拆进算子，期间现有测试是唯一安全网。拆解顺序：先叶子（点查、索引、类型扫、边扫），后 `HopLoop` / `PathAll`，最后删壳。
- **DuckDB 往返成本**：所有排序聚合都进 DuckDB，小结果集会比现在慢。接受，但 `Limit` 早停和下推谓词要保证「不排序、不聚合」的查询不进 DuckDB。
- **WALK 默认 + 无上界**：`*` 在稠密图上路径数指数增长，靠 `max_paths` / `max_hops` / `max_frontier_per_hop` 兜底。文档要写清默认预算为 0 时 `*` 必须写上界。
- **列删除**：`Catalog.dropProperty` 涉及 schemaVersion 读路径，`KeyCodec` 解码旧版本时要能按版本跳过列。阶段 6 前先读 `KeyCodec` 确认可行；不可行则改为「仅标记不可见，不改存储」。
- **保留字收窄**：把 `set`、`node`、`rel`、`type` 等降为上下文关键字后，`MATCH (set:org)` 与 `SET` 子句要靠位置消歧，解析器要有回溯或两级 lookahead。阶段 1 的 `GqlSyntaxTest` 要覆盖这些冲突词。
