# cengyan-cli-java 设计方案（已实现）

使用说明见 README.md。下文是设计依据，实现与之一致，差异见末尾。

独立的 Java SDK。本目录自成一体：自己的 `pom.xml`，不进根 reactor，不继承根 pom，
不依赖任何服务端模块（cengyan-protocol / distributed / compute 等），服务端代码也不依赖它。

## 1. 工程

- 坐标：`com.cengyan:cengyan-cli-java:1.0.0`（groupId 待确认，见末尾）
- 包名：`com.cengyan.core`
- 编译：`--release 8`，主代码只用 Java 8 API
- 运行时依赖：仅 `jackson-core`（流式 API），shade 重定位到 `com.cengyan.core.shaded.jackson`，
  产出一个可直接引入的 jar，没有暴露给使用方的传递依赖
- 测试依赖：JUnit 5
- 日志：`java.util.logging`，logger 名 `com.cengyan.core`；只记录重试、重登、连接失败，不输出密码和 token
- Nexus：以后加 `distributionManagement` 即可

目录结构：

```
cengyan-cli-java/
├── pom.xml
├── README.md
├── DESIGN.md
└── src/
    ├── main/java/com/cengyan/core/...
    └── test/
        ├── java/com/cengyan/core/...
        └── resources/contract/   # 协议契约用例（错误码映射、分片哈希向量、编码用例）
```

## 2. 包结构（`com.cengyan.core`）

| 类 | 职责 |
|---|---|
| `ClusterClient`、`ClusterClient.Builder` | 入口：地址、账号、超时、`SSLContext`、连接池大小 |
| `ClusterGraph` | `read` / `write` / `execute`，`insertNodes` / `insertEdges` |
| `Result`、`Record` | 结果与行；带类型的取值方法；`as(Class)` / `list(Class)` |
| `BulkOptions`、`BulkProgress`、`RequestIdProvider` | 批量写入参数、进度、`(index, rows) -> id` |
| `exception.*` | 非受检异常层次 |
| `mapping.*` | `@CengyanProperty`、`@CengyanId`、`@CengyanIgnore`、反射映射器 |
| `internal.*` | `Json`、`LineConnection`、`ConnectionPool`、`ComputeTransport`、`PartitionRouter`、`BulkWriter`、`ErrorCodes`（不对外） |

## 3. 协议语义（与 pycengyan 一致）

- 写请求的 `requestId` 在 `ClusterClient.request` 一次生成，断线重试、令牌过期重发都沿用
- 写请求断线换节点重试最多 3 次，超过截止时间返回 `FAILED`；读请求断线不重试，下次请求换节点
- 每次尝试把 `remainingTimeoutMs` 设为剩余预算，socket 读超时同为剩余预算
- `FAILED` / `QUERY_TIMEOUT`：写请求 → `CommitUncertainException`；读请求 → `InternalException` / `QueryTimeoutException`
- 紧凑 JSON；拒绝 NaN/Infinity；单行超过 8 MiB 在发送前抛 `RequestTooLargeException`
- 批量写入：行数 1–10000，字节 1 KiB–4 MiB 并预留 64 KiB；全局批次编号；`insertNodes` 默认按分片对齐，
  优先发往分片 Leader 同机的计算节点；无效行抛 `BulkInsertException`，带 `failedBatch`
- 连接：按地址的有界连接池（默认每地址 8 条），一条连接同一时间一个请求，出错/超时即作废，空闲超时回收
- Compute 端口不终止 TLS：`SSLContext` 用于连接前置的 TLS 终止代理

## 4. 异常（均继承 `CengyanException extends RuntimeException`）

```
CengyanException (code, message, response, queryId)
├─ TransportException ─ ConnectionException / ReadTimeoutException / ProtocolException
│   └─ CommitUncertainException            ← requestId()
├─ AuthenticationException / AuthorizationException
├─ QueryException ─ GqlSyntaxException, TypeMismatchException, InvalidArgumentException, BusyException,
│                   QueryTimeoutException, LimitExceededException, RequestTooLargeException,
│                   ConflictException ─ RequestConflictException, BulkInsertException …
├─ ClusterStateException / InternalException / UnsupportedException
└─ ResultException
```

批量写入过程中抛出的异常都可通过 `e.bulkProgress()` 取到 `confirmed`、`completedBatches`、`failedBatch`、`requestId`。

## 5. 值与 POJO 映射

Java → JSON（参数、批量行）：
- `String`、`Boolean`、整数、`Float`/`Double`、`null`、`List`、`Map` 原样
- `Instant`/`OffsetDateTime`/`ZonedDateTime`/`java.util.Date` → 带 `Z` 的 ISO 字符串；`LocalDateTime` → 不带时区
- `UUID`、`BigDecimal` → 字符串；`BigInteger` 在 long 范围内转 long，超出拒绝
- 其余类型拒绝，不做隐式 `toString()`

JSON → Java（结果）：整数 `Long`、小数 `Double`、对象 `LinkedHashMap`、数组 `ArrayList`。
`Record` 提供 `getLong`/`getInt`（溢出报错）、`getDouble`、`getString`、`getBoolean`、`getInstant`、
`getMap`、`getList`、`getNode`/`getEdge`/`getPath`、`as(Class)`。

POJO 映射：
- 字段名即属性名，`@CengyanProperty("x")` 覆盖，`@CengyanIgnore` 排除；需无参构造，字段可为 private
- 按列名映射：`RETURN n.name AS name` → `name` 字段
- 整行只有一列且为点/边时展开：`id` → `@CengyanId` 字段（无注解时用名为 `id` 的字段），
  `properties` 按字段名填，边再填 `src`/`dst`
- 写入方向同一套规则：`insertNodes("Person", List<Person>)`；POJO 无 id 字段时报错
- 转换：`Long` → `int`/`short` 检查溢出；ISO 字符串 → `Instant`/`LocalDateTime`/`Date`；`enum` ↔ `String`

## 6. 测试

- 单元测试：假 transport，覆盖 pycengyan 测试的全部场景
- 契约用例放在 `src/test/resources/contract/`（只在本目录内，不改 Python SDK、不放仓库根目录）。
  分片哈希向量是预先生成的固定数据，SDK 构建不依赖服务端模块
- 真实进程冒烟：服务端用 JDK 21 启动已构建的 jar，SDK 跑在 JDK 8 上

## 7. 编码顺序

1. pom、shade、Java 8 兼容核对；`Json`、`LineConnection`、异常、错误码映射
2. `ConnectionPool` + `ComputeTransport`
3. `ClusterClient`（登录、重登、admin、`refreshNodes`）、`Result`/`Record`
4. `PartitionRouter` + `BulkWriter`（分片对齐、重放）、契约用例
5. POJO 映射（读写两个方向）
6. README、冒烟测试

## 实现说明

- Maven groupId 暂定 `com.cengyan`（与包名一致），需要与服务端统一时改 pom 一行即可
- 契约用例（错误码映射、分片哈希向量）放在 `src/test/resources/contract/`，生成脚本在 `contract-gen/`，不改 Python SDK
- `LocalDateTime` 按 UTC 编码（服务端把无时区时间视为 UTC），`LocalDate` 写成 `yyyy-MM-dd`
- 写请求遇到服务端错误响应且响应里没有 requestId 时，SDK 把本次的 requestId 补进异常，便于确认或重放
