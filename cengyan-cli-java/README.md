# cengyan-cli-java

Cengyan 图数据库的官方同步 Java SDK。只面向 Compute 协议：图名随每次请求显式发送，客户端不保存服务端会话。
协议语义与 Python SDK `pycengyan` 一致。

- 运行环境：**Java 8+**
- 一个 jar，没有传递依赖（jackson-core 已重定位到 `com.cengyan.core.shaded.jackson`）
- 坐标：`com.cengyan:cengyan-cli-java:1.0.0`，包名 `com.cengyan.core`

## 构建

```bash
cd cengyan-cli-java
mvn package          # 产物 target/cengyan-cli-java-1.0.0.jar，可直接放进 classpath
```

本目录是独立工程：不继承仓库根 pom，不依赖任何服务端模块，服务端构建也不包含它。
以后发布到内网 Nexus 时，在 `pom.xml` 加上 `distributionManagement` 后 `mvn deploy` 即可。

## 基本用法

```java
import com.cengyan.core.*;

try (ClusterClient client = ClusterClient.builder()
        .addresses("10.0.0.1:19669", "10.0.0.2:19669")
        .user("root").password("***")
        .build()) {
    client.createGraph("Company", 16);
    ClusterGraph graph = client.graph("Company");
    graph.write("CREATE NODE TYPE Person (name STRING NULL, born DATETIME NULL)");

    Map<String, Object> params = Collections.singletonMap("name", "张三");
    for (Record row : graph.read("MATCH (n:Person {name: $name}) RETURN n.name AS name, n.born AS born", params)) {
        String name = row.getString("name");
        Instant born = row.getInstant("born");
    }
}
```

`ClusterClient` 线程安全，应在应用内复用同一个实例。首次请求时自动登录；令牌过期时自动重登一次并重发。
`password` 必填，SDK 不提供默认口令；`user` 默认 `root`。

### 读与写

SDK 不猜 GQL 的读写属性，`execute` 默认按只读处理：

```java
graph.read("MATCH (n) RETURN n LIMIT 10");          // 断线不重试，下次请求换节点
graph.write("INSERT (n:Person {id: 1, name: 'A'})"); // 断线换节点重试最多 3 次，同一 requestId
graph.execute(query, params, true, Duration.ofMinutes(5));  // 显式指定读写和超时
```

写请求的 `requestId` 在 `ClusterClient.request` 里一次定下，断线重试和令牌过期后的重发都用它，
服务端在幂等窗口内按它去重。

## 结果

`Result` 可迭代，支持 `size()`、`first()`、`one()`、`scalar()`、`stream()`，以及
`toJson()` / `writeJsonLines()` / `writeCsv()` / `toCsv(Path)` 导出（CSV 带 BOM，列表/对象写成 JSON）。

| 服务端值 | Java 类型 | 取值方法 |
|---|---|---|
| 整数 | `Long` | `getLong`、`getInt`（超出 int 报错，不截断） |
| 小数 | `Double` | `getDouble`（整数也可取） |
| 字符串 | `String` | `getString` |
| 布尔 | `Boolean` | `getBoolean` |
| DATETIME | ISO `String` | `getInstant` |
| 点 / 边 / 路径 | `Map` | `getNode` / `getEdge` / `getPath` |
| 列表 / 对象 | `List` / `Map` | `getList` / `getMap` |

值为 null 时取值方法返回 null；类型不符抛 `ResultException`（code `RESULT_TYPE`）。
`one()` 在行数不为 1 时抛 `ResultException`（code `RESULT_SHAPE`）。

## POJO 映射

```java
class Person {
    @CengyanId String key;                  // 主键（不标注时用名为 id 的字段）
    String name;                            // 字段名即属性名 / 列名
    @CengyanProperty("full_age") int age;   // 改名
    Instant born;                           // DATETIME
    @CengyanIgnore String cache;            // 不参与映射
}

List<Person> people = graph.read("MATCH (n:Person) RETURN n").list(Person.class);           // 整个点：id + properties
Person one = graph.read("MATCH (n:Person {id:'p1'}) RETURN n.name AS name").one(Person.class);  // 按列名
List<String> names = graph.read("MATCH (n:Person) RETURN n.name").list(String.class);       // 单列值
graph.insertNodes("Person", people);                                                        // 写入用同一套规则
```

- 需要无参构造函数（可以是 private）；内部类必须是 `static`
- 结果里多出的列忽略，缺的字段保持默认值；null 不会覆盖基本类型字段
- 整行只有一列且是点/边时展开为 `id` + `properties`（边还有 `src` / `dst`）
- 数值转换检查溢出；ISO 字符串可转 `Instant` / `LocalDateTime` / `OffsetDateTime` / `Date`；枚举按名字
- 边 POJO 需要 `src`、`dst` 字段

## 批量导入

```java
BulkProgress progress = graph.insertNodes("Person", rows);   // rows：Iterable<Map> 或 Iterable<POJO>，可以是惰性的
// BulkProgress{confirmed=100000, skipped=0, duplicates=0, completedBatches=98, requestId=...}
```

- 每批默认 1024 行；上限 10000 行、4 MiB（SDK 实际最多用到 4 MiB − 64 KiB，给服务端追加的字段留余量）
- 写点默认按主键分片攒批，每批只落在一个分片上，并优先发往与该分片 Leader 同机的计算节点
- 写边每行必须带 `id`、`src`、`dst`；`skipMissingEndpoints(true)` 时端点不存在的边计入 `skipped`
- 值的编码与 `execute` 参数一致：`Instant` / `LocalDateTime` / `Date` 写成 UTC ISO 字符串，
  `BigDecimal` / `UUID` 写成字符串，`BigInteger` 必须在 long 范围内；NaN/Infinity 和不支持的类型在发送前拒绝

### 失败与重放

```java
RequestIdProvider identity = (index, batch) -> "import-" + jobId + "-" + index;
BulkOptions options = BulkOptions.builder().batchSize(1024).requestId(identity).build();
try {
    graph.insertNodes("Person", rows, options);
} catch (CommitUncertainException error) {
    // 提交状态无法确认：可能已写入。在幂等窗口内用同一身份重放，服务端按 requestId 去重。
    int failed = error.bulkProgress().failedBatch();
    graph.insertNodes("Person", rows, options.toBuilder().startBatch(failed).build());
}
```

批次编号始终是全局的。重放时 rows、batchSize、maxBatchBytes、alignPartitions 必须与首次一致，
`startBatch` 之前的批次只占位不发送，编号和切批结果因此与首次相同。无效行（缺 `id`、`id` 类型不对、
无法编码）抛 `BulkInsertException`，`failedBatch` 指向下一个未发送批次，修好数据后按同样方式重放。

`CommitUncertainException` 只表示"不知道写没写成"，**不要**当成普通失败重跑——先确认，
或按上面的方式在幂等窗口（服务端默认 1 小时）内以同一 `requestId` 重放。

## 异常

全部是非受检异常，继承 `CengyanException`（`code()`、`response()`、`queryId()`、`requestId()`、`bulkProgress()`）。

```
CengyanException
├─ TransportException ─ ConnectionException / ReadTimeoutException / ProtocolException
│   └─ CommitUncertainException      写请求提交状态未知
├─ AuthenticationException / AuthorizationException
├─ QueryException                    服务端明确拒绝，请求没有生效
│   ├─ GqlSyntaxException, TypeMismatchException, TypeNotFoundException, GraphNotFoundException, ...
│   ├─ InvalidArgumentException, RequestTooLargeException, LimitExceededException, ResourceExhaustedException
│   ├─ BusyException                 暂时没有执行名额，稍后重试即可
│   ├─ QueryTimeoutException
│   ├─ ConflictException ─ RequestConflictException
│   └─ BulkInsertException           批量数据在发送前被拒绝
├─ ClusterStateException             拓扑、成员、维护模式等
├─ InternalException / UnsupportedException
└─ ResultException                   结果形状或类型不符、POJO 映射失败
```

服务端兜底码 `FAILED` 和 `QUERY_TIMEOUT` 对写请求抛 `CommitUncertainException`；对读请求分别抛
`InternalException` 和 `QueryTimeoutException`。单条请求编码后超过服务端 8 MiB 行上限时，SDK 在发送前抛
`RequestTooLargeException`，不进入断线重试。错误码映射与 pycengyan 逐条一致（契约测试保证）。

## 管理命令

```java
client.admin("showNodes");                                                  // 只读
client.admin("drainCompute", true, Collections.singletonMap("nodeId", "c2"));  // 会改集群状态，显式 write=true
```

是否可重试由 `write` 决定，SDK 不按命令名猜测。

## 连接与 TLS

- 每个计算节点地址一个连接池：一条连接同一时间只承载一个请求；默认每个地址保留 8 条空闲连接、空闲 60 秒回收
  （`maxIdleConnectionsPerAddress`、`idleTimeout`）；出错或超时的连接直接丢弃
- `timeout`（默认 30 秒）是单次请求的总预算，写请求重试也在这个预算内

Compute 的客户端端口本身**不终止 TLS**。需要加密链路时，在 Compute 前部署 TLS 终止代理，SDK 连接代理：

```java
ClusterClient.builder().addresses("graph.internal:19669").password("***").tls(true).build();         // JVM 默认信任库
ClusterClient.builder().addresses("graph.internal:19669").password("***").sslContext(context).build(); // 自签 CA / 双向 TLS
```

会校验证书主机名。`refreshNodes()` 会把地址换成服务端登记的 IP，走代理或校验证书主机名时不要调用。

## 日志

使用 `java.util.logging`，logger 名 `com.cengyan.core`，只在写请求重试、重新登录、读请求断线时以 FINE 级别记录，
不输出密码和令牌。Spring / Spark 可以用 `jul-to-slf4j` 桥接。

## 开发

```bash
mvn test                                               # 单元测试 + 契约测试（不需要服务端）
smoke/run-smoke.sh <jdk21>/bin/java <jdk8>/bin/java    # 真实 Storage + Compute 冒烟（需要先构建服务端 jar）
```

- `src/test/resources/contract/`：契约用例。`partition-vectors.json` 由服务端 `PartitionRouter` 生成，
  `error-codes.json` 由 pycengyan 的错误码映射生成；重新生成方法见 `contract-gen/README.md`
- 主代码只能使用 Java 8 API；在 JDK 9+ 上构建时自动使用 `--release 8`
