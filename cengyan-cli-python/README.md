# pycengyan Python SDK

同步 Python 客户端，只面向 Compute 协议：图空间随请求显式发送，客户端不保存服务端会话。

## 安装

```bash
pip install ./cengyan-cli-python
```

发布包内解压后位于 `sdk/python/`，或直接加 `PYTHONPATH=<包>/sdk/python/src`。

## 基本用法

```python
from pycengyan import ClusterClient

with ClusterClient([("127.0.0.1", 19669)], user="root", password="***") as client:
    client.create_graph("Company", partitions=16)
    with_graph = client.graph("Company")
    with_graph.execute("CREATE NODE TYPE Person (name STRING NULL)", write=True)
    result = with_graph.execute("MATCH (n:Person) RETURN n.name AS name LIMIT 10")
    for row in result:
        print(row["name"])
```

`ClusterClient` 可以给多个计算节点地址；写请求断线会在其他节点用同一 `requestId` 重试最多三次，
读请求断线不重发，但下一次请求会离开失败的节点。写请求的 `requestId` 在 `ClusterClient.request`
里一次定下，断线重试和令牌过期后的重发都用它，服务端在幂等窗口内按它去重。

`password` 必填，SDK 不提供默认口令；`user` 默认 `root`。

### 读与写

`execute()` 默认按只读处理，不会自动猜 GQL 的读写属性：

```python
graph.read("MATCH (n) RETURN n LIMIT 10")         # 断线不重试
graph.write("INSERT (n:Person {name: 'A'})")      # 断线重试，同一 requestId
```

### 批量导入

```python
progress = graph.insert_nodes("Person", rows, batch_size=1024)
# {'confirmed': 100000, 'skipped': 0, 'duplicates': 0,
#  'completedBatches': 98, 'requestId': '...'}
```

批次按行数和字节上限切分，`rows` 可以是生成器。值里的 `datetime`、
`Decimal`、`UUID` 会按线上编码规则序列化，与 `execute` 的行为一致；`NaN` / `Infinity`
不是合法 JSON，会在发送前以 `BulkInsertError`（`INVALID_ARGUMENT`）拒绝。

上限与服务端一致：`batch_size` 取 1–10000，`max_batch_bytes` 取 1 KiB–4 MiB。服务端校验 4 MiB
时会带上它追加的字段，因此 SDK 实际每批最多用到 4 MiB − 64 KiB。

`insert_nodes` 默认 `align_partitions=True`：按主键分片攒批，每批只落在一个分片上，并优先发给
与该分片 Leader 同机的计算节点。批次编号始终是全局的，按实际发送顺序递增。

失败时异常上带批次位置：

```python
from pycengyan import CommitUncertainError

identity = lambda index, batch: f"import-{job}-{index}"
try:
    graph.insert_nodes("Person", rows, batch_size=1024, request_id=identity)
except CommitUncertainError as error:
    # 提交状态无法确认：可能已写入。窗口内用同一身份重放，服务端按 requestId 去重。
    failed = error.response["failedBatch"]
    graph.insert_nodes("Person", rows, batch_size=1024, request_id=identity, start_batch=failed)
```

重放时 `rows`、`batch_size`、`max_batch_bytes`、`align_partitions` 必须与首次调用一致，
`start_batch` 之前的批次只占位不发送，编号和切批结果因此与首次相同。遇到无效行（缺 `id`、
`id` 类型不对、无法编码）时同样抛 `BulkInsertError`，`failedBatch` 指向下一个未发送批次，
修好数据后按同样方式重放。

`CommitUncertainError` 只表示"不知道写没写成"，**不要**当成普通失败重跑 —— 先确认，
或按上面的方式在幂等窗口（服务端默认 1 小时）内以同一 `requestId` 重放。

服务端明确拒绝的请求会抛出具体类型，例如 `GqlSyntaxError`、`TypeMismatchError`、
`InvalidArgumentError`、`UniqueViolationError`、`RequestConflictError`、`RequestTooLargeError`，
它们都继承 `QueryError`，请求没有生效。`BusyError` 表示暂时没有执行名额，稍后重试即可；
集群拓扑、维护模式等问题抛 `ClusterStateError`。

`FAILED`（服务端兜底码）和 `QUERY_TIMEOUT` 对写请求抛 `CommitUncertainError`；对读请求分别抛
`InternalError` 和 `QueryTimeoutError`，不会被误报成"写入状态未知"。

正式异常名是 `CengyanConnectionError` / `GqlSyntaxError`。旧名 `pycengyan.ConnectionError` /
`pycengyan.SyntaxError` 仍然可用，但不在 `__all__` 里，`from pycengyan import *` 不会覆盖内置异常。

单条请求编码后超过服务端 8 MiB 行上限时，SDK 在发送前抛 `RequestTooLargeError`，不会进入断线重试。

### 令牌

登录返回的令牌有效期 8 小时。过期后 `ClusterClient` 会用同一账号自动重登一次并重发，
写请求沿用原 `requestId`，不会重复写入。

### TLS

Compute 的客户端端口本身**不终止 TLS**。需要加密链路时，在 Compute 前部署 TLS 终止代理
（负载均衡、stunnel、Envoy 等），SDK 连接代理：

```python
client = ClusterClient([("graph.internal", 19669)], password="***", tls=True)

# 自签证书或双向 TLS：传自己的 ssl 上下文（传了 ssl_context 即视为 tls=True）
import ssl
context = ssl.create_default_context(cafile="/etc/cengyan/ca.pem")
client = ClusterClient([("graph.internal", 19669)], password="***", ssl_context=context)
```

`refresh_nodes()` 会把地址换成服务端登记的 IP；走代理或校验证书主机名时不要调用它。

### 管理命令

```python
client.admin("showMembers")                  # 只读
client.admin("setConfiguration", write=True, voters=[...])   # 会改集群状态
```

是否可重试由 `write=` 决定，SDK 不再按命令名猜测。

## 结果

`Result` 支持 `len()`、迭代、`first()`、`one()`、`scalar()`，以及 `to_csv` / `to_json` /
`to_jsonl` 导出；`one()` 在行数不为 1 时抛 `ResultError`（同时是 `ValueError`）。
CSV 里的列表/对象值（节点、路径等）写成 JSON 字符串。

## 开发

```bash
cd cengyan-cli-python
python -m pytest                 # pyproject 已配置 pythonpath=src
ruff check src tests
```

真实进程冒烟（需先 `mvn package`，并指定 JDK 21）：

```bash
PYTHONPATH=src python tests/cluster_process_smoke.py --java /opt/jdk-21/bin/java
```
