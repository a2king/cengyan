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
读请求断线不重发，但下一次请求会离开失败的节点。

`user` / `password` 必填，SDK 不提供默认口令。

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

批次按行数和字节上限（默认 4 MiB）切分，`rows` 可以是生成器。值里的 `datetime`、
`Decimal`、`UUID` 会按线上编码规则序列化，与 `execute` 的行为一致。

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

`CommitUncertainError` 只表示"不知道写没写成"，**不要**当成普通失败重跑 —— 先确认，
或按上面的方式在幂等窗口（服务端默认 1 小时）内以同一 `requestId` 重放。

服务端明确拒绝的请求会抛出具体类型，例如 `SyntaxError`、`TypeMismatchError`、
`UniqueViolationError`、`RequestConflictError`，它们都继承 `QueryError`，请求没有生效。

### 令牌

登录返回的令牌有效期 8 小时。过期后 `ClusterClient` 会用同一账号自动重登一次并重发，
写请求沿用原 `requestId`，不会重复写入。

### TLS

```python
client = ClusterClient([("graph.internal", 19669)], password="***", tls=True)
```

自签证书需自行配置 `ssl` 上下文：传 `transport_factory` 给 `ComputeTransport`。

### 管理命令

```python
client.admin("showMembers")                  # 只读
client.admin("setConfiguration", write=True, voters=[...])   # 会改集群状态
```

是否可重试由 `write=` 决定，SDK 不再按命令名猜测。

## 结果

`Result` 支持 `len()`、迭代、`first()`、`one()`、`scalar()`，以及 `to_csv` / `to_json` /
`to_jsonl` 导出；`one()` 在行数不为 1 时抛 `ResultError`（同时是 `ValueError`）。

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
