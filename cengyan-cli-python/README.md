# cengyan Python SDK

同步 Python 客户端。当前版本提供 TCP JSON、HTTP JSON、图空间上下文、Result、Schema、数据写入、Bulk、运维和结果导出。

```python
from cengyan import Client

with Client("127.0.0.1", 19669, "root", "cengyan") as client:
    with client.graph("Company") as graph:
        result = graph.execute("MATCH (n) RETURN n LIMIT 10")
        for row in result.iter_rows():
            print(row)
```

每个 `GraphSession` 都拥有独立连接和服务端 Session。退出 `with` 时会先发送
`logout`，再关闭底层连接；`Client.close()` 会级联关闭尚未退出的图空间上下文。

批量导入：

```python
with Client("127.0.0.1", 19669, "root", "cengyan") as client:
    with client.graph("Company") as graph:
        graph.bulk.insert_nodes(
            "Person",
            [{"id": "p-1", "name": "Alice"}],
            batch_size=1024,
        )
```

HTTP JSON 网关使用：

```python
with Client("127.0.0.1", 19671, "root", "cengyan", transport="http") as client:
    with client.graph("Company") as graph:
        result = graph.execute("MATCH (n) RETURN n LIMIT 10")
```
