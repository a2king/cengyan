# 通过 Compute 批量导入

单机和多机都使用 `ClusterClient`，不使用旧 GraphSession、HTTP 单机入口或本地 WriteBatch 导入。
数据经 Compute 校验和规划，由 Storage 的事务协调器处理跨分片准备、提交和恢复。

发布配置现采用 Data 分片重建模式，生命周期及迁移方式见 [Data 分片重建模式](rebuild-data-mode.md)。

## 并发写入配置

每个 Storage 的 `storage.properties` 支持：

```properties
write_threads=10
rpc_max_payload_bytes=33554432
```

默认值为 10，必须是正整数，重启 Storage 后生效。该值分别控制两个独立线程池：
写事务协调池和分片写入执行池，各 10 个线程；不是整个进程合计 10 个线程。
租约校验、认证和控制请求使用独立的控制线程池，不占用写事务协调名额。

- 批量实体旧值按分片 `getMany` 读取；prepare 的锁快照也批量读取。
- 同一事务每轮最多并行 4 个参与分片，所有事务共享上述分片执行池；队列有界。
- prepare 全部成功后才提交决定；失败时先等待本轮已发出的 prepare 返回，再决定回滚。
- finalize 按最多 64 个变更键、计划字节预算分块，批量读取锁/实体并提交 CAS；完整写命令过大时继续拆块。
- 新批次按 requestId 在参与分片中分散选择协调者；重放和恢复保留已持久化的协调者。
- 原子可见性、唯一约束、请求幂等和同步持久化仍生效。

`ops.metrics` 的 Storage 指标新增 `writeThreads`、`writeActive`、`writeQueued`、
`writeParticipantActive`、`writeParticipantQueued`。Compute 的 `query_slots` 和 Spark 实际
active tasks 是另外两层并发限制，调整 `write_threads` 不会自动修改它们。

升级需要使用同一版本的 Compute 和 Storage JAR（两者均包含 distributed 模块）。
已有 Storage 配置若显式写着旧值，需要改为 `rpc_max_payload_bytes=33554432` 并重启所有 Storage。
事务计划上限 6 MiB，而事务决定和 finalized 的 CAS 同时包含旧、新完整记录，内部命令可超过 8 MiB；
内部 RPC 默认上限为 32 MiB，Raft 日志缓冲为 16 MiB 级别，可容纳当前事务计划生成的状态 CAS。外部 bulkInsert 的 4 MiB 上限仍然有效，
5000 行是否能作为一个请求还取决于属性字节数和生成的事务计划大小。
验证包含 32 分片、单副本、10 个并发批次、每批 512 个节点，以及计数、幂等重放和锁清理。
该验证是正确性测试，不能作为目标机器的吞吐承诺。历史事务读视图扫描及属性索引逐键规划仍存在，
长期导入性能还需按真实数据及索引配置测量。

## SDK 示例

```python
from cengyan import ClusterClient

with ClusterClient([('127.0.0.1', 19669)], password='你的密码', timeout=60) as client:
    graph = client.graph('Company')
    nodes = graph.insert_nodes('N', [
        {'id': 'a', 'name': 'Alice'}, {'id': 'b', 'name': 'Bob'},
    ], batch_size=1024)
    edges = graph.insert_edges('E', [
        {'id': 'e1', 'src': 'a', 'dst': 'b'},
    ], batch_size=1024, skip_missing_endpoints=False)
    print(nodes, edges)
```

先创建图和类型；ID 的 INT64/STRING 类型必须符合建图定义。边 ID 必须显式提供。
同 ID 完整覆盖属性，不能更改既有类型或边端点；属性和唯一约束冲突不会被跳过。
只有边端点缺失可以通过 `skip_missing_endpoints=True` 跳过。

## 边界

- 单个网络批次最多 10000 行，Compute 限制序列化请求不超过 4 MiB。
- SDK 默认按 1024 行和 4 MiB 估算分批；建议留出字段封装开销，必要时降低 max_batch_bytes。
- 每个网络批次有独立稳定 requestId；同批写重试复用该 ID，不能重新生成。
- 多个网络批次不构成一个全局事务。失败后停止后续批次，仅报告已确认进度。
- 不要把十亿条数据装进一个列表：传入生成器，通过 Spark 分区或外部任务限制并发。
- 节点 ID、边 ID、出入邻接、唯一索引可落在不同分片，不再是旧单机“一次本地 WriteBatch”语义。

协议示例：

```json
{
  "op": "bulkInsert",
  "token": "登录令牌",
  "requestId": "本批次稳定ID",
  "graph": "Company",
  "kind": "edge",
  "type": "E",
  "skipMissing": false,
  "rows": [{"id": "e1", "src": "a", "dst": "b"}]
}
```

优先使用 SDK 生成 requestId 和管理连接。最终 `FAILED` 可能表示提交状态未知，不能据此认定数据全部回滚。
源码中 `dev-scripts/` 下的历史私有导入工具未作为发布 SDK，迁移前应核对其调用是否符合当前协议。
