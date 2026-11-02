# 批量导入

## 设计目标

`bulkInsert` 是导入协议，不是把很多条 GQL 拼在一个字符串里。一个请求包含一个类型的一批行，服务端完成：

1. JSON 解码；
2. Schema 和值类型校验；
3. 唯一索引校验；
4. 节点字符串 ID 映射；
5. 节点或边拓扑键、属性键、索引键组装；
6. 一次 RocksDB `WriteBatch` 提交。

默认单批上限为 10000 行，请求体上限为 16 MiB。对于 10 亿级数据，建议：

```text
Spark 分区 / 外部任务
    → 512～4096 行一个 bulkInsert
    → 每个分区复用一个连接和登录会话
    → 通过 repartition 控制并发
```

不要创建十亿条 GQL 字符串，也不要让一个批次无限增长。批次大小要同时受以下因素约束：

- 网络请求体大小（16 MiB）；
- JVM 堆和 Jackson 对象；
- RocksDB WriteBatch 内存；
- 单条属性大小；
- WAL 写入延迟；
- 服务端查询线程数。

## 节点请求

```json
{
  "op": "bulkInsert",
  "session": "session-id",
  "kind": "node",
  "type": "Person",
  "rows": [
    {"id": "p-001", "name": "Alice"},
    {"id": "p-002", "name": "Bob"}
  ]
}
```

## 边请求

```json
{
  "op": "bulkInsert",
  "session": "session-id",
  "kind": "edge",
  "type": "knows",
  "rows": [
    {"src": "p-001", "dst": "p-002", "rank": 0, "score": 0.5}
  ]
}
```

边的 `src` / `dst` 使用已注册的节点 ID。两端可以落在不同分片。尚未注册的字符串端点默认让整批失败；`skipMissing=true` 时跳过这些边。

## 原子性

单批内部是原子的：

- 节点：ID 映射、节点属性和节点索引一起提交；
- 边：同一分片的出边、入边、边属性和边索引一起提交；跨分片边先提交出边一侧，再提交入边；
- 类型错误、非空属性缺失、唯一索引冲突会导致整个批次失败。`skipMissing=true` 时，未注册的字符串端点不计入失败。

批次之间不自动组成跨批事务。导入器应记录已经成功提交的批次，失败后从批次边界重试。重复导入可以在业务层使用稳定 ID，或者传 `ignoreExisting=true` 跳过已有元素。

## 当前限制与运维建议

- 单批最多 10000 行，单行协议帧最大约 16 MiB；应优先按请求体大小而不是只按行数切批。
- 节点批次的字符串 ID 映射、属性和索引在同一个 `WriteBatch` 中提交。
- 边批次允许 `src` 和 `dst` 位于不同分片。跨分片写入使用补偿标记，进程在两步之间退出时，下次启动会补上入边。
- 批次之间没有跨批事务。导入器必须保存分区号、批次号和提交结果，失败后从批次边界重试。
- `--batch` 是 `data_init_node.py` / `data_init_edge.py` 的服务端批大小，不是本地列表的装饰参数。
- 10 亿级数据应通过 Spark `repartition`、稳定业务 ID、分批重试和导入后的索引重建进行规划。

## Python/Spark 示例

### Physical shard layout

The server keeps catalog/auth/index metadata in the root RocksDB and stores graph data in a graph/partition hierarchy:

```text
data/
  data/                         # root metadata database
    <graph-name>/
      part-0/db/                # one RocksDB per physical partition
      part-1/db/
```

On startup, legacy graph data in the root database is migrated into this layout. Changing a graph's partition count rewrites graph keys between the stable `part-<id>` directories; the partition-count value is metadata, not part of the path.

The number of files inside `part-<id>/db` is still controlled by RocksDB (SST/WAL/manifest files); those files are not additional logical partitions.

```bash
spark-submit --py-files dev-scripts/cengyan.py dev-scripts/data_init_node.py \
  --sql "select id, name from company_source" \
  --host 127.0.0.1 --port 19669 --user root --password cengyan \
  --space Company --node-type Person --id-field id \
  --property-fields name --batch 2048 --repartition 16
```

