# cengyan-client

连接 Compute 的客户端。无参数时进入交互，输入 GQL 和管理命令。`import` 子命令做批量导入，一次只导入一种点或一种边。

## 交互

```bash
mvn -pl cengyan-client -am package
java -jar cengyan-client/target/cengyan-client-1.0.0.jar --host 127.0.0.1 --port 19669
```

`--host` 默认 `127.0.0.1`，`--port` 默认 `19669`。`exit` 或 `quit` 退出。也可以把一行命令放在参数后面，执行一次后退出。

```text
:login root 你的初始化密码
USE Company
MATCH (n:Company) RETURN n.id, n.name
:stats
```

`:addStorage`、`:addCompute`、`:setPartCount`、`:rebuildIndex` 已删除。

## Linux 可执行文件

Linux 上不要再写 `java -jar`。发布包自带 Java 21，解压后直接执行 `./cengyan-client`。默认架构是 x64，也可以打 aarch64。

```bash
mvn -pl cengyan-client -am package -DskipTests
python packaging/build-client.py --arch x64
```

产物是 `dist/cengyan-client-linux-x64.tar.gz`。解压后只有一个文件 `cengyan-client`，jar 和 Java 21 都在这个文件里面。拷到 Linux 后：

```bash
tar -xzf cengyan-client-linux-x64.tar.gz
./cengyan-client --host 127.0.0.1 --port 19669
./cengyan-client import --compute-nodes 127.0.0.1:19669 --username root --password '你的初始化密码' --graph-space Company --file-type orc --files './org/part-*' --node org --id eid
```

第一次执行会把运行时展开到 `~/.cache/cengyan/client/`，之后直接启动。目标机器不必再安装 JDK，也不必把 `java` 配进 PATH。

## 批量导入

Studio 发布包根目录上仍有 `cengyan-client.jar`，给已经装了 Java 21 的环境用。数据机上优先用上面的 `./cengyan-client`。

一次命令导入一个点类型或一个边类型，分三步：

1. **上传**：读文件，按点或边的 ID 去重，按分片攒成有序的上传段，传给分片的**每个**副本。上传段只放在副本的暂存目录里，查询看不到，也不进分片库和 Raft 日志。
2. **归并**：全部分片上传完以后，各副本把本分片的上传段归并成互不重叠的大文件，并按副本当前的数据预检冲突。任何一个分片归并或预检失败，整次导入都不生效。
3. **发布**：每个分片一条 Raft 指令，各副本一次性摄入。新类型没有已有数据要比对，文件直接放进分片库最底层，之后不需要压实。

导入过程中客户端每 20 秒向集群发心跳。超过 30 分钟没有心跳，各副本会删除这次导入的暂存文件。

```bash
./cengyan-client import \
  --compute-nodes 172.17.251.111:19669,172.17.251.112:19669 \
  --username root \
  --password '你的初始化密码' \
  --graph-space Company \
  --file-type tsv \
  --files './nodes/part-*' \
  --node Person \
  --id id \
  --parameter name \
  --unq ignore \
  --thread 8 \
  --batch-mb 64 \
  --buffer-mb 1024 \
  --retry 5 \
  --retry-interval 5
```

`nodes/part-000.tsv` 第一行是表头，从第二行开始是数据，默认分隔符是制表符：

```text
id	name
p1	Alice
p2	Bob
```

边必须自带主键列。起点和终点用点的主键。

```bash
./cengyan-client import \
  --compute-nodes 172.17.251.111:19669 \
  --username root \
  --password '你的初始化密码' \
  --graph-space Company \
  --file-type tsv \
  --files './edges/part-*' \
  --edge knows \
  --id edge_id \
  --src-id src \
  --dst-id dst \
  --parameter since
```

`--file-type` 还可以是 `parquet` 或 `orc`，列名规则与 TSV 表头相同。`--files` 只能写一次，多个路径用逗号拼接，每一段都可以带通配。

### 主键规则和 `--unq`

点的 ID 在**整张图**里唯一：一个 ID 只能属于一个点类型。边的 ID 只在**它的边类型**里唯一，不同边类型可以用同一个边 ID。

`--unq` 决定导入的主键已经存在时怎么办：

| 取值 | 同一类型里已有这个主键 | 点 ID 已经属于另一个点类型 |
|---|---|---|
| `ignore`（默认） | 跳过 | 跳过 |
| `replace` | 覆盖 | 跳过，不会把一个点改成别的类型 |
| `error` | 整次导入失败 | 整次导入失败 |

边 ID 已经存在、但起点或终点和已有的边不同时，不论 `--unq` 取什么都会失败：邻接记录在端点所在的分片上，批量导入改不到。

批量导入不检查边的端点是否存在，也不维护索引。类型上有索引时导入会被拒绝，先删除索引，导入完成后再创建。

### 参数

`--batch-mb` 是单个分片攒到多大就上传一段，单位 MB，默认 `64`。`--buffer-mb` 是全部分片合计最多攒多少，默认 `1024`；超过时先把最大的那个分片传出去。分片多、`--buffer-mb` 小时上传段会比较小，这只会多一些上传请求：副本会在后台把小段合并，发布前再归并成大文件，小段不会进入分片库。客户端 JVM 的堆要能放下 `--buffer-mb` 的大约两倍。

`--thread` 是并行上传、归并、发布的连接数，默认 `8`。

`--retry` 是一次请求失败后再试的次数，默认 `5`，`0` 表示不重试。`--retry-interval` 是两次尝试的间隔，单位秒，默认 `5`。只重试超时、连接中断、集群忙、换主这类暂时的失败；主键冲突、类型不匹配、参数错误不会重试。分块按偏移覆盖，归并和发布按会话幂等，重试不会把数据导入两遍。

### 进度和结果

多个 Compute 地址写成 `host:port,host:port`，各上传连接会分散到不同的 Compute。上传阶段每 60 秒打印一次已上传条数、跳过条数、平均和最近一分钟的上传速度、失败重试次数；归并和发布阶段打印已发布的分片数和条数。

结束时打印起始时间、结束时间、总耗时、完成条数、跳过条数、覆盖条数、平均速度、上传耗时、归并发布耗时和会话号。失败时同一份结果里会多一行「导数发生错误」。

上传或归并阶段失败时没有任何分片生效，重新执行整条命令即可。发布阶段个别分片失败时，其他分片照常发布完，结果里会列出失败的分片；这时已发布的分片不能撤回，按 `--unq ignore` 重新执行整条命令，已导入的主键会被跳过。不论成功失败，客户端最后都会通知各节点删除这次导入的暂存文件。
