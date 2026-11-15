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

导入过程中客户端每 20 秒向集群发心跳。超过 30 分钟没有心跳，各副本会删除这次导入留下的暂存文件。已经写入数据库的数据不会被这次清理删掉。

```bash
./cengyan-client import \
  --compute-nodes 172.17.251.111:19669 \
  --username root \
  --password '你的初始化密码' \
  --graph-space Company \
  --file-type tsv \
  --files './nodes/part-*' \
  --node Person \
  --id id \
  --parameter name \
  --unq ignore \
  --thread 4 \
  --batch-mb 64 \
  --retry 3 \
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

`--file-type` 还可以是 `parquet` 或 `orc`，列名规则与 TSV 表头相同。`--files` 只能写一次，多个路径用逗号拼接，每一段都可以带通配。`--unq` 取值 `ignore`（默认）、`replace`、`error`。

`--batch-mb` 是每批提交的大小，单位 MB，默认 `64`。某个分片上编码后的数据攒到这个大小就上传一批。内存里还没上传的数据大约保持在这个值的 4 倍，`--thread` 个正在上传的批次另外占用内存。文件很大时把这个值调小，可以少占内存；调大则每批更大、提交次数更少。

`--retry` 是当前这一批失败后再试的次数，默认 `3`，`0` 表示不重试。`--retry-interval` 是两次尝试的间隔，单位秒，默认 `5`。只重试超时、连接中断和写入状态未知；主键冲突、校验和错误、参数错误不会重试。重试使用同一份文件和同一个会话号，不会把这一批再插入一遍。

多个 Compute 地址写成 `host:port,host:port`。导入期间每 60 秒打印进度：完成总条数、总跳过、从开始到现在的平均速度、最近一分钟的速度，以及因超时或连接中断实际重试的次数。全部成功后才发送结束指令，存储节点这时才会做合并。中途失败会立刻停掉心跳并抛出错误，不发送结束指令，也不做合并；还没提交的暂存文件由存储节点在心跳超时后删除。结束时打印起始时间、结束时间、总耗时、完成条数、跳过条数、平均速度、唯一索引构建时间、快照分发时间和会话号。失败时同一份结果里会多一行「导数发生错误」。
