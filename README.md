# 层岩（Cengyan）图数据库

Java 21 图数据库，使用 Ratis 共识、RocksDB 分片存储、独立 Compute 和 GQL 查询。

## 统一运行架构

```text
Python ClusterClient / Java CLI / Studio
                  ↓ Compute client_address
Compute：认证、队列、执行协调、GQL、跨分片 Join、DuckDB
                  ↓ 内部 RPC / Protobuf
Storage：Metadata / Data Raft、RocksDB、事务、索引、Schema、拓扑任务
```

**单节点和多节点只差角色数量，不再有另一套单机 Server。**

- 单节点：同一机器启动一个 Storage 和一个 Compute，建图副本数为 1。
- 多节点：部署多个 Storage/Compute，图副本数独立设置，生产通常至少 3 副本。
- 只有 Storage 不提供外部 GQL 服务；客户端始终连接 Compute。
- 旧 `cengyan-server`、本地 `cengyan-store`/`cengyan-catalog`、本地 QueryEngine、
  `cengyan.properties`、`start-server`/`start-all` 和 Python `Client/GraphSession` 已删除。
- 已有分布式数据目录不变；旧单机库不支持直接接入新的 Storage。

## 构建与发布

要求：JDK 21、Maven 3.9+；构建 Web 还需符合 Vite 要求的 Node.js/npm；归档需 Python 3.11+。

```powershell
$env:JAVA_HOME = 'D:\workspace\code_env\jdk21'
.\build-dist.ps1
```

```bash
export JAVA_HOME=/opt/jdk-21
bash build-dist.sh
```

默认先构建 Web，再执行全量 Java `clean package`（含测试），最后归档到 `dist/`。
不要并行运行多个 Maven reactor 操作同一工作区。支持 `-SkipTests`/`--skip-tests`、
`-SkipWeb`/`--skip-web`，仅在明确需要时使用。

发布包内容：

```text
cengyan-storage.jar            数据库 Storage 角色
cengyan-compute.jar            数据库 Compute 角色
cengyan-cli.jar                可选交互式客户端（不是数据库进程）
cengyan-studio.jar             可选 Web 客户端服务（不是另一套数据库）
config/{storage,compute,studio}.properties
bin/start-cluster-node.sh
bin/{status,stop}-cluster-node.sh
sdk/python/
docs/
BUILD-INFO.json / SHA256SUMS
```

三个打包入口（`build-dist.ps1`、`build-dist.bat`、`build-dist.sh`）均只输出
`dist/cengyan-<版本>-linux.tar.gz`，不再生成 ZIP、Windows 包或外部 `.sha256` 文件。
保留包内 `BUILD-INFO.json` 和 `SHA256SUMS`。兼容 `-Target linux` / `--target linux`，不再接受 `windows` / `all`。
新包发布成功后清理标准命名的旧发布归档及其校验文件；自定义包、已有解压目录和数据不会被删除。
包内 `bin/` 下所有文件（包括 `java.env`）均为 `777`，目录为 `755`；在 Linux 上执行 `tar -xzpf` 解压可保留权限，无需手动 chmod。
`777` 允许所有用户修改脚本与环境配置，仅应在受信任的部署目录使用。
不包含 JDK。Linux 脚本兼容 Bash 4.2+，
但 RocksDB/DuckDB 原生库仍要求兼容的 glibc/libstdc++，不保证 CentOS 7 可直接运行。
打包不会删除已有 `dist/cengyan-<version>` 解压目录和数据。

## 单节点快速启动

以下仅用于全新的本机开发环境。默认两份配置各自只有一个回环网络成员。
先修改 `config/compute.properties` 中的 `root_password`。

```bash
export JAVA_HOME=/opt/jdk-21
./bin/start-cluster-node.sh --daemon storage
./bin/start-cluster-node.sh --daemon compute

./bin/status-cluster-node.sh storage
./bin/status-cluster-node.sh compute
```

Windows（分别在两个终端前台运行）：

```powershell
.\bin\start-cluster-node.ps1 -Role storage
.\bin\start-cluster-node.ps1 -Role compute
```

Linux 不加 `--daemon` 也为前台模式，适合 systemd/container。状态检查是进程检查，不是 Raft readiness。
正常停机先 Compute，后 Storage；停止脚本不会自动 `kill -9`。

```bash
./bin/stop-cluster-node.sh compute
./bin/stop-cluster-node.sh storage
```

默认端口：Storage Raft `19700`、RPC `19701`；Compute Raft `19800`、RPC `19801`、客户端 `19669`。
Storage/Compute 的相对 `data_dir` 都按配置文件目录解析；正式部署建议使用独立的绝对路径。
已有多节点集群不能靠删配置条目缩成单节点，必须提交排空/成员移除操作。

## 多节点与动态扩容

完整步骤见 [Linux 真机部署](docs/linux-cluster-deployment.md)。

- 首次 Bootstrap：各角色配置相同的初始成员列表、本机唯一 node_id 和真实地址。
- 后续 Join：新节点配置现有成员作为联系入口，列表不包含新节点自己；老节点不用改配置重启。
- Storage 成员格式：`id@raft@rpc`；Compute：`id@rpc@raft@client`。
- 正式下线 Storage：`drainStorage` → 等待分片迁移 → `removeStorage` → 停止旧进程。
- 正式下线 Compute：`drainCompute` → 等待任务结束 → 从另一 Compute 入口执行 `removeCompute`。
- 所有正式 Storage 参与 Metadata 投票，所有正式 Compute 参与 Compute Group 投票；成员变更需多数派。
- 内部 RPC/Raft 不重复用户认证，禁止暴露给不可信网络；多机必须同步时钟。

## Python SDK（单节点、多节点共用）

```bash
python3 -m pip install ./sdk/python
```

```python
from cengyan import ClusterClient

# 多节点只需改为多个 (host, port)。单节点仍连接 Compute。
with ClusterClient([('127.0.0.1', 19669)], password='你的初始化密码') as client:
    client.create_graph('Company', partitions=3, replicas=1,
                        vertex_id_type='STRING', edge_id_type='STRING')
    graph = client.graph('Company')
    graph.write('CREATE NODE TYPE Company (name STRING NULL)')
    graph.insert_nodes('Company', [{'id': 'a', 'name': 'Alice'}])
    print(graph.read('MATCH (n:Company) RETURN n.id, n.name').to_dicts())
```

图身份随每个请求显式传递。读断线不重算；写断线最多重试三次并复用 requestId；
批量导入按网络批次提交，失败停止并报告已确认进度。SDK 无 requests 依赖。
旧 `Client/GraphSession` API 已删除，使用方必须迁移到 ClusterClient。

## 客户端工具

Java CLI 可以连接 Compute，保存登录令牌并在客户端附带图上下文：

```bash
java -jar cengyan-cli.jar --host 127.0.0.1 --port 19669
```

```text
:login root 你的初始化密码
USE Company
MATCH (n:Company) RETURN n.id, n.name
:drainStorage storage-3
:removeStorage storage-3
```

旧单机的 addStorage/addCompute/setPartCount/rebuildIndex 管理命令已禁用；新增节点用自动 Join。
Studio 前后端代码作为可选客户端保留，配置 `db_host/db_port` 指向 Compute。
完整集群管理适配尚需验收，首次部署以 ClusterClient 为准，不把 Studio 作为就绪证明。

## 源码模块

| 模块 | 用途 |
|---|---|
| cengyan-common | 公共查询错误码、异常、取消契约；不包含数据库 |
| cengyan-protocol / rpc | Protobuf、固定分片路由、Netty RPC/流控 |
| cengyan-raft | Multi-Raft、RocksDB 状态机、快照和恢复 |
| cengyan-distributed | 图目录、图实体、事务、索引、Schema 和副本任务 |
| cengyan-parser / query | GQL 解析、远程查询协调、表达式、DuckDB |
| cengyan-storage / compute | 两个数据库角色入口 |
| cengyan-cli / studio | 可选客户端工具 |

## 验证与边界

```bash
mvn clean test
# 在 cengyan-cli-python 目录：
PYTHONPATH=src python3 -m unittest discover -s tests -v
# JAR 构建后的真实独立进程冒烟：
PYTHONPATH=src python3 tests/cluster_process_smoke.py --java /opt/jdk-21/bin/java
```

移除旧实现后不再运行它的专用测试，不能与历史的 186 个测试直接比较。
现有远程集成测试覆盖登录、权限、建图、点边导入、更新、索引、聚合、路径和集群 Join；
单机一 Storage + 一 Compute 还通过独立 JAR/SDK 冒烟验证。

历史 [perf-report](docs/perf-report.md) 是已移除实现的参考数据，不代表当前角色架构性能。
真实多机网络分区、长期压力、磁盘故障和生产容量仍需专项验收。

文档：[索引](docs/README.md) · [架构](docs/distributed-architecture.md) ·
[状态](docs/distributed-implementation-status.md) · [部署](docs/linux-cluster-deployment.md) ·
[批量导入](docs/bulk-import.md) · [GQL](docs/gql/00-和标准相比.md)
