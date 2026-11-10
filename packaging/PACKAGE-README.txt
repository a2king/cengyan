层岩图数据库：Studio 部署包

部署时只需在一台机器上解压并启动 Studio。服务器、存储节点、计算节点的安装、配置、
滚动重启、升级和移除都在 Studio 网页上完成。

内容：
  cengyan-studio.jar       Studio 控制台（网页、集群管理）
  cengyan-cli.jar          可选 Java 交互式客户端
  nodes/                  Studio 下发到各服务器的 Storage / Compute 程序和日志配置
  jdk/                    离线 JDK 21（Linux x64、aarch64），Studio 推送到各服务器
  config/studio.properties Studio 监听地址等
  config/logging.properties Studio 日志级别
  data/                   Studio 自身状态：服务器、节点、配置版本、SSH 私钥（升级时必须保留）
  sdk/python/             Python 客户端
  docs/                   设计和运维文档
  BUILD-INFO.json          构建信息
  SHA256SUMS              包内文件校验

启动：
  tar -xzpf cengyan-<版本>-linux.tar.gz
  cd cengyan-<版本>
  ./bin/start-studio.sh
  浏览器打开 http://<本机 IP>:19670 ，点击"集群管理"

  本机没有 Java 21 时，start-studio.sh 自动把 jdk/ 中对应架构的 JDK 解压到 runtime/jdk 使用。
  也可以在 bin/java.env 中设置 JAVA_HOME。

首次使用：
  1. 设置 Studio 管理员密码。
  2. 添加服务器：填写内网 IP 和 root 密码。Studio 检查系统、配置免密登录（随后丢弃密码）、
     安装 JDK 21、同步程序。
  3. 添加存储节点（生产环境至少 3 个），设置数据库 root 密码，再添加计算节点。
  4. 节点就绪后，用数据库账号登录 Studio 开始建图和查询。

被管理服务器要求：Linux x86_64 或 aarch64，glibc 2.17+，systemd，root SSH 登录，
/opt 所在磁盘至少 1GB 可用。节点安装在 /opt/cengyan，以 systemd 服务 cengyan-<节点ID> 运行，
日志用 journalctl -u cengyan-<节点ID> 查看，也可在 Studio 节点页查看。

停止 Studio：./bin/stop-studio.sh 。停止 Studio 不影响已运行的节点。

升级：
  1. ./bin/stop-studio.sh 停止旧 Studio。
  2. 解压新包到新目录，把旧目录的 data/ 和 config/studio.properties 复制过去。
  3. 启动新 Studio。节点版本低于 Studio 随附版本时，概览页提示"升级集群"，确认后逐台滚动升级。

安全：data/ssh/id_rsa 是登录所有受管服务器的 root 私钥，data/ 目录只允许运行 Studio 的用户读取。
Studio 端口只开放给可信内网。bin/ 下文件权限为 777，应部署到受信任目录。

完整设计见 docs/studio-cluster-management.md。
