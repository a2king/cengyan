层岩图数据库部署包

目录：
  cengyan-server.jar       图数据库服务
  cengyan-studio.jar       Web 控制台后端
  bin/                     Windows 和 Linux 启停命令
  config/                  配置文件和样例
  data/                    数据目录
  logs/                    各服务日志目录
  pid/                     各服务 PID 文件目录
  tmp/                     临时文件目录
  lib/                     可选的应用私有 Linux 运行库

要求：JDK 21。启动命令会先检查 Java 主版本；检测到 Java 8、17 或其他版本时会中断并输出原因。

Windows（命令会后台启动，日志写入 logs/）：
  bin\start-server.bat     启动图数据库
  bin\stop-server.bat      停止图数据库
  bin\start-studio.bat     启动 Web 控制台
  bin\stop-studio.bat      停止 Web 控制台
  bin\start-all.bat        启动全部服务
  bin\stop-all.bat         停止全部服务

Linux（命令会后台启动，日志写入 logs/）：
  ./bin/start-server.sh     启动图数据库
  ./bin/stop-server.sh      停止图数据库
  ./bin/start-studio.sh     启动 Web 控制台
  ./bin/stop-studio.sh      停止 Web 控制台
  ./bin/start-all.sh        启动全部服务
  ./bin/stop-all.sh         停止全部服务
  bin/java.env              可选的 JDK 21 路径配置

PID 文件：
  pid/server.pid
  pid/studio.pid

日志文件：
  logs/server.log
  logs/studio.log
Windows 启动脚本的标准错误另外写入：
  logs/server-error.log
  logs/studio-error.log

服务端配置：config/cengyan.properties
Studio 配置：config/studio.properties
Studio 配置中的 host 控制 Web 控制台监听地址，部署到内网时通常填写：
  host=0.0.0.0
然后通过服务器实际内网 IP 和端口访问，例如：http://192.168.1.20:19670
如果服务器的 Java 21 不在系统 PATH 中，编辑 bin/java.env，填写：
  export JAVA_HOME=/你的/jdk-21/目录
如果系统自带的 libstdc++.so.6 太旧，也可以在 bin/java.env 配置兼容库目录，例如：
  export LD_LIBRARY_PATH=/opt/rh/gcc-toolset-11/root/usr/lib64:${LD_LIBRARY_PATH:-}
也可以把与服务器 glibc 和 CPU 架构兼容的 libstdc++.so.6 放进 lib/；启动命令会自动优先使用它。
不要把 libc.so.6 或 ld-linux*.so.* 放进应用 lib/，否则会覆盖系统 glibc 并导致命令无法运行。
修改配置后重新执行对应启动命令即可。服务端的数据目录按启动包根目录解析，默认写入 data/data。
