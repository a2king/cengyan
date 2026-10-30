可选的应用私有 Linux 运行库

如果服务器系统的 /lib64/libstdc++.so.6 缺少 GLIBCXX_3.4.20，
可以把与服务器 glibc 版本和 CPU 架构兼容的 libstdc++.so.6 放在本目录。
bin/start-*.sh 会自动把本目录加入 LD_LIBRARY_PATH。

这里只能放 libstdc++.so.6。不要放 libc.so.6、ld-linux-x86-64.so.2 或其他 glibc 文件，
否则会覆盖服务器的动态加载器，导致 awk、Java 等程序出现 GLIBC_PRIVATE 错误。

请先确认库文件满足：
  strings libstdc++.so.6 | grep GLIBCXX_3.4.20

不能随意从新版本 Ubuntu 复制库文件到旧系统；如果该库依赖更高版本 glibc，
仍然会加载失败。优先从相同发行版、相同架构、较新 GCC 运行环境中取得。
