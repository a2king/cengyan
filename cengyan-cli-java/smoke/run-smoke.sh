#!/usr/bin/env bash
# 真实进程冒烟：用 JDK 21 在临时目录启动一套 Storage + Compute，SDK 在指定 JDK（默认 JDK 8）上运行 ClusterSmoke。
# 不修改任何已有数据目录。需要先构建服务端 jar（仓库根目录 mvn package）和本 SDK（mvn package）。
#
# 用法：smoke/run-smoke.sh <server-jdk21-java> <sdk-java> [repo-root]
set -euo pipefail

SERVER_JAVA=${1:?需要 JDK 21 的 java}
SDK_JAVA=${2:?需要运行 SDK 的 java（如 JDK 8）}
SDK_DIR=$(cd "$(dirname "$0")/.." && pwd)
REPO=${3:-$(cd "$SDK_DIR/.." && pwd)}
VERSION=1.0.0

STORAGE_JAR=$REPO/cengyan-storage/target/cengyan-storage-$VERSION.jar
COMPUTE_JAR=$REPO/cengyan-compute/target/cengyan-compute-$VERSION.jar
SDK_JAR=$SDK_DIR/target/cengyan-cli-java-$VERSION.jar
for jar in "$STORAGE_JAR" "$COMPUTE_JAR" "$SDK_JAR"; do
  [ -f "$jar" ] || { echo "缺少 $jar，先执行 mvn package" >&2; exit 2; }
done

WORK=$(mktemp -d -t cengyan-java-smoke-XXXXXX)
PIDS=()
cleanup() {
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
  rm -rf "$WORK"
}
trap cleanup EXIT

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'; }
P=(); for _ in 1 2 3 4 5; do P+=("$(free_port)"); done

cat > "$WORK/storage.properties" <<EOF
cluster_id=java-smoke
node_id=s1
rpc_address=127.0.0.1:${P[0]}
raft_address=127.0.0.1:${P[1]}
metadata_members=s1@127.0.0.1:${P[1]}@127.0.0.1:${P[0]}
data_dir=storage
admission_threads=4
rpc_max_inflight=128
EOF
cat > "$WORK/compute.properties" <<EOF
cluster_id=java-smoke
node_id=c1
client_address=127.0.0.1:${P[2]}
rpc_address=127.0.0.1:${P[3]}
raft_address=127.0.0.1:${P[4]}
compute_members=c1@127.0.0.1:${P[3]}@127.0.0.1:${P[4]}@127.0.0.1:${P[2]}
storage_seeds=127.0.0.1:${P[0]}
data_dir=compute
root_password=secret
query_slots=2
node_memory_bytes=268435456
node_temp_bytes=2147483648
EOF

cd "$WORK"
"$SERVER_JAVA" -Xmx256m -jar "$STORAGE_JAR" storage.properties > storage.log 2>&1 & PIDS+=($!)
"$SERVER_JAVA" -Xmx256m -jar "$COMPUTE_JAR" compute.properties > compute.log 2>&1 & PIDS+=($!)

for port in "${P[0]}" "${P[2]}"; do
  for _ in $(seq 1 300); do
    if (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then break; fi
    sleep 0.1
  done
done

if ! "$SDK_JAVA" -cp "$SDK_JAR:$SDK_DIR/target/test-classes" com.cengyan.core.smoke.ClusterSmoke "127.0.0.1:${P[2]}" secret; then
  echo "---- storage.log" >&2; tail -c 5000 storage.log >&2
  echo "---- compute.log" >&2; tail -c 5000 compute.log >&2
  exit 1
fi
