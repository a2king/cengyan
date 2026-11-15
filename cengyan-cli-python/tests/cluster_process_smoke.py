"""真实独立 Storage/Compute JAR + Python SDK 冒烟，不修改已有数据目录。

使用：PYTHONPATH=src python tests/cluster_process_smoke.py --java <jdk21>/bin/java
"""
import argparse
import json
import socket
import subprocess
import tempfile
import time
from pathlib import Path

from pycengyan import ClusterClient


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--java", required=True)
    parser.add_argument("--package-root", type=Path, help="解压后的部署包目录；验证包内 JAR 而非 target 产物")
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    version = "1.0.0"
    reservations = []
    try:
        for _ in range(5):
            held = socket.socket()
            held.bind(("127.0.0.1", 0))
            reservations.append(held)
        ports = [held.getsockname()[1] for held in reservations]
    finally:
        for held in reservations:
            held.close()
    processes, logs = [], []
    with tempfile.TemporaryDirectory(prefix="cengyan-process-smoke-") as temporary:
        root = Path(temporary)
        storage_config = root / "storage.properties"
        compute_config = root / "compute.properties"
        storage_config.write_text("\n".join([
            "cluster_id=process-smoke", "node_id=s1", f"rpc_address=127.0.0.1:{ports[0]}",
            f"raft_address=127.0.0.1:{ports[1]}",
            f"metadata_members=s1@127.0.0.1:{ports[1]}@127.0.0.1:{ports[0]}",
            "data_dir=storage", "admission_threads=4", "rpc_max_inflight=128",
        ]), encoding="utf-8")
        compute_config.write_text("\n".join([
            "cluster_id=process-smoke", "node_id=c1", f"client_address=127.0.0.1:{ports[2]}",
            f"rpc_address=127.0.0.1:{ports[3]}", f"raft_address=127.0.0.1:{ports[4]}",
            f"compute_members=c1@127.0.0.1:{ports[3]}@127.0.0.1:{ports[4]}@127.0.0.1:{ports[2]}",
            f"storage_seeds=127.0.0.1:{ports[0]}", "data_dir=compute", "root_password=secret",
            "query_slots=2", "node_memory_bytes=268435456", "node_temp_bytes=2147483648",
        ]), encoding="utf-8")
        try:
            for role, config in (("storage", storage_config), ("compute", compute_config)):
                jar = (args.package_root / f"cengyan-{role}.jar" if args.package_root
                       else repo / f"cengyan-{role}" / "target" / f"cengyan-{role}-{version}.jar")
                if not jar.exists():
                    raise RuntimeError(f"先执行 mvn package：{jar}")
                log = (root / f"{role}.log").open("wb")
                logs.append(log)
                processes.append(subprocess.Popen([args.java, "-Xmx256m", "-jar", str(jar), str(config)], stdout=log, stderr=subprocess.STDOUT))
            deadline = time.monotonic() + 30
            for port in (ports[0], ports[2]):
                while True:
                    if any(process.poll() is not None for process in processes):
                        raise RuntimeError("节点启动提前退出")
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                            break
                    except OSError as error:
                        if time.monotonic() >= deadline:
                            raise RuntimeError("节点启动超时") from error
                        time.sleep(0.05)
            with ClusterClient([("127.0.0.1", ports[2])], password="secret", timeout=30) as client:
                client.create_graph("Smoke", partitions=3, vertex_id_type="STRING", edge_id_type="STRING")
                graph = client.graph("Smoke")
                graph.execute("CREATE NODE TYPE N (name STRING NULL)", write=True)
                graph.execute("CREATE REL TYPE E ()", write=True)
                inserted = graph.insert_nodes("N", [{"id": "a", "name": "Alice"}, {"id": "b", "name": "Bob"}], batch_size=1)
                assert inserted["confirmed"] == 2, inserted
                edges = graph.insert_edges("E", [{"id": "e1", "src": "a", "dst": "b"}, {"id": "skip", "src": "missing", "dst": "b"}], skip_missing_endpoints=True)
                assert edges["confirmed"] == 1 and edges["skipped"] == 1, edges
                result = graph.execute("MATCH (a:N {id:'a'})-[:E]->(b:N) RETURN b.name AS name").to_dicts()
                assert result == [{"name": "Bob"}], result
                assert len(client.refresh_nodes()) == 1
                print(json.dumps({"status": "OK", "nodes": inserted, "edges": edges, "query": result}, ensure_ascii=False))
        except Exception:
            for log in logs:
                log.flush()
            for role in ("storage", "compute"):
                file = root / f"{role}.log"
                if file.exists():
                    print(file.read_text(encoding="utf-8", errors="replace")[-10000:])
            raise
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            for log in logs:
                log.close()


if __name__ == "__main__":
    main()
