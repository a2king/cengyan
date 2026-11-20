"""Assemble one Linux file named cengyan-client.

The jar and a private Temurin 21 JRE are appended after the shell header. The first run unpacks them under the user cache, then later runs start directly.
"""

import argparse
import hashlib
import io
import tarfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCHES = {"x64": "x64", "x86_64": "x64", "aarch64": "aarch64", "arm64": "aarch64"}

LAUNCHER = """#!/bin/bash
set -euo pipefail
PAYLOAD_OFFSET=000000000000
STAMP=0000000000000000000000000000000000000000000000000000000000000000
SOURCE=$0
while [ -L "$SOURCE" ]; do
    DIR=$(CDPATH= cd -- "$(dirname -- "$SOURCE")" && pwd)
    LINK=$(readlink "$SOURCE")
    case $LINK in
        /*) SOURCE=$LINK ;;
        *) SOURCE=$DIR/$LINK ;;
    esac
done
case $SOURCE in
    /*) ;;
    *) SOURCE=$(CDPATH= cd -- "$(dirname -- "$SOURCE")" && pwd)/$(basename "$SOURCE") ;;
esac
DEST=${XDG_CACHE_HOME:-$HOME/.cache}/cengyan/client/$STAMP
if [ ! -x "$DEST/runtime/bin/java" ] || [ ! -f "$DEST/cengyan-client.jar" ]; then
    mkdir -p "$(dirname "$DEST")"
    LOCK=$DEST.lock
    if mkdir "$LOCK" 2>/dev/null; then
        trap 'rmdir "$LOCK" 2>/dev/null || true' EXIT
        rm -rf "$DEST.partial"
        mkdir -p "$DEST.partial"
        echo "正在展开客户端运行时，只需这一次..." >&2
        tail -c +$((10#$PAYLOAD_OFFSET + 1)) "$SOURCE" | tar -xzf - -C "$DEST.partial"
        rm -rf "$DEST"
        mv "$DEST.partial" "$DEST"
        rmdir "$LOCK"
        trap - EXIT
    else
        while [ ! -x "$DEST/runtime/bin/java" ]; do
            sleep 1
        done
    fi
fi
exec "$DEST/runtime/bin/java" -jar "$DEST/cengyan-client.jar" "$@"
"""


def normalize_arch(name):
    try:
        return ARCHES[name]
    except KeyError:
        raise SystemExit("架构只能是 x64 或 aarch64") from None


def jre_cache(arch):
    return ROOT / "packaging" / "jdk" / f"temurin-jre-21-linux-{arch}.tar.gz"


def jre_url(arch):
    return ("https://api.adoptium.net/v3/binary/latest/21/ga/linux/"
            f"{arch}/jre/hotspot/normal/eclipse?project=jdk")


def ensure_jre(arch):
    path = jre_cache(arch)
    if path.is_file() and path.stat().st_size > 1_000_000:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tar.gz.partial")
    request = urllib.request.Request(jre_url(arch), headers={"User-Agent": "cengyan-build-client"})
    with urllib.request.urlopen(request, timeout=120) as response, temporary.open("wb") as output:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            output.write(chunk)
    temporary.replace(path)
    return path


def default_jar():
    matches = sorted((ROOT / "cengyan-client" / "target").glob("cengyan-client-*.jar"))
    jars = [path for path in matches if "sources" not in path.name and "javadoc" not in path.name]
    if not jars:
        raise SystemExit("还没有客户端 jar。先执行 mvn -pl cengyan-client -am package -DskipTests")
    return jars[-1]


def jre_prefix(members):
    javas = [member.name for member in members if member.name.endswith("bin/java") and member.isfile()]
    if len(javas) != 1:
        raise SystemExit("JRE 压缩包里应该正好有一个 bin/java")
    prefix = javas[0][:-len("bin/java")]
    if prefix and not prefix.endswith("/"):
        raise SystemExit(f"无法识别 JRE 目录：{javas[0]}")
    return prefix


def add_jre(archive, jre_tar, destination):
    with tarfile.open(jre_tar) as source:
        members = source.getmembers()
        prefix = jre_prefix(members)
        for member in members:
            if prefix and not member.name.startswith(prefix):
                continue
            relative = member.name[len(prefix):]
            if not relative or relative.startswith("/") or ".." in Path(relative).parts:
                continue
            info = tarfile.TarInfo(f"{destination}runtime/{relative}")
            info.mode = member.mode
            info.mtime = member.mtime
            info.uid = 0
            info.gid = 0
            info.type = member.type
            if member.isdir():
                archive.addfile(info)
            elif member.isfile():
                info.size = member.size
                archive.addfile(info, source.extractfile(member))
            elif member.issym() or member.islnk():
                link = member.linkname
                if member.islnk() and prefix and link.startswith(prefix):
                    link = f"{destination}runtime/{link[len(prefix):]}"
                info.linkname = link
                archive.addfile(info)


def add_bytes(archive, name, data, mode):
    info = tarfile.TarInfo(name)
    info.size = len(data)
    info.mode = mode
    info.mtime = 0
    archive.addfile(info, io.BytesIO(data))


def render(offset, stamp):
    text = LAUNCHER.replace("PAYLOAD_OFFSET=" + "0" * 12, f"PAYLOAD_OFFSET={offset:012d}", 1)
    text = text.replace("STAMP=" + "0" * 64, "STAMP=" + stamp, 1)
    return text.encode("utf-8")


def single_file(jar, jre_tar):
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w:gz") as archive:
        add_bytes(archive, "cengyan-client.jar", Path(jar).read_bytes(), 0o644)
        add_jre(archive, jre_tar, "")
    body = payload.getvalue()
    stamp = hashlib.sha256(body).hexdigest()
    offset = len(render(0, "0" * 64))
    script = render(offset, stamp)
    if len(script) != offset:
        raise SystemExit("启动脚本长度和载荷偏移不一致")
    return script + body


def build(jar, jre_tar, output, arch):
    del arch
    output.parent.mkdir(parents=True, exist_ok=True)
    packed = single_file(jar, jre_tar)
    temporary = output.with_suffix(".tar.gz.partial")
    with tarfile.open(temporary, "w:gz") as archive:
        add_bytes(archive, "cengyan-client", packed, 0o755)
    temporary.replace(output)
    return output


def main():
    parser = argparse.ArgumentParser(description="打包可直接执行的 Linux 客户端")
    parser.add_argument("--arch", default="x64", help="x64（默认）或 aarch64")
    parser.add_argument("--jar", type=Path, help="要打进包里的客户端 jar，默认用 target 里的 shaded jar")
    args = parser.parse_args()
    arch = normalize_arch(args.arch)
    jar = args.jar or default_jar()
    if not jar.is_file():
        raise SystemExit(f"找不到 jar：{jar}")
    jre = ensure_jre(arch)
    output = ROOT / "dist" / f"cengyan-client-linux-{arch}.tar.gz"
    build(jar, jre, output, arch)
    print(output)


if __name__ == "__main__":
    main()
