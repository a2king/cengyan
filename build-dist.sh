#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

require_java21() {
    if [[ -n "${JAVA_HOME:-}" ]]; then
        JAVA_EXE="$JAVA_HOME/bin/java"
        if [[ ! -x "$JAVA_EXE" ]]; then
            echo "[ERROR] 需要 JDK 21，但 JAVA_HOME 指向的 Java 不存在：$JAVA_EXE" >&2
            exit 1
        fi
    else
        JAVA_EXE="$(command -v java || true)"
        if [[ -z "$JAVA_EXE" ]]; then
            echo '[ERROR] 未找到 Java。请安装 JDK 21，并设置 JAVA_HOME 或将 java 加入 PATH。' >&2
            exit 1
        fi
    fi

    local version major
    version="$("$JAVA_EXE" -version 2>&1 | awk -F'"' '/version/ {print $2; exit}')"
    if [[ -z "$version" ]]; then
        echo "[ERROR] 无法识别 Java 版本：$JAVA_EXE" >&2
        exit 1
    fi
    if [[ "$version" =~ ^1\.([0-9]+) ]]; then
        major="${BASH_REMATCH[1]}"
    else
        major="${version%%.*}"
    fi
    if [[ "$major" != '21' ]]; then
        echo "[ERROR] 项目必须使用 Java 21，当前检测到 Java $major（$version）。请设置 JAVA_HOME 后重试。" >&2
        exit 1
    fi
    echo "使用 Java $version：$JAVA_EXE"
}

require_java21
command -v mvn >/dev/null 2>&1 || { echo '[ERROR] 未找到 Maven。请安装 Maven，并将 mvn 加入 PATH。' >&2; exit 1; }
command -v python3 >/dev/null || { echo '[ERROR] 构建机需要 Python 3.11+' >&2; exit 1; }
python3 -c 'import sys; assert sys.version_info >= (3, 11), "Python 3.11+ required"'
TARGET=linux
SKIP_WEB=false
MAVEN_ARGS=(clean package)
while [[ $# -gt 0 ]]; do
    case "$1" in
        --target) TARGET="${2:?--target requires linux}"; shift 2 ;;
        --skip-tests) MAVEN_ARGS+=(-DskipTests); shift ;;
        --skip-web) SKIP_WEB=true; shift ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done
case "$TARGET" in linux) ;; *) echo 'Only the Linux tar.gz package is supported' >&2; exit 2 ;; esac
# Fail before the long build when the offline JDKs that Studio deploys are missing.
for jdk in jdk-21-linux-x64.tar.gz jdk-21-linux-aarch64.tar.gz; do
    [[ -f "$ROOT_DIR/packaging/jdk/$jdk" ]] || { echo "[ERROR] 缺少离线 JDK：packaging/jdk/$jdk。请下载 Temurin 21 Linux JDK 的 .tar.gz 并按此文件名保存" >&2; exit 1; }
done
# Keep Maven and Surefire on the same JDK even when JAVA_HOME was initially unset.
if [[ -z "${JAVA_HOME:-}" ]]; then export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$JAVA_EXE")")")"; fi
if [[ "$SKIP_WEB" == false ]]; then
    command -v npm >/dev/null || { echo '[ERROR] 构建 Web 需要 Node.js/npm' >&2; exit 1; }
    (cd "$ROOT_DIR/cengyan-studio-web" && npm ci --no-audit --no-fund && npm run build)
fi
(cd "$ROOT_DIR" && mvn "${MAVEN_ARGS[@]}")
python3 "$ROOT_DIR/packaging/assemble-dist.py" --target "$TARGET"
echo 'Linux tar.gz 打包完成；bin 内文件权限为 777，原有 dist 解压目录和数据未改动。'
