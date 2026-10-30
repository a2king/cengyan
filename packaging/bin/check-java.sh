#!/usr/bin/env bash

JAVA_CONFIG_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
JAVA_CONFIG_FILE="$JAVA_CONFIG_DIR/java.env"
if [[ -f "$JAVA_CONFIG_FILE" ]]; then
    # Optional local Java configuration for this deployment package.
    # shellcheck disable=SC1090
    source "$JAVA_CONFIG_FILE"
fi
if [[ -n "${LD_LIBRARY_PATH:-}" ]]; then
    export LD_LIBRARY_PATH
fi
PRIVATE_LIB_DIR="$JAVA_CONFIG_DIR/../lib"
for PRIVATE_LOADER in "$PRIVATE_LIB_DIR"/ld-linux*.so.* "$PRIVATE_LIB_DIR"/libc.so.6; do
    if [[ -e "$PRIVATE_LOADER" ]]; then
        echo "[ERROR] $PRIVATE_LOADER 不能放入应用 lib/ 目录。请删除 libc.so.6 和 ld-linux*.so.*，只保留兼容的 libstdc++.so.6。" >&2
        return 1
    fi
done
if [[ -f "$PRIVATE_LIB_DIR/libstdc++.so.6" ]]; then
    export LD_LIBRARY_PATH="$PRIVATE_LIB_DIR:${LD_LIBRARY_PATH:-}"
fi

require_java21() {
    if [[ -n "${JAVA_HOME:-}" ]]; then
        JAVA_EXE="$JAVA_HOME/bin/java"
        if [[ ! -x "$JAVA_EXE" ]]; then
            echo "[ERROR] 需要 JDK 21，但 JAVA_HOME 指向的 Java 不存在：$JAVA_EXE" >&2
            return 1
        fi
    else
        JAVA_EXE="$(command -v java || true)"
        if [[ -z "$JAVA_EXE" ]]; then
            echo '[ERROR] 未找到 Java。请安装 JDK 21，并设置 JAVA_HOME 或将 java 加入 PATH。' >&2
            return 1
        fi
    fi

    local version major
    version="$("$JAVA_EXE" -version 2>&1 | awk -F'"' '/version/ {print $2; exit}')"
    if [[ -z "$version" ]]; then
        echo "[ERROR] 无法识别 Java 版本：$JAVA_EXE" >&2
        return 1
    fi
    if [[ "$version" =~ ^1\.([0-9]+) ]]; then
        major="${BASH_REMATCH[1]}"
    else
        major="${version%%.*}"
    fi
    if [[ "$major" != '21' ]]; then
        echo "[ERROR] 项目必须使用 Java 21，当前检测到 Java $major（$version）。请设置 JAVA_HOME 后重试。" >&2
        return 1
    fi
    return 0
}
