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

VERSION="$(sed -n 's/.*<version>[[:space:]]*\([^<[:space:]]*\)[[:space:]]*<\/version>.*/\1/p' "$ROOT_DIR/pom.xml" | head -n 1)"
if [[ -z "$VERSION" ]]; then
    echo '[ERROR] 无法从 pom.xml 读取项目版本。' >&2
    exit 1
fi
PACKAGE_NAME="cengyan-$VERSION"
DIST_DIR="$ROOT_DIR/dist"
PACKAGE_ROOT="$DIST_DIR/$PACKAGE_NAME"
ARCHIVE_PATH="$DIST_DIR/$PACKAGE_NAME-linux.tar.gz"

echo '开始构建 cengyan-server 和 cengyan-studio ...'
(cd "$ROOT_DIR" && mvn -DskipTests -pl cengyan-server,cengyan-studio -am package)

rm -rf "$PACKAGE_ROOT" "$ARCHIVE_PATH"
mkdir -p "$PACKAGE_ROOT"/{bin,config,data,logs,pid,tmp,lib}

SERVER_JAR="$ROOT_DIR/cengyan-server/target/cengyan-server-$VERSION.jar"
STUDIO_JAR="$ROOT_DIR/cengyan-studio/target/cengyan-studio-$VERSION.jar"
for jar in "$SERVER_JAR" "$STUDIO_JAR"; do
    if [[ ! -f "$jar" ]]; then
        echo "[ERROR] 未找到构建产物：$jar" >&2
        exit 1
    fi
done
cp "$SERVER_JAR" "$PACKAGE_ROOT/cengyan-server.jar"
cp "$STUDIO_JAR" "$PACKAGE_ROOT/cengyan-studio.jar"
cp "$ROOT_DIR/cengyan.properties" "$PACKAGE_ROOT/config/cengyan.properties"
cp "$ROOT_DIR/cengyan.properties" "$PACKAGE_ROOT/config/cengyan.properties.example"
cp "$ROOT_DIR/packaging/studio.properties" "$PACKAGE_ROOT/config/studio.properties"
cp "$ROOT_DIR/packaging/studio.properties" "$PACKAGE_ROOT/config/studio.properties.example"
cp "$ROOT_DIR/packaging/bin/"*.sh "$PACKAGE_ROOT/bin/"
cp "$ROOT_DIR/packaging/bin/java.env" "$PACKAGE_ROOT/bin/java.env"
cp "$ROOT_DIR/packaging/PACKAGE-README.txt" "$PACKAGE_ROOT/README.txt"
cp "$ROOT_DIR/packaging/lib/README.txt" "$PACKAGE_ROOT/lib/README.txt"
chmod +x "$PACKAGE_ROOT/bin/"*.sh

tar -czf "$ARCHIVE_PATH" -C "$DIST_DIR" "$PACKAGE_NAME"
echo "打包完成：$PACKAGE_ROOT"
echo "压缩包：$ARCHIVE_PATH"
