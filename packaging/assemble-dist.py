"""Assemble one verified Linux tar.gz: Studio plus the node jars and offline JDKs it deploys over SSH.
bin files are set to 777; existing deployment directories are preserved."""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import xml.etree.ElementTree as ET
import zipfile


ROOT = Path(__file__).resolve().parent.parent
ROLES = ("storage", "compute", "studio", "cli")
# Studio deploys Storage/Compute to managed servers over SSH; their jars ship under nodes/.
NODE_ROLES = ("storage", "compute")
ROLE_CONFIGS = ("studio",)
LOG_CONFIGS = ("logging.properties",)
# Explicit allowlist: stale files in a working tree must not resurrect old launchers.
# Per-node scripts stay in packaging/bin for manual testing but are not shipped.
COMMON_SCRIPTS = {"java.env"}
LINUX_SCRIPTS = {"check-java.sh", "start-studio.sh", "stop-studio.sh"}
# Temurin 21 Linux archives placed in packaging/jdk/ (not in git); Studio pushes them to servers.
JDKS = {"x86_64": "jdk-21-linux-x64.tar.gz", "aarch64": "jdk-21-linux-aarch64.tar.gz"}
DOCS = ("studio-cluster-management.md", "rebuild-data-mode.md", "distributed-implementation-status.md")


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def copy_file(source, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    if target.suffix in (".sh", ".properties", ".example", ".md", ".txt", ".xml") or target.name == "java.env":
        text = target.read_text(encoding="utf-8-sig")
        target.write_bytes(text.replace("\r\n", "\n").replace("\r", "\n").encode("utf-8"))


def verify_jar(jar, role):
    main = {"storage": "io.cengyan.storage.StorageServer", "compute": "io.cengyan.compute.ComputeServer",
             "cli": "io.cengyan.cli.CengyanCli", "studio": "io.cengyan.studio.StudioServer"}[role]
    with zipfile.ZipFile(jar) as archive:
        if archive.testzip() is not None:
            raise RuntimeError(f"Corrupt JAR: {jar}")
        manifest = archive.read("META-INF/MANIFEST.MF").decode("utf-8").replace("\r\n ", "")
        if f"Main-Class: {main}" not in manifest:
            raise RuntimeError(f"Unexpected entry point in {jar}: expected {main}")
        names = set(archive.namelist())
        if role in ("storage", "compute"):
            for native in ("librocksdbjni-linux64.so", "librocksdbjni-linux-aarch64.so"):
                if native not in names:
                    raise RuntimeError(f"Missing Linux native library {native}: {jar}")
        if role == "compute":
            for native in ("libduckdb_java.so_linux_amd64", "libduckdb_java.so_linux_arm64"):
                if native not in names:
                    raise RuntimeError(f"Missing Linux native library {native}: {jar}")


def verify_jdk(archive, arch):
    """Studio unpacks with tar --strip-components=1: one top-level directory containing bin/java."""
    if not archive.is_file():
        raise RuntimeError(f"Missing offline JDK for {arch}: {archive}. Download the Temurin 21 Linux {arch} "
                           f"JDK .tar.gz and save it as packaging/jdk/{archive.name}")
    try:
        with tarfile.open(archive, "r:gz") as tar:
            names = [member.name.lstrip("./") for member in tar.getmembers()]
    except (tarfile.TarError, OSError) as error:
        raise RuntimeError(f"Corrupt JDK archive {archive}: {error}") from error
    tops = {name.split("/", 1)[0] for name in names if name}
    if len(tops) != 1:
        raise RuntimeError(f"JDK archive must contain exactly one top-level directory: {archive}")
    top = next(iter(tops))
    if f"{top}/bin/java" not in names:
        raise RuntimeError(f"JDK archive has no {top}/bin/java: {archive}")


def assemble(package, version):
    for directory in ("bin", "config", "data", "logs", "pid", "docs", "nodes", "jdk"):
        (package / directory).mkdir(parents=True, exist_ok=True)
    for role in ROLES:
        jar = ROOT / f"cengyan-{role}/target/cengyan-{role}-{version}.jar"
        verify_jar(jar, role)
        target = package / "nodes" if role in NODE_ROLES else package
        copy_file(jar, target / f"cengyan-{role}.jar")
    copy_file(ROOT / "packaging/logback.xml", package / "nodes/logback.xml")
    jdks = {}
    for arch, name in JDKS.items():
        source = ROOT / "packaging/jdk" / name
        verify_jdk(source, arch)
        copy_file(source, package / "jdk" / name)
        jdks[name] = sha256(package / "jdk" / name)
    for role in ROLE_CONFIGS:
        for suffix in ("", ".example"):
            copy_file(ROOT / f"packaging/{role}.properties", package / f"config/{role}.properties{suffix}")
    for name in LOG_CONFIGS:
        copy_file(ROOT / "packaging" / name, package / "config" / name)
    scripts = COMMON_SCRIPTS | LINUX_SCRIPTS
    for name in sorted(scripts):
        copy_file(ROOT / "packaging/bin" / name, package / "bin" / name)
    copy_file(ROOT / "packaging/PACKAGE-README.txt", package / "README.txt")
    for name in DOCS:
        copy_file(ROOT / "docs" / name, package / "docs" / name)
    sdk = ROOT / "cengyan-cli-python"
    copy_file(sdk / "pyproject.toml", package / "sdk/python/pyproject.toml")
    # pyproject 里声明了 readme = "README.md"，缺了它包内 `pip install` 会失败。
    copy_file(sdk / "README.md", package / "sdk/python/README.md")
    for source in (sdk / "src").rglob("*.py"):
        copy_file(source, package / "sdk/python/src" / source.relative_to(sdk / "src"))
    try:
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, encoding="utf-8").strip()
        dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True, encoding="utf-8", errors="replace").strip())
    except (OSError, subprocess.CalledProcessError):
        revision, dirty = "unknown", True
    jars = {}
    for role in ROLES:
        relative = f"nodes/cengyan-{role}.jar" if role in NODE_ROLES else f"cengyan-{role}.jar"
        jars[relative] = sha256(package / relative)
    metadata = {"version": version, "target": "linux", "java": 21, "gitRevision": revision,
                "workingTreeDirty": dirty, "linuxNativeArchitectures": list(JDKS), "jars": jars, "jdks": jdks}
    # newline="\n": on Windows write_text would emit CRLF and `sha256sum -c` on Linux could not find the files.
    (package / "BUILD-INFO.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8", newline="\n")
    lines = [f"{sha256(file)}  {file.relative_to(package).as_posix()}" for file in sorted(package.rglob("*")) if file.is_file()]
    (package / "SHA256SUMS").write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def unix_info(info):
    info.uid = info.gid = 0
    info.uname = info.gname = "root"
    parts = Path(info.name).parts
    if not info.isdir() and len(parts) > 1 and parts[1] == "bin":
        info.mode = 0o777
    else:
        info.mode = 0o755 if info.isdir() or info.name.endswith(".sh") else 0o644
    return info


def write_tar(package, output):
    with tarfile.open(output, "w:gz") as archive:
        archive.add(package, arcname=package.name, filter=unix_info)


def clean_old_archives(dist, keep):
    # Only standard release names are ours; keep custom bundles and deployment data.
    release_name = re.compile(r"cengyan-\d[\w.+-]*-(?:linux|windows)\.(?:zip|tar\.gz)(?:\.sha256)?")
    for file in dist.iterdir():
        if file != keep and file.is_file() and release_name.fullmatch(file.name):
            file.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=("linux",), default="linux",
                        help="Compatibility option; only the Linux tar.gz package is supported")
    parser.parse_args()
    version = ET.parse(ROOT / "pom.xml").getroot().find("{http://maven.apache.org/POM/4.0.0}version").text.strip()
    dist = ROOT / "dist"
    dist.mkdir(exist_ok=True)
    # Only clean our own random staging directory; never remove dist/cengyan-<version>/data.
    with tempfile.TemporaryDirectory(prefix=".release-", dir=dist) as temporary:
        stage = Path(temporary)
        package = stage / f"cengyan-{version}"
        assemble(package, version)
        tar_path = stage / f"cengyan-{version}-linux.tar.gz"
        write_tar(package, tar_path)
        # Publish only after assembly succeeds; never delete a working archive on failure.
        output = dist / tar_path.name
        os.replace(tar_path, output)
        clean_old_archives(dist, output)
        print(output)


if __name__ == "__main__":
    main()
