param(
    [ValidateSet('linux')][string]$Target = 'linux',
    [switch]$SkipTests,
    [switch]$SkipWeb
)
$ErrorActionPreference = 'Stop'

function Get-Java21 {
    $javaExe = $null
    if ($env:JAVA_HOME) {
        $javaExe = Join-Path $env:JAVA_HOME 'bin\java.exe'
        if (-not (Test-Path -LiteralPath $javaExe)) {
            throw "需要 JDK 21，但 JAVA_HOME 指向的 Java 不存在：$javaExe"
        }
    } else {
        $javaCommand = Get-Command java.exe -ErrorAction SilentlyContinue
        if (-not $javaCommand) {
            throw '未找到 Java。请安装 JDK 21，并设置 JAVA_HOME 或将 java 加入 PATH。'
        }
        $javaExe = $javaCommand.Source
    }

    $previousErrorAction = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $versionOutput = (& $javaExe -version 2>&1 | Out-String)
    $ErrorActionPreference = $previousErrorAction
    $versionMatch = [regex]::Match($versionOutput, 'version "([^"]+)"')
    if (-not $versionMatch.Success) {
        throw "无法识别 Java 版本。检测命令：$javaExe"
    }

    $version = $versionMatch.Groups[1].Value
    if ($version -match '^1\.(\d+)') {
        $major = [int]$Matches[1]
    } else {
        $major = [int](($version -split '[\.-]')[0])
    }
    if ($major -ne 21) {
        throw "项目必须使用 Java 21，当前检测到 Java $major（$version）。请设置 JAVA_HOME 指向 JDK 21 后重试。"
    }

    if (-not $env:JAVA_HOME) {
        $env:JAVA_HOME = Split-Path (Split-Path $javaExe -Parent) -Parent
    }
    Write-Host "使用 Java $version：$javaExe"
    return $javaExe
}

$repoRoot = $PSScriptRoot
$javaExe = Get-Java21
$maven = Get-Command mvn.cmd -ErrorAction SilentlyContinue
if (-not $maven) {
    throw '未找到 Maven。请安装 Maven，并将 mvn 加入 PATH。'
}

$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) { throw '打包需要 Python 3.11+（仅构建机需要，部署机无需 Python 即可运行 JAR）。' }
& $python.Source -c 'import sys; assert sys.version_info >= (3, 11), "Python 3.11+ required"'
if ($LASTEXITCODE -ne 0) { throw '打包需要 Python 3.11+' }
# Fail before the long build when the offline JDKs that Studio deploys are missing.
foreach ($jdk in 'jdk-21-linux-x64.tar.gz', 'jdk-21-linux-aarch64.tar.gz') {
    if (-not (Test-Path -LiteralPath (Join-Path $repoRoot "packaging/jdk/$jdk"))) {
        throw "缺少离线 JDK：packaging/jdk/$jdk。请下载 Temurin 21 Linux JDK 的 .tar.gz 并按此文件名保存"
    }
}

Push-Location $repoRoot
try {
    if (-not $SkipWeb) {
        $npm = Get-Command npm.cmd -ErrorAction Stop
        Push-Location (Join-Path $repoRoot 'cengyan-studio-web')
        try {
            & $npm.Source ci --no-audit --no-fund
            if ($LASTEXITCODE -ne 0) { throw 'npm ci 失败' }
            & $npm.Source run build
            if ($LASTEXITCODE -ne 0) { throw 'Studio Web 构建失败' }
        } finally { Pop-Location }
    }
    # Never run two Maven reactors against the same target directories concurrently.
    $arguments = @('clean','package')
    if ($SkipTests) { $arguments += '-DskipTests'; Write-Warning '已显式跳过 Java 测试。' }
    & $maven.Source @arguments
    if ($LASTEXITCODE -ne 0) { throw "Maven 构建失败，退出码：$LASTEXITCODE" }
    & $python.Source (Join-Path $repoRoot 'packaging/assemble-dist.py') --target $Target
    if ($LASTEXITCODE -ne 0) { throw '归档或产物校验失败；未发布新的完整部署包。' }
} finally {
    Pop-Location
}
Write-Host "Linux tar.gz 打包完成：$(Join-Path $repoRoot 'dist')；bin 内文件权限为 777，原有解压目录和数据未改动。"

