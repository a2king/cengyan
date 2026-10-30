$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$appHome = Split-Path -Parent $scriptDir
$pidDir = Join-Path $appHome 'pid'
$logDir = Join-Path $appHome 'logs'
$pidFile = Join-Path $pidDir 'server.pid'
$logFile = Join-Path $logDir 'server.log'
$errorFile = Join-Path $logDir 'server-error.log'
$jar = Join-Path $appHome 'cengyan-server.jar'
$config = Join-Path $appHome 'config\cengyan.properties'

if ($env:JAVA_HOME) {
    $javaExe = Join-Path $env:JAVA_HOME 'bin\java.exe'
    if (-not (Test-Path -LiteralPath $javaExe)) { throw "需要 JDK 21，但 JAVA_HOME 指向的 Java 不存在：$javaExe" }
} else {
    $javaCommand = Get-Command java.exe -ErrorAction SilentlyContinue
    if (-not $javaCommand) { throw '未找到 Java。请安装 JDK 21，并设置 JAVA_HOME 或将 java 加入 PATH。' }
    $javaExe = $javaCommand.Source
}
$previousErrorAction = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$versionOutput = (& $javaExe -version 2>&1 | Out-String)
$ErrorActionPreference = $previousErrorAction
$versionMatch = [regex]::Match($versionOutput, 'version "([^"]+)"')
if (-not $versionMatch.Success) { throw "无法识别 Java 版本：$javaExe" }
$version = $versionMatch.Groups[1].Value
$major = if ($version -match '^1\.(\d+)') { [int]$Matches[1] } else { [int](($version -split '[\.-]')[0]) }
if ($major -ne 21) { throw "项目必须使用 Java 21，当前检测到 Java $major（$version）。请设置 JAVA_HOME 指向 JDK 21 后重试。" }

if (-not (Test-Path -LiteralPath $jar)) { throw "未找到 $jar" }
if (-not (Test-Path -LiteralPath $config)) { throw "未找到 $config" }
New-Item -ItemType Directory -Force -Path $pidDir, $logDir | Out-Null

if (Test-Path -LiteralPath $pidFile) {
    $oldProcessId = (Get-Content -LiteralPath $pidFile -Raw).Trim()
    $oldProcess = Get-Process -Id $oldProcessId -ErrorAction SilentlyContinue
    if ($oldProcess) { throw "图数据库已经在运行，PID=$oldProcessId。" }
    Remove-Item -Force -LiteralPath $pidFile
}

$process = Start-Process -FilePath $javaExe `
    -ArgumentList @('-jar', $jar, $config) `
    -WorkingDirectory $appHome `
    -RedirectStandardOutput $logFile `
    -RedirectStandardError $errorFile `
    -WindowStyle Hidden `
    -PassThru
$process.Id | Set-Content -LiteralPath $pidFile -Encoding ascii
Write-Host "图数据库已后台启动，PID=$($process.Id)，日志：$logFile"


