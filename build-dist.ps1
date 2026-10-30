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

function Get-ProjectVersion([string]$pomPath) {
    $match = [regex]::Match((Get-Content -Raw -LiteralPath $pomPath), '<version>\s*([^<\s]+)\s*</version>')
    if (-not $match.Success) {
        throw "无法从 $pomPath 读取项目版本。"
    }
    return $match.Groups[1].Value
}

$repoRoot = $PSScriptRoot
$javaExe = Get-Java21
$maven = Get-Command mvn.cmd -ErrorAction SilentlyContinue
if (-not $maven) {
    throw '未找到 Maven。请安装 Maven，并将 mvn 加入 PATH。'
}

$version = Get-ProjectVersion (Join-Path $repoRoot 'pom.xml')
$packageName = "cengyan-$version"
$distDir = Join-Path $repoRoot 'dist'
$packageRoot = Join-Path $distDir $packageName
$archivePath = Join-Path $distDir "$packageName-windows.zip"

Write-Host '开始构建 cengyan-server 和 cengyan-studio ...'
Push-Location $repoRoot
try {
    & $maven.Source '-DskipTests' '-pl' 'cengyan-server,cengyan-studio' '-am' 'package'
    if ($LASTEXITCODE -ne 0) {
        throw "Maven 构建失败，退出码：$LASTEXITCODE"
    }
} finally {
    Pop-Location
}

if (Test-Path -LiteralPath $packageRoot) {
    Remove-Item -Recurse -Force -LiteralPath $packageRoot
}
if (Test-Path -LiteralPath $archivePath) {
    Remove-Item -Force -LiteralPath $archivePath
}

$null = New-Item -ItemType Directory -Force -Path `
    (Join-Path $packageRoot 'bin'),
    (Join-Path $packageRoot 'config'),
    (Join-Path $packageRoot 'data'),
    (Join-Path $packageRoot 'logs'),
    (Join-Path $packageRoot 'pid'),
    (Join-Path $packageRoot 'tmp'),
    (Join-Path $packageRoot 'lib')

$serverJar = Join-Path $repoRoot "cengyan-server\target\cengyan-server-$version.jar"
$studioJar = Join-Path $repoRoot "cengyan-studio\target\cengyan-studio-$version.jar"
foreach ($jar in @($serverJar, $studioJar)) {
    if (-not (Test-Path -LiteralPath $jar)) {
        throw "未找到构建产物：$jar"
    }
}
Copy-Item -LiteralPath $serverJar -Destination (Join-Path $packageRoot 'cengyan-server.jar')
Copy-Item -LiteralPath $studioJar -Destination (Join-Path $packageRoot 'cengyan-studio.jar')

Copy-Item -LiteralPath (Join-Path $repoRoot 'cengyan.properties') -Destination (Join-Path $packageRoot 'config\cengyan.properties')
Copy-Item -LiteralPath (Join-Path $repoRoot 'cengyan.properties') -Destination (Join-Path $packageRoot 'config\cengyan.properties.example')
Copy-Item -LiteralPath (Join-Path $repoRoot 'packaging\studio.properties') -Destination (Join-Path $packageRoot 'config\studio.properties')
Copy-Item -LiteralPath (Join-Path $repoRoot 'packaging\studio.properties') -Destination (Join-Path $packageRoot 'config\studio.properties.example')

Copy-Item -Path (Join-Path $repoRoot 'packaging\bin\*') -Destination (Join-Path $packageRoot 'bin') -Recurse
Copy-Item -LiteralPath (Join-Path $repoRoot 'packaging\PACKAGE-README.txt') -Destination (Join-Path $packageRoot 'README.txt')
Copy-Item -LiteralPath (Join-Path $repoRoot 'packaging\lib\README.txt') -Destination (Join-Path $packageRoot 'lib\README.txt')

for ($attempt = 1; $attempt -le 3; $attempt++) {
    try {
        Compress-Archive -Path $packageRoot -DestinationPath $archivePath -CompressionLevel Optimal -ErrorAction Stop
        break
    } catch {
        if ($attempt -eq 3) {
            throw
        }
        Start-Sleep -Seconds 2
    }
}
Write-Host "打包完成：$packageRoot"
Write-Host "压缩包：$archivePath"


