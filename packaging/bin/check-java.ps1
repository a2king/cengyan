$ErrorActionPreference = 'Stop'

if ($env:JAVA_HOME) {
    $javaExe = Join-Path $env:JAVA_HOME 'bin\java.exe'
    if (-not (Test-Path -LiteralPath $javaExe)) {
        Write-Error "需要 JDK 21，但 JAVA_HOME 指向的 Java 不存在：$javaExe"
        exit 1
    }
} else {
    $javaCommand = Get-Command java.exe -ErrorAction SilentlyContinue
    if (-not $javaCommand) {
        Write-Error '未找到 Java。请安装 JDK 21，并设置 JAVA_HOME 或将 java 加入 PATH。'
        exit 1
    }
    $javaExe = $javaCommand.Source
}

$previousErrorAction = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$versionOutput = (& $javaExe -version 2>&1 | Out-String)
$ErrorActionPreference = $previousErrorAction
$versionMatch = [regex]::Match($versionOutput, 'version "([^"]+)"')
if (-not $versionMatch.Success) {
    Write-Error "无法识别 Java 版本：$javaExe"
    exit 1
}

$version = $versionMatch.Groups[1].Value
if ($version -match '^1\.(\d+)') {
    $major = [int]$Matches[1]
} else {
    $major = [int](($version -split '[\.-]')[0])
}
if ($major -ne 21) {
    Write-Error "项目必须使用 Java 21，当前检测到 Java $major（$version）。请设置 JAVA_HOME 指向 JDK 21 后重试。"
    exit 1
}

Write-Host "使用 Java $version：$javaExe"
exit 0


