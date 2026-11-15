param(
    [Parameter(Mandatory=$true)][ValidateSet('storage','compute')][string]$Role,
    [string]$Config
)
$ErrorActionPreference='Stop'
$root=Split-Path $PSScriptRoot -Parent
if (-not $Config) { $Config=Join-Path $root "config\$Role.properties" }
if (-not $env:JAVA_HOME) { throw '请配置 JAVA_HOME 指向 JDK 21' }
$java=Join-Path $env:JAVA_HOME 'bin\java.exe'
if (-not (Test-Path -LiteralPath $java)) { throw "找不到 Java：$java" }
$logConfig=Join-Path $root 'config\logback.xml'
if (-not (Test-Path -LiteralPath $logConfig -PathType Leaf)) { throw "找不到日志配置：$logConfig" }
& $java "-Dlogback.configurationFile=$logConfig" -jar (Join-Path $root "cengyan-$Role.jar") $Config
exit $LASTEXITCODE
