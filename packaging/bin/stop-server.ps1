$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$appHome = Split-Path -Parent $scriptDir
$pidFile = Join-Path $appHome 'pid\server.pid'
if (-not (Test-Path -LiteralPath $pidFile)) { Write-Host '图数据库未运行。'; exit 0 }
$processId = [int](Get-Content -LiteralPath $pidFile -Raw).Trim()
$process = Get-Process -Id $processId -ErrorAction SilentlyContinue
if ($process) { Stop-Process -Id $processId -Force; Write-Host '图数据库已停止。' } else { Write-Host '图数据库进程已退出。' }
Remove-Item -Force -LiteralPath $pidFile


