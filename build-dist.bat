@echo off
setlocal
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0build-dist.ps1" %*
exit /b %ERRORLEVEL%
