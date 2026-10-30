@echo off
setlocal
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0stop-studio.ps1"
exit /b %ERRORLEVEL%
