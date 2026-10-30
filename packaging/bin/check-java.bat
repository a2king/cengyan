@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0check-java.ps1"
exit /b %ERRORLEVEL%
