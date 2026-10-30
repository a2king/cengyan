@echo off
setlocal
call "%~dp0stop-studio.bat"
call "%~dp0stop-server.bat"
exit /b 0
