@echo off
setlocal
call "%~dp0start-server.bat"
if errorlevel 1 exit /b 1
timeout /t 2 /nobreak >nul
call "%~dp0start-studio.bat"
if errorlevel 1 exit /b 1
echo 图数据库和 Web 控制台已后台启动。
exit /b 0
