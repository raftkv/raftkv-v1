@echo off
chcp 65001 >nul 2>&1
title RaftKV 鲲鹏生态现场演示
color 0A

echo ============================================================
echo          RaftKV 确定性管控中枢系统 - 现场演示
echo ============================================================
echo.

echo 【第1步 / 共3步】查看5节点Raft集群运行状态...
echo ------------------------------------------------------------
docker-compose ps
echo.
echo ============================================================
echo   集群状态展示完毕
echo ============================================================
echo.

echo 【停顿3秒，请向观众讲解集群拓扑...】
timeout /t 3 /nobreak >nul
echo.

echo 【第2步 / 共3步】实时资源监控（运行10秒）...
echo ------------------------------------------------------------
echo   观察要点：镜像仅4.25MB，内存占用不足100MB
echo ------------------------------------------------------------
echo.
start /b docker stats --no-stream
timeout /t 1 /nobreak >nul
taskkill /f /im docker.exe /fi "WINDOWTITLE eq docker*" >nul 2>&1
for /f "tokens=2" %%i in ('tasklist /fi "imagename eq docker.exe" /fo list ^| find "PID"') do (
    taskkill /f /pid %%i >nul 2>&1
)
docker stats --no-stream
echo.
timeout /t 2 /nobreak >nul
docker stats --no-stream
echo.
timeout /t 2 /nobreak >nul
docker stats --no-stream
echo.
timeout /t 2 /nobreak >nul
docker stats --no-stream
echo.
timeout /t 2 /nobreak >nul
docker stats --no-stream
echo.
echo ============================================================
echo   资源监控展示完毕（10秒已到）
echo ============================================================
echo.

echo 【第3步 / 共3步】全量诊断压测（100并发 / full模式）...
echo ------------------------------------------------------------
echo   观察要点：吞吐量14049 req/s，P99延迟7ms，失败率0
echo ------------------------------------------------------------
echo.
.\diag_tool.exe --concurrency 100 --target localhost:9501 --mode=full
echo.

echo ============================================================
echo          演示全部完成！
echo ============================================================
pause