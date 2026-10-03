$engine = "<HOME>\Desktop\RaftKV\raftkv_go_engine"
$outFile = "$engine\dist_loadtest_output.txt"

# Get baseline
$baseline = docker exec raftkv-mysql mysql -uroot -praftkv_pwd raftkv_logs -e "SELECT count(*) FROM raft_logs;" 2>&1
Write-Output "MySQL 基线: $baseline"

# Start load test in background
$proc = Start-Process -FilePath "$engine\cluster_loadtest.exe" `
    -ArgumentList "-duration","60s","-concurrency","30" `
    -WorkingDirectory $engine `
    -RedirectStandardOutput $outFile `
    -NoNewWindow -PassThru

Write-Output "压测启动 PID=$($proc.Id) at $(Get-Date -Format 'HH:mm:ss')"

# Wait 20(20 seconds then kill Leader
Start-Sleep 20
Write-Output ""
Write-Output "=== 20s: 故障注入 docker stop raftkv-gw-5 ==="
docker stop raftkv-gw-5 2>&1
Write-Output "Leader node-5 已停止 at $(Get-Date -Format 'HH:mm:ss')"

# Wait for load test to finish (60s total + 10s buffer)
Write-Output "等待压测完成..."
$proc.WaitForExit(70000)

# Show load test output
Write-Output ""
Write-Output "=== 压测终端输出 ==="
Get-Content $outFile

# Final MySQL count
Write-Output ""
Write-Output "=== MySQL 最终行数 ==="
docker exec raftkv-mysql mysql -uroot -praftkv_pwd raftkv_logs -e "SELECT count(*) AS final_rows FROM raft_logs;" 2>&1

# Show remaining cluster status
Write-Output ""
Write-Output "=== 集群状态 ==="
foreach ($p in 9001,9002,9003,9104,9105) {
    try {
        $r = Invoke-RestMethod -Uri "http://localhost:$p/raft/status" -TimeoutSec 3
        Write-Output "端口 $p -> state=$($r.state) term=$($r.term) leader=$($r.leader_id) commit=$($r.commit_index) logs=$($r.log_count)"
    } catch {
        Write-Output "端口 $p -> 已停止 (故障注入)"
    }
}