$engine = "<HOME>\Desktop\RaftKV\raftkv_go_engine"
$out = "$engine\dist_load_out.txt"

# Baseline
$bl = docker exec raftkv-mysql mysql -uroot -praftkv_pwd raftkv_logs -e "SELECT count(*) FROM raft_logs;" 2>&1
$baseline = ($bl | Select-String -Pattern "\d+").Matches[0].Value
Write-Output "MySQL基线: $baseline"

# Start load test
$proc = Start-Process -FilePath "$engine\cluster_loadtest.exe" `
    -ArgumentList "-duration","60s","-concurrency","30" `
    -WorkingDirectory $engine `
    -RedirectStandardOutput $out `
    -NoNewWindow -PassThru
Write-Output "压测启动 PID=$($proc.Id) at $(Get-Date -Format 'HH:mm:ss')"

# Wait 20s then kill Leader
Start-Sleep 20
Write-Output ""
Write-Output "=== 20s: docker stop raftkv-gw-5 ==="
docker stop raftkv-gw-5 2>&1 | Out-Null
Write-Output "Leader node-5 已停止 at $(Get-Date -Format 'HH:mm:ss')"

# Wait for load test to finish
$proc.WaitForExit(70000)
Write-Output "压测结束 at $(Get-Date -Format 'HH:mm:ss')"

# Output
Write-Output ""
Write-Output "=== 压测终端输出 ==="
Get-Content $out

# Final MySQL
Write-Output ""
Write-Output "=== MySQL最终 ==="
docker exec raftkv-mysql mysql -uroot -praftkv_pwd raftkv_logs -e "SELECT count(*) AS final_rows FROM raft_logs;" 2>&1

# Cluster status
Write-Output ""
Write-Output "=== 集群状态 ==="
foreach ($p in 9001,9002,9003,9104,9105) {
    try {
        $r = Invoke-RestMethod -Uri "http://localhost:$p/raft/status" -TimeoutSec 3
        Write-Output "端口 $p -> state=$($r.state) term=$($r.term) leader=$($r.leader_id) commit=$($r.commit_index)"
    } catch {
        Write-Output "端口 $p -> 已停止(故障注入)"
    }
}