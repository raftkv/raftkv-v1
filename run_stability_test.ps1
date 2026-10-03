$engine = "<HOME>\Desktop\RaftKV\raftkv_go_engine"
$outFile = "$engine\loadtest_15min.txt"

# Start load tester in background
$proc = Start-Process -FilePath "$engine\load_tester.exe" `
    -ArgumentList "-duration","15m","-concurrency","20" `
    -WorkingDirectory $engine `
    -RedirectStandardOutput $outFile `
    -NoNewWindow -PassThru

Write-Output "LoadTester PID=$($proc.Id) started at $(Get-Date -Format 'HH:mm:ss')"

# 1min mark
Start-Sleep 60
Write-Output ""
Write-Output "=== 1min ==="
docker stats --no-stream --format "{{.Name}}  CPU={{.CPUPerc}}  MEM={{.MemUsage}}  {{.MemPerc}}" raftkv-demo-gateway raftkv-mysql 2>&1

# 5min mark
Start-Sleep 240
Write-Output ""
Write-Output "=== 5min ==="
docker stats --no-stream --format "{{.Name}}  CPU={{.CPUPerc}}  MEM={{.MemUsage}}  {{.MemPerc}}" raftkv-demo-gateway raftkv-mysql 2>&1

# 10min mark
Start-Sleep 300
Write-Output ""
Write-Output "=== 10min ==="
docker stats --no-stream --format "{{.Name}}  CPU={{.CPUPerc}}  MEM={{.MemUsage}}  {{.MemPerc}}" raftkv-demo-gateway raftkv-mysql 2>&1

# 15min mark
Start-Sleep 300
Write-Output ""
Write-Output "=== 15min ==="
docker stats --no-stream --format "{{.Name}}  CPU={{.CPUPerc}}  MEM={{.MemUsage}}  {{.MemPerc}}" raftkv-demo-gateway raftkv-mysql 2>&1

# Wait for load tester to finish
Start-Sleep 15
Write-Output ""
Write-Output "=== MySQL Final ==="
docker exec raftkv-mysql mysql -uroot -praftkv_pwd raftkv_logs -e "SELECT count(*) AS final_count FROM raft_logs;" 2>&1

Write-Output ""
Write-Output "=== LoadTester Last 20 Lines ==="
Get-Content $outFile -Tail 20