# RaftKV 4小时长稳监控脚本
# 每30分钟采集一次，共9个采集点(0h ~ 4h)
$logFile = "<HOME>\Desktop\edge_4h_monitor.log"
$nodes = @(
    "raftkv_go_engine-node-1-1",
    "raftkv_go_engine-node-2-1",
    "raftkv_go_engine-node-3-1",
    "raftkv_go_engine-node-4-1",
    "raftkv_go_engine-node-5-1"
)
$nodeNames = @("node-1","node-2","node-3","node-4","node-5")
$totalPoints = 9
$startTime = Get-Date

"============================================" | Out-File $logFile -Encoding UTF8
"RaftKV 边缘恶劣环境4小时长稳测试" | Out-File $logFile -Append -Encoding UTF8
"开始时间: $($startTime.ToString('yyyy-MM-dd HH:mm:ss'))" | Out-File $logFile -Append -Encoding UTF8
"配置: 0.5核/512MB/5%丢包/30ms抖动(适配50ms心跳)" | Out-File $logFile -Append -Encoding UTF8
"采集间隔: 30分钟, 共 $totalPoints 个采集点" | Out-File $logFile -Append -Encoding UTF8
"============================================" | Out-File $logFile -Append -Encoding UTF8

for ($i = 1; $i -le $totalPoints; $i++) {
    $now = Get-Date
    $elapsedMin = ($i - 1) * 30
    
    "" | Out-File $logFile -Append -Encoding UTF8
    "========== 采集点 $i / $totalPoints (t=${elapsedMin}min) $($now.ToString('yyyy-MM-dd HH:mm:ss')) ==========" | Out-File $logFile -Append -Encoding UTF8
    
    $leaderCount = 0
    $totalCpu = 0
    $totalMem = 0
    $allHealthy = $true
    
    for ($j = 0; $j -lt 5; $j++) {
        $n = $nodes[$j]
        $nn = $nodeNames[$j]
        
        try {
            $stats = docker stats --no-stream --format "{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}" $n 2>&1
            $parts = $stats -split '\|'
            $cpu = $parts[0]
            $mem = $parts[1]
            $memPct = $parts[2]
            
            $raft = docker exec $n wget -qO- http://localhost:9000/raft/status 2>&1
            $health = docker exec $n wget -qO- http://localhost:9000/health/live 2>&1
            
            $state = ""
            $term = ""
            $leader = ""
            if ($raft -match '"state":"([^"]+)"') { $state = $matches[1] }
            if ($raft -match '"term":(\d+)') { $term = $matches[1] }
            if ($raft -match '"leader_id":"([^"]+)"') { $leader = $matches[1] }
            
            if ($state -eq "Leader") { $leaderCount++ }
            if ($health -ne "OK") { $allHealthy = $false }
            
            "  ${nn}: CPU=$cpu MEM=$mem ($memPct) State=$state Term=$term Leader=$leader Health=$health" | Out-File $logFile -Append -Encoding UTF8
        } catch {
            "  ${nn}: 采集失败 - $_" | Out-File $logFile -Append -Encoding UTF8
            $allHealthy = $false
        }
    }
    
    # OOM检查
    $oomCount = 0
    foreach ($n in $nodes) {
        $inspect = docker inspect $n --format "{{.State.OOMKilled}}" 2>&1
        if ($inspect -match "true") { $oomCount++ }
    }
    
    if ($oomCount -eq 0) {
        "  [OK] 无OOM事件 (Leader数=$leaderCount, 全部健康=$allHealthy)" | Out-File $logFile -Append -Encoding UTF8
    } else {
        "  [WARNING] 检测到OOM事件! OOM节点数=$oomCount" | Out-File $logFile -Append -Encoding UTF8
    }
    
    "  采集完成,等待下一周期..." | Out-File $logFile -Append -Encoding UTF8
    
    if ($i -lt $totalPoints) {
        Start-Sleep -Seconds 1800
    }
}

$endTime = Get-Date
$duration = ($endTime - $startTime).TotalMinutes
"" | Out-File $logFile -Append -Encoding UTF8
"============================================" | Out-File $logFile -Append -Encoding UTF8
"4小时长稳测试完成" | Out-File $logFile -Append -Encoding UTF8
"结束时间: $($endTime.ToString('yyyy-MM-dd HH:mm:ss'))" | Out-File $logFile -Append -Encoding UTF8
"实际时长: $([math]::Round($duration, 1))分钟" | Out-File $logFile -Append -Encoding UTF8
"============================================" | Out-File $logFile -Append -Encoding UTF8
"DONE" | Out-File $logFile -Append -Encoding UTF8