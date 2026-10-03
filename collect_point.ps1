# RaftKV 单次数据采集脚本
# 用法: powershell -File collect_point.ps1 -PointNum <N> -ElapsedMin <M>
param(
    [int]$PointNum = 1,
    [int]$ElapsedMin = 0
)

$logFile = "<HOME>\Desktop\edge_4h_monitor.log"
$nodes = @(
    "raftkv_go_engine-node-1-1",
    "raftkv_go_engine-node-2-1",
    "raftkv_go_engine-node-3-1",
    "raftkv_go_engine-node-4-1",
    "raftkv_go_engine-node-5-1"
)
$nodeNames = @("node-1","node-2","node-3","node-4","node-5")

$now = Get-Date
$ts = $now.ToString('yyyy-MM-dd HH:mm:ss')

"" | Out-File $logFile -Append -Encoding UTF8
"========== 采集点 $PointNum / 9 (t=${ElapsedMin}min) $ts ==========" | Out-File $logFile -Append -Encoding UTF8

$leaderCount = 0
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
        $state = ""; $term = ""; $leader = ""
        if ($raft -match '"state":"([^"]+)"') { $state = $matches[1] }
        if ($raft -match '"term":(\d+)') { $term = $matches[1] }
        if ($raft -match '"leader_id":"([^"]+)"') { $leader = $matches[1] }
        if ($state -eq "Leader") { $leaderCount++ }
        if ($health -ne "OK") { $allHealthy = $false }
        "  ${nn}: CPU=$cpu MEM=$mem ($memPct) State=$state Term=$term Leader=$leader Health=$health" | Out-File $logFile -Append -Encoding UTF8
    } catch {
        "  ${nn}: 采集失败" | Out-File $logFile -Append -Encoding UTF8
        $allHealthy = $false
    }
}

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

Write-Output "采集点 $PointNum 完成: $ts"