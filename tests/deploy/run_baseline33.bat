@echo off
cd /d ".\tests\deploy"
echo [BASELINE-START] %date% %time% >> ".\tests\evidence\d3-batch33\pre_raft_baseline_30min.txt"
e04_loadtest.exe -duration=30m -concurrency=128 -write-ratio=20 >> ".\tests\evidence\d3-batch33\pre_raft_baseline_30min.txt" 2>&1
echo [BASELINE-END] %date% %time% >> ".\tests\evidence\d3-batch33\pre_raft_baseline_30min.txt"