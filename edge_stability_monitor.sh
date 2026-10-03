#!/bin/sh
# RaftKV 边缘恶劣环境4小时长稳监测脚本
# 每30分钟采集一次CPU/内存/延迟数据
# 共8个采集点(0h, 0.5h, 1h, 1.5h, 2h, 2.5h, 3h, 3.5h, 4h)

LOGFILE="/tmp/edge_stability_4h.log"
INTERVAL=1800  # 30分钟
TOTAL_POINTS=9  # 0h到4h共9个点

echo "============================================" > $LOGFILE
echo "RaftKV 边缘恶劣环境4小时长稳测试" >> $LOGFILE
echo "开始时间: $(date '+%Y-%m-%d %H:%M:%S')" >> $LOGFILE
echo "配置: 0.5核/512MB/10%丢包/100ms抖动" >> $LOGFILE
echo "============================================" >> $LOGFILE

for i in $(seq 1 $TOTAL_POINTS); do
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    ELAPSED=$(( (i - 1) * 30 ))
    echo "" >> $LOGFILE
    echo "========== 采集点 $i / $TOTAL_POINTS (t=${ELAPSED}min) $TIMESTAMP ==========" >> $LOGFILE
    
    # 采集每个节点的资源使用和Raft状态
    for node_id in 1 2 3 4 5; do
        NODE="raftkv_go_engine-node-${node_id}-1"
        
        # CPU和内存 (从docker stats)
        STATS=$(docker stats --no-stream --format "{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}" $NODE 2>/dev/null)
        
        # Raft状态
        RAFT_STATUS=$(docker exec $NODE wget -qO- http://localhost:9000/raft/status 2>/dev/null)
        
        # 健康检查
        HEALTH=$(docker exec $NODE wget -qO- http://localhost:9000/health/live 2>/dev/null)
        
        echo "  node-$node_id: CPU/MEM=$STATS RAFT=$RAFT_STATUS HEALTH=$HEALTH" >> $LOGFILE
    done
    
    # 检查OOM事件
    OOM=$(docker events --since 30m --filter event=oom --filter container=raftkv_go_engine-node-1-1 --filter container=raftkv_go_engine-node-2-1 --filter container=raftkv_go_engine-node-3-1 --filter container=raftkv_go_engine-node-4-1 --filter container=raftkv_go_engine-node-5-1 --until 0s 2>/dev/null | head -1)
    if [ -n "$OOM" ]; then
        echo "  [WARNING] 检测到OOM事件: $OOM" >> $LOGFILE
    else
        echo "  [OK] 无OOM事件" >> $LOGFILE
    fi
    
    echo "  采集完成，等待下一周期..." >> $LOGFILE
    
    if [ $i -lt $TOTAL_POINTS ]; then
        sleep $INTERVAL
    fi
done

echo "" >> $LOGFILE
echo "============================================" >> $LOGFILE
echo "4小时长稳测试完成" >> $LOGFILE
echo "结束时间: $(date '+%Y-%m-%d %H:%M:%S')" >> $LOGFILE
echo "============================================" >> $LOGFILE