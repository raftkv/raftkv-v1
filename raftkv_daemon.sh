#!/bin/bash
# ============================================================
# raftkv_daemon.sh — systemd前台守护脚本
# 启动5个Raft节点+raftkv_relay，进入监控循环
# 任何子进程退出后10秒内自动重启
# ============================================================

BASE_DIR=/root/raftkv_cluster
GW=$BASE_DIR/raftkv_gateway
RELAY=/root/raftkv_relay_linux_arm64
LOG_DIR=$BASE_DIR/logs
PID_DIR=$BASE_DIR/pids
DATA_DIR=$BASE_DIR/data

mkdir -p $LOG_DIR $PID_DIR $DATA_DIR

# 清理旧进程
cleanup_old() {
    pkill -f raftkv_gateway 2>/dev/null
    pkill -f raftkv_relay_linux 2>/dev/null
    rm -f $PID_DIR/*.pid
    sleep 2
}

# 启动单个节点
start_node() {
    local id=$1
    local grpc_port=$2
    local http_port=$3
    local data_dir=$DATA_DIR/$id
    mkdir -p $data_dir

    local peers=""
    case $id in
        node-1) peers="node-2=localhost:9501,node-3=localhost:9502,node-4=localhost:9604,node-5=localhost:9605" ;;
        node-2) peers="node-1=localhost:9500,node-3=localhost:9502,node-4=localhost:9604,node-5=localhost:9605" ;;
        node-3) peers="node-1=localhost:9500,node-2=localhost:9501,node-4=localhost:9604,node-5=localhost:9605" ;;
        node-4) peers="node-1=localhost:9500,node-2=localhost:9501,node-3=localhost:9502,node-5=localhost:9605" ;;
        node-5) peers="node-1=localhost:9500,node-2=localhost:9501,node-3=localhost:9502,node-4=localhost:9604" ;;
    esac

    NODE_ID=$id GRPC_PORT=$grpc_port HTTP_PORT=$http_port PEERS="$peers" \
        $GW >> $LOG_DIR/${id}.log 2>&1 &
    echo $! > $PID_DIR/${id}.pid
    echo "[raftkv] Started $id (PID=$!) gRPC=$grpc_port HTTP=$http_port"
}

# 启动raftkv_relay
start_relay() {
    cd $BASE_DIR
    $RELAY config.toml >> $LOG_DIR/raftkv_relay.log 2>&1 &
    echo $! > $PID_DIR/raftkv_relay.pid
    echo "[raftkv] Started raftkv_relay (PID=$!)"
}

# 检查PID是否存活
is_alive() {
    local pid=$1
    [ -n "$pid" ] && kill -0 $pid 2>/dev/null
}

# 重启单个节点
restart_node() {
    local id=$1
    local grpc_port=$2
    local http_port=$3
    echo "[raftkv] $id crashed, restarting in 3s..."
    sleep 3
    # 杀残留
    local old_pid=$(cat $PID_DIR/${id}.pid 2>/dev/null)
    [ -n "$old_pid" ] && kill $old_pid 2>/dev/null
    sleep 1
    start_node $id $grpc_port $http_port
}

# ======== 主流程 ========

echo "[raftkv] Daemon starting, cleaning old processes..."
cleanup_old

echo "[raftkv] Launching 5-node Raft cluster..."
start_node node-1 9500 9001
start_node node-2 9501 9002
start_node node-3 9502 9003
start_node node-4 9604 9104
start_node node-5 9605 9105

echo "[raftkv] Launching raftkv_relay..."
start_relay

echo "[raftkv] All processes launched, entering watchdog loop..."

# 监控循环：每10秒巡检，挂掉的进程原地重启
while true; do
    sleep 10

    # 检查5个节点
    for entry in "node-1:9500:9001" "node-2:9501:9002" "node-3:9502:9003" "node-4:9604:9104" "node-5:9605:9105"; do
        id=$(echo $entry | cut -d: -f1)
        gp=$(echo $entry | cut -d: -f2)
        hp=$(echo $entry | cut -d: -f3)
        pid=$(cat $PID_DIR/${id}.pid 2>/dev/null)
        if ! is_alive "$pid"; then
            restart_node $id $gp $hp
        fi
    done

    # 检查raftkv_relay
    relay_pid=$(cat $PID_DIR/raftkv_relay.pid 2>/dev/null)
    if ! is_alive "$relay_pid"; then
        echo "[raftkv] raftkv_relay crashed, restarting in 3s..."
        sleep 3
        [ -n "$relay_pid" ] && kill $relay_pid 2>/dev/null
        sleep 1
        start_relay
    fi
done