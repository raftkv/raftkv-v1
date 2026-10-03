#!/bin/bash

BASE_DIR=/root/raftkv_cluster
GW=$BASE_DIR/raftkv_gateway
LOG_DIR=$BASE_DIR/logs
PID_DIR=$BASE_DIR/pids
DATA_DIR=$BASE_DIR/data

mkdir -p $LOG_DIR $PID_DIR $DATA_DIR

stop_all() {
    echo 'Stopping old processes...'
    for f in $PID_DIR/*.pid; do
        [ -f "$f" ] && kill $(cat "$f") 2>/dev/null
    done
    pkill -f raftkv_gateway 2>/dev/null
    rm -f $PID_DIR/*.pid
    sleep 2
    echo 'Old processes stopped.'
}

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
        nohup $GW >> $LOG_DIR/${id}.log 2>&1 &
    echo $! > $PID_DIR/${id}.pid
    echo "  Started $id (PID=$!) gRPC=$grpc_port HTTP=$http_port"
}

start_relay() {
    cd $BASE_DIR
}

check_health() {
    echo ''
    echo '=== Health Check ==='
    for port in 9001 9002 9003 9104 9105; do
        local id=""
        case $port in
            9001) id="node-1" ;;
            9002) id="node-2" ;;
            9003) id="node-3" ;;
            9104) id="node-4" ;;
            9105) id="node-5" ;;
        esac
        if curl -sf http://localhost:$port/health/live > /dev/null 2>&1; then
            echo "  $id (HTTP:$port): ALIVE"
        else
            echo "  $id (HTTP:$port): WAITING..."
        fi
    done
}

case "${1:-start}" in
    start)
        stop_all
        echo 'Starting RaftKV 5-node Raft cluster...'
        start_node node-1 9500 9001
        start_node node-2 9501 9002
        start_node node-3 9502 9003
        start_node node-4 9604 9104
        start_node node-5 9605 9105
        start_relay
        echo ''
        echo 'Waiting 10s for cluster initialization...'
        sleep 10
        check_health
        echo ''
        echo '=== All processes ==='
        ;;
    stop)
        stop_all
        echo 'All stopped.'
        ;;
    status)
        echo '=== Running processes ==='
        echo ''
        check_health
        ;;
    *)
        echo "Usage: $0 {start|stop|status}"
        exit 1
        ;;
esac