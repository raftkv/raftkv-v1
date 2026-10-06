#!/bin/bash
# deploy_up.sh — 一键拉起RaftKV集群
# 修正A: SM4_KEY双模式（留空=生成复用, 显式=用指定值）
# D3-sec-rotate: 默认改为5节点(docker-compose-5node.yml)
# --legacy: 使用2节点裁剪版(docker-compose.yml), 历史形态，仅作对照
set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${DEPLOY_DIR}/deploy.env"
KEY_FILE="${DEPLOY_DIR}/.sm4_key"

# --- 拓扑选择 ---
LEGACY=false
if [ "${1:-}" = "--legacy" ]; then
    LEGACY=true
    COMPOSE_FILE="${DEPLOY_DIR}/docker-compose.yml"
    PROJECT_NAME="deploy"
    NODE_COUNT=2
    echo "[deploy] --legacy: 使用2节点裁剪版(历史形态，仅作对照)"
else
    COMPOSE_FILE="${DEPLOY_DIR}/docker-compose-5node.yml"
    PROJECT_NAME="deploy5"
    NODE_COUNT=5
    echo "[deploy] 默认: 使用5节点(docker-compose-5node.yml)"
fi

if [ ! -f "$ENV_FILE" ]; then
    echo "[deploy] FAIL: deploy.env 不存在，请从 deploy.env.example 复制并填入真实值"
    exit 1
fi

source "$ENV_FILE"

IMAGE_NAME="${IMAGE_NAME:-raftkit-gateway:v1}"
FP_ANCHOR="${FP_ANCHOR:-raftkv-v25-test}"
GRPC_PORT="${GRPC_PORT:-9500}"
HTTP_PORT="${HTTP_PORT:-9000}"

if [ -z "${LICENSE_DIR:-}" ] || [ "${LICENSE_DIR}" = "__SET_YOUR_LICENSE_DIR__" ]; then
    echo "[deploy] FAIL: LICENSE_DIR 未设置，请编辑 deploy.env"
    exit 1
fi

if [ -z "${SM4_KEY:-}" ]; then
    if [ -f "$KEY_FILE" ]; then
        SM4_KEY=$(cat "$KEY_FILE")
        echo "[deploy] SM4_KEY: 从本地密钥文件复用"
    else
        SM4_KEY=$(openssl rand -hex 16)
        echo "$SM4_KEY" > "$KEY_FILE"
        chmod 600 "$KEY_FILE" 2>/dev/null || true
        echo "[deploy] SM4_KEY: 首次生成并写入本地密钥文件"
    fi
else
    echo "[deploy] SM4_KEY: 使用deploy.env指定值"
fi

export IMAGE_NAME FP_ANCHOR GRPC_PORT HTTP_PORT SM4_KEY LICENSE_DIR

echo "[deploy] 配置:"
echo "  IMAGE_NAME=$IMAGE_NAME"
echo "  FP_ANCHOR=$FP_ANCHOR"
echo "  GRPC_PORT=$GRPC_PORT"
echo "  HTTP_PORT=$HTTP_PORT"
echo "  LICENSE_DIR=$LICENSE_DIR"
echo "  SM4_KEY=${SM4_KEY:0:8}...(隐藏)"
echo "  COMPOSE_FILE=$(basename "$COMPOSE_FILE")"
echo "  NODE_COUNT=$NODE_COUNT"

echo ""
echo "[deploy] docker compose up -d..."
cd "$DEPLOY_DIR"
docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d 2>&1

echo ""
echo "[deploy] 等待Leader出现..."
for i in $(seq 1 30); do
    sleep 1
    for n in $(seq 1 $NODE_COUNT); do
        s=$(docker exec raft-node-$n curl -s http://127.0.0.1:9000/raft/stats 2>/dev/null || true)
        if echo "$s" | grep -q "state=Leader"; then
            echo "[deploy] Leader=raft-node-$n after ${i}s"
            echo "[deploy] stats: $s"
            echo "[deploy] PASS: 集群已就绪"
            exit 0
        fi
    done
done

echo "[deploy] FAIL: Leader未在30s内当选"
exit 1
