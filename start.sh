#!/bin/bash
# RaftKV 确定性管控中枢 - 一键启动脚本
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "══════════════════════════════════════════════"
echo "  RaftKV 确定性管控中枢 - 启动"
echo "══════════════════════════════════════════════"

if [ ! -f "./gateway" ]; then
    echo "[错误] 未找到 gateway 二进制文件"
    echo "请先运行: make build 或 make build-release"
    exit 1
fi

if [ ! -f "./license.key" ]; then
    echo "[警告] 未找到 license.key 授权文件"
    echo "程序将启动失败，请先使用 generate_license_tool 生成授权"
    echo ""
    echo "步骤："
    echo "  1. 先运行 ./gateway 查看机器指纹"
    echo "  2. 在授权机上运行: ./generate_license_tool <指纹>"
    echo "  3. 将生成的 license.key 复制到本目录"
    echo ""
    read -p "是否仍要继续启动？(y/N): " confirm
    if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
        exit 0
    fi
fi

chmod +x ./gateway

NODE_ID="${NODE_ID:-node-1}"
GRPC_PORT="${GRPC_PORT:-9500}"
HTTP_PORT="${HTTP_PORT:-9000}"
PEERS="${PEERS:-}"

echo "[启动] 节点: $NODE_ID, gRPC: $GRPC_PORT, HTTP: $HTTP_PORT"
echo ""

exec ./gateway \
    -id "$NODE_ID" \
    -port "$GRPC_PORT" \
    -http "$HTTP_PORT" \
    -peers "$PEERS"