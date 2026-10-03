#!/bin/bash
# ============================================================
# manage_node.sh — RaftKV 集群节点一键扩缩容管理脚本
#
# 功能:
#   add <node_id> <grpc_port> <http_port> <host>
#     将新节点追加到所有现有节点的 PEERS 列表
#     生成新节点的启动命令
#     提示用户执行滚动重启完成加入
#
#   remove <node_id>
#     从所有现有节点的 PEERS 列表移除指定节点
#     提示用户执行滚动重启完成剔除
#
#   list
#     列出当前集群所有节点
#
# 用法:
#   bash manage_node.sh add node-6 9606 9106 localhost
#   bash manage_node.sh remove node-6
#   bash manage_node.sh list
#
# 注意:
#   当前版本为静态 peers 设计, 新增/移除节点后需执行一次
#   集群滚动重启 (预计15秒内自动恢复), 无需停机操作。
# ============================================================

set -euo pipefail

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# 默认配置
BASE_DIR="${RAFTKV_BASE_DIR:-/root/raftkv_cluster}"
CONFIG_FILE="${RAFTKV_CONFIG:-config.toml}"
DAEMON_SCRIPT="daemon.sh"

# 节点端口映射表 (与 daemon.sh 保持一致)
declare -A NODE_GRPC_PORT
declare -A NODE_HTTP_PORT
NODE_GRPC_PORT[node-1]=9500;  NODE_HTTP_PORT[node-1]=9001
NODE_GRPC_PORT[node-2]=9501;  NODE_HTTP_PORT[node-2]=9002
NODE_GRPC_PORT[node-3]=9502;  NODE_HTTP_PORT[node-3]=9003
NODE_GRPC_PORT[node-4]=9604;  NODE_HTTP_PORT[node-4]=9104
NODE_GRPC_PORT[node-5]=9605;  NODE_HTTP_PORT[node-5]=9105

# 已注册的节点列表
KNOWN_NODES=("node-1" "node-2" "node-3" "node-4" "node-5")

usage() {
    echo -e "${CYAN}RaftKV 集群节点管理工具${NC}"
    echo ""
    echo -e "用法:"
    echo -e "  ${GREEN}bash manage_node.sh list${NC}                          列出当前集群节点"
    echo -e "  ${GREEN}bash manage_node.sh add <id> <grpc> <http> <host>${NC}  添加新节点"
    echo -e "  ${GREEN}bash manage_node.sh remove <id>${NC}                    移除节点"
    echo ""
    echo -e "示例:"
    echo -e "  bash manage_node.sh add node-6 9606 9106 localhost"
    echo -e "  bash manage_node.sh remove node-6"
    echo ""
    echo -e "${YELLOW}注意: 当前版本为静态 peers 设计, 操作后需滚动重启集群${NC}"
    echo -e "${YELLOW}      (预计15秒内自动恢复, 无需停机)${NC}"
}

# 获取所有已注册节点
get_all_nodes() {
    echo "${KNOWN_NODES[@]}"
}

# 生成某节点的 PEERS 字符串 (排除自身)
generate_peers() {
    local self=$1
    shift
    local nodes=("$@")
    local peers=""
    for node in "${nodes[@]}"; do
        if [ "$node" != "$self" ]; then
            local port=${NODE_GRPC_PORT[$node]}
            if [ -n "$peers" ]; then
                peers="$peers,"
            fi
            peers="$peers$node=localhost:$port"
        fi
    done
    echo "$peers"
}

# --- list 命令 ---
cmd_list() {
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}  当前集群节点列表${NC}"
    echo -e "${CYAN}========================================${NC}"
    echo ""
    printf "%-10s %-10s %-10s\n" "节点ID" "gRPC端口" "HTTP端口"
    printf "%-10s %-10s %-10s\n" "------" "--------" "--------"
    for node in "${KNOWN_NODES[@]}"; do
        printf "%-10s %-10s %-10s\n" "$node" "${NODE_GRPC_PORT[$node]}" "${NODE_HTTP_PORT[$node]}"
    done
    echo ""
    echo -e "共 ${#KNOWN_NODES[@]} 个节点"
}

# --- add 命令 ---
cmd_add() {
    local new_id=$1
    local new_grpc=$2
    local new_http=$3
    local new_host=${4:-localhost}

    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}  添加节点: $new_id${NC}"
    echo -e "${CYAN}========================================${NC}"
    echo ""

    # 检查是否已存在
    for node in "${KNOWN_NODES[@]}"; do
        if [ "$node" == "$new_id" ]; then
            echo -e "${RED}❌ 节点 $new_id 已存在${NC}"
            exit 1
        fi
    done

    # 注册新节点
    NODE_GRPC_PORT[$new_id]=$new_grpc
    NODE_HTTP_PORT[$new_id]=$new_http
    KNOWN_NODES+=("$new_id")

    local total=${#KNOWN_NODES[@]}
    local majority=$(( (total / 2) + 1 ))

    echo -e "${GREEN}✅ 节点 $new_id 已注册${NC}"
    echo -e "  gRPC端口: $new_grpc"
    echo -e "  HTTP端口: $new_http"
    echo -e "  主机地址: $new_host"
    echo ""
    echo -e "集群规模: $total 节点 (majority=$majority)"
    echo ""

    # 生成新节点的 PEERS
    local peers=$(generate_peers "$new_id" "${KNOWN_NODES[@]}")
    echo -e "${YELLOW}[1] 新节点启动命令:${NC}"
    echo ""
    echo "  NODE_ID=$new_id \\"
    echo "  GRPC_PORT=$new_grpc \\"
    echo "  HTTP_PORT=$new_http \\"
    echo "  PEERS=\"$peers\" \\"
    echo "  $BASE_DIR/raftkv_gateway &"
    echo ""

    # 生成需要更新的现有节点 PEERS
    echo -e "${YELLOW}[2] 需更新的现有节点 PEERS (追加 $new_id):${NC}"
    echo ""
    for node in "${KNOWN_NODES[@]}"; do
        if [ "$node" != "$new_id" ]; then
            local node_peers=$(generate_peers "$node" "${KNOWN_NODES[@]}")
            echo "  $node: PEERS=\"$node_peers\""
        fi
    done
    echo ""

    # 滚动重启提示
    echo -e "${YELLOW}[3] 滚动重启步骤 (零停机):${NC}"
    echo ""
    echo "  1. 启动新节点 $new_id"
    echo "  2. 逐个重启现有节点 (每节点间隔10秒):"
    for node in "${KNOWN_NODES[@]}"; do
        if [ "$node" != "$new_id" ]; then
            echo "     docker restart raftkv-gw-${node#node-}"
        fi
    done
    echo "  3. 等待15秒, 验证集群状态"
    echo ""
    echo -e "${GREEN}提示: 滚动重启期间集群始终可用 (Quorum自动维持)${NC}"
    echo -e "${GREEN}      预计15秒内集群自动收敛, 无需停机${NC}"
}

# --- remove 命令 ---
cmd_remove() {
    local target=$1

    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}  移除节点: $target${NC}"
    echo -e "${CYAN}========================================${NC}"
    echo ""

    # 检查是否存在
    local found=0
    local remaining=()
    for node in "${KNOWN_NODES[@]}"; do
        if [ "$node" == "$target" ]; then
            found=1
        else
            remaining+=("$node")
        fi
    done

    if [ $found -eq 0 ]; then
        echo -e "${RED}❌ 节点 $target 不存在${NC}"
        exit 1
    fi

    KNOWN_NODES=("${remaining[@]}")
    local total=${#KNOWN_NODES[@]}
    local majority=$(( (total / 2) + 1 ))

    echo -e "${GREEN}✅ 节点 $target 已标记移除${NC}"
    echo -e "剩余节点: $total 个 (majority=$majority)"
    echo ""

    if [ $total -lt $majority ]; then
        echo -e "${RED}⚠️ 警告: 移除后无法维持 Quorum!${NC}"
        echo -e "${RED}   剩余 $total 节点 < majority $majority${NC}"
        exit 1
    fi

    # 生成更新后的 PEERS
    echo -e "${YELLOW}[1] 停止目标节点:${NC}"
    echo ""
    echo "  docker stop raftkv-gw-${target#node-}"
    echo "  docker rm raftkv-gw-${target#node-}"
    echo ""

    echo -e "${YELLOW}[2] 更新剩余节点 PEERS (移除 $target):${NC}"
    echo ""
    for node in "${KNOWN_NODES[@]}"; do
        local node_peers=$(generate_peers "$node" "${KNOWN_NODES[@]}")
        echo "  $node: PEERS=\"$node_peers\""
    done
    echo ""

    echo -e "${YELLOW}[3] 滚动重启剩余节点 (零停机):${NC}"
    echo ""
    echo "  逐个重启 (每节点间隔10秒):"
    for node in "${KNOWN_NODES[@]}"; do
        echo "    docker restart raftkv-gw-${node#node-}"
    done
    echo "  等待15秒, 验证集群状态"
    echo ""
    echo -e "${GREEN}提示: 移除节点后集群自动维持 Quorum ($total ≥ $majority)${NC}"
    echo -e "${GREEN}      预计15秒内集群自动收敛, 无需停机${NC}"
}

# --- 主入口 ---
case "${1:-}" in
    list)
        cmd_list
        ;;
    add)
        if [ $# -lt 4 ]; then
            echo -e "${RED}用法: bash manage_node.sh add <node_id> <grpc_port> <http_port> [host]${NC}"
            exit 1
        fi
        cmd_add "$2" "$3" "$4" "${5:-localhost}"
        ;;
    remove)
        if [ $# -lt 2 ]; then
            echo -e "${RED}用法: bash manage_node.sh remove <node_id>${NC}"
            exit 1
        fi
        cmd_remove "$2"
        ;;
    *)
        usage
        exit 1
        ;;
esac