#!/bin/bash
# suite_health.sh — gRPC健康检查套件 t30-t33
# 覆盖: SERVING判定(t30), NOT_SERVING/恢复(t31),
#       GRPC_PORT fail-closed(t32), HTTP端点不动(t33)
set -euo pipefail
source "$(dirname "$0")/harness.sh"

suite_begin "health"

RID="health-$(date +%s)"

# ── 预编译 health probe 二进制路径 ──
PROBE_BIN="${TESTS_DIR}/health-probe-bin"
if [ ! -f "$PROBE_BIN" ]; then
    echo "[health] FAIL: health-probe-bin 未找到，请先编译"
    ASSERT_FAIL=$((ASSERT_FAIL + 1))
    suite_end
    exit 1
fi
echo "[health] health-probe-bin 就绪: $PROBE_BIN"

# ── 辅助: grpc_health_check <container_name> ──
# 返回: SERVING 或 NOT_SERVING
grpc_health_check() {
    local container="$1"
    docker cp "$PROBE_BIN" "${container}:/tmp/health-probe" 2>/dev/null || true
    docker exec "$container" chmod +x /tmp/health-probe 2>/dev/null || true
    if docker exec "$container" /tmp/health-probe localhost:9500 >/dev/null 2>&1; then
        echo "SERVING"
    else
        echo "NOT_SERVING"
    fi
}

# ════════════════════════════════════════════════════════════
# t30: grpc_serving — 两节点集群，gRPC Check()两节点均SERVING
# ════════════════════════════════════════════════════════════
t_begin "t30" "grpc_serving: both nodes SERVING"

up_cluster "${RID}-t30" || { echo "[t30] FAIL: cluster up failed"; suite_end; exit 1; }

t30_1=$(grpc_health_check "$C1")
t30_2=$(grpc_health_check "$C2")

assert_eq "$t30_1" "SERVING" "t30: node1 SERVING"
assert_eq "$t30_2" "SERVING" "t30: node2 SERVING"

echo "node1: $t30_1 | node2: $t30_2" | log_evidence "t30_health.txt"
down_cluster "${RID}-t30"

# ════════════════════════════════════════════════════════════
# t31: grpc_not_serving — 停Follower→probe失败→重启→SERVING
# ════════════════════════════════════════════════════════════
t_begin "t31" "grpc_not_serving: stop follower → probe fails → restart → SERVING"

up_cluster "${RID}-t31" || { echo "[t31] FAIL: cluster up failed"; suite_end; exit 1; }
leader=$(get_leader)
follower=$(get_follower)
follower_container=$(_c_name "$follower")

# 初始状态: Follower SERVING
t31_init=$(grpc_health_check "$follower_container")
assert_eq "$t31_init" "SERVING" "t31: follower initially SERVING"

# 停Follower
stop_node "$follower"
sleep 3

# 健康检查应失败（连接拒绝 = NOT_SERVING）
t31_stopped=$(grpc_health_check "$follower_container")
assert_eq "$t31_stopped" "NOT_SERVING" "t31: stopped follower → NOT_SERVING"

# 重启Follower
start_node "$follower"
wait_for 20 grpc_serving "$follower_container"

# 恢复后应SERVING
t31_recovered=$(grpc_health_check "$follower_container")
assert_eq "$t31_recovered" "SERVING" "t31: follower recovered → SERVING"

echo "init=$t31_init stopped=$t31_stopped recovered=$t31_recovered" | log_evidence "t31_health.txt"
down_cluster "${RID}-t31"

# ════════════════════════════════════════════════════════════
# t32: grpc_port_fail_closed — GRPC_PORT=abc/0 → 拒绝启动
# ════════════════════════════════════════════════════════════
t_begin "t32" "grpc_port_fail_closed: GRPC_PORT=abc/0 → reject (F5 fail-closed)"

key=$(openssl rand -hex 16)

# t32a: GRPC_PORT=abc
docker run -d --name "t32a-${RID}" --network none \
    -e RAFTKV_FP_ANCHOR="${FP_ANCHOR}" -e SM4_KEY="$key" \
    -e NODE_ID=node-1 -e GRPC_PORT=abc -e HTTP_PORT=9000 -e HTTP_BIND=0.0.0.0 \
    -v "${LICENSE_DIR}/node-1.key:/app/license.key:ro" \
    "$IMAGE_NAME" 2>&1 || true
wait_for_exit "t32a-${RID}" 15
t32a_state=$(docker inspect -f '{{.State.Status}}' "t32a-${RID}" 2>/dev/null || echo "missing")
t32a_logs=$(docker logs "t32a-${RID}" 2>&1 || true)
docker rm -f "t32a-${RID}" 2>/dev/null || true
echo "$t32a_logs" | log_evidence "t32a_grpc_abc.log"
assert_ne "$t32a_state" "running" "t32a: GRPC_PORT=abc → not running"

# t32b: GRPC_PORT=0
docker run -d --name "t32b-${RID}" --network none \
    -e RAFTKV_FP_ANCHOR="${FP_ANCHOR}" -e SM4_KEY="$key" \
    -e NODE_ID=node-1 -e GRPC_PORT=0 -e HTTP_PORT=9000 -e HTTP_BIND=0.0.0.0 \
    -v "${LICENSE_DIR}/node-1.key:/app/license.key:ro" \
    "$IMAGE_NAME" 2>&1 || true
wait_for_exit "t32b-${RID}" 15
t32b_state=$(docker inspect -f '{{.State.Status}}' "t32b-${RID}" 2>/dev/null || echo "missing")
t32b_logs=$(docker logs "t32b-${RID}" 2>&1 || true)
docker rm -f "t32b-${RID}" 2>/dev/null || true
echo "$t32b_logs" | log_evidence "t32b_grpc_0.log"
assert_ne "$t32b_state" "running" "t32b: GRPC_PORT=0 → not running"

# ════════════════════════════════════════════════════════════
# t33: http_untouched — /raft/status JSON字段集与基线逐字段对比
# ════════════════════════════════════════════════════════════
t_begin "t33" "http_untouched: /raft/status fields match dev6 baseline"

up_cluster "${RID}-t33" || { echo "[t33] FAIL: cluster up failed"; suite_end; exit 1; }
leader=$(get_leader)

# 获取 /raft/status JSON
status_json=$(docker exec "$(_c_name "$leader")" curl -s http://127.0.0.1:9000/raft/status 2>/dev/null)
echo "$status_json" | log_evidence "t33_raft_status.json"

# 提取字段名并排序
actual_fields=$(echo "$status_json" | sed 's/[{}"]//g' | tr ',' '\n' | cut -d: -f1 | tr -d ' ' | sort)

# dev6基线字段集（9个字段）
expected_fields="commit_index
id
last_applied
leader_id
log_count
peer_count
state
term
voted_for"

assert_eq "$actual_fields" "$expected_fields" "t33: /raft/status fields match baseline (9 fields)"

# 同时验证 /raft/stats 端点仍然正常
stats_text=$(docker exec "$(_c_name "$leader")" curl -s http://127.0.0.1:9000/raft/stats 2>/dev/null)
assert_contains "$stats_text" "state=Leader" "t33: /raft/stats still works"
echo "$stats_text" | log_evidence "t33_raft_stats.txt"

down_cluster "${RID}-t33"

suite_end