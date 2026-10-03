#!/bin/bash
# _probe_selftest.sh — 探针自测双场景
set -uo pipefail
source "$(dirname "$0")/harness.sh"

echo "=== 探针自测 ==="
echo ""

# ── 场景1: 未就绪 — 无gRPC服务器的容器 ──
echo "--- 场景1: 未就绪 (bare alpine, no gRPC) ---"
docker run -d --name probe-test-notready alpine:3.18 sleep 60 >/dev/null 2>&1
docker cp "${TESTS_DIR}/health-probe-bin" probe-test-notready:/tmp/health-probe 2>/dev/null
docker exec probe-test-notready chmod +x /tmp/health-probe 2>/dev/null
echo "探针执行:"
set +e
docker exec probe-test-notready /tmp/health-probe localhost:9500
rc=$?
set -e
echo "exit code: $rc"
if [ "$rc" -ne 0 ]; then
    echo "结果: PASS (非0, 连接拒绝如预期)"
else
    echo "结果: FAIL (预期非0)"
fi
docker rm -f probe-test-notready >/dev/null 2>&1
echo ""

# ── 场景2: 就绪 — 真实集群节点 ──
echo "--- 场景2: 就绪 (cluster node, gRPC SERVING) ---"
RUN_ID="probe-test-$(date +%s)"
up_cluster "$RUN_ID" || { echo "FAIL: cluster up failed"; exit 1; }
leader=$(get_leader)
container=$(_c_name "$leader")
echo "leader: $leader, container: $container"
docker cp "${TESTS_DIR}/health-probe-bin" "${container}:/tmp/health-probe" 2>/dev/null
docker exec "$container" chmod +x /tmp/health-probe 2>/dev/null
echo "探针执行:"
set +e
docker exec "$container" /tmp/health-probe localhost:9500
rc=$?
set -e
echo "exit code: $rc"
if [ "$rc" -eq 0 ]; then
    echo "结果: PASS (0, SERVING如预期)"
else
    echo "结果: FAIL (预期0)"
fi
down_cluster "$RUN_ID"
echo ""
echo "=== 自测完成 ==="