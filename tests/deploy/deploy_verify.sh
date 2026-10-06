#!/bin/bash
# deploy_verify.sh — 一键验收（起集群→探针全绿→写入读回→干净关闭）
# 修正A: 一次性验证场景，随机SM4_KEY + down -v
set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${DEPLOY_DIR}/deploy.env"
COMPOSE_FILE="${DEPLOY_DIR}/docker-compose.yml"
PROBE_BIN="${DEPLOY_DIR}/../health-probe-bin"
EVIDENCE_BASE="${DEPLOY_DIR}/../evidence/deploy-verify"
RUN_ID="verify-$(date +%Y%m%d_%H%M%S)"
RUN_EVIDENCE="${EVIDENCE_BASE}/${RUN_ID}"

mkdir -p "$RUN_EVIDENCE"

LOG="${RUN_EVIDENCE}/verify.log"
echo "" > "$LOG"

log() { echo "$1" | tee -a "$LOG"; }

PASS=0
FAIL=0

assert_eq() {
    if [ "$1" = "$2" ]; then
        log "  [PASS] $3: $1 == $2"
        PASS=$((PASS + 1))
    else
        log "  [FAIL] $3: actual=$1 expected=$2"
        FAIL=$((FAIL + 1))
    fi
}

assert_ne() {
    if [ "$1" != "$2" ]; then
        log "  [PASS] $3: $1 != $2"
        PASS=$((PASS + 1))
    else
        log "  [FAIL] $3: actual=$1 should != $2"
        FAIL=$((FAIL + 1))
    fi
}

extract_stat() {
    echo "$1" | tr ' ' '\n' | grep "^${2}=" | cut -d= -f2
}

json_field() {
    echo "$1" | sed -n 's/.*"'"$2"'":\([^,}]*\).*/\1/p' | tr -d '"' | tr -d ' '
}

if [ ! -f "$ENV_FILE" ]; then
    log "[verify] FAIL: deploy.env 不存在"
    exit 1
fi
source "$ENV_FILE"

IMAGE_NAME="${IMAGE_NAME:-raftkit-gateway:v1}"
FP_ANCHOR="${FP_ANCHOR:-raftkv-v25-test}"
GRPC_PORT="${GRPC_PORT:-9500}"
HTTP_PORT="${HTTP_PORT:-9000}"

SM4_KEY=$(openssl rand -hex 16)
export IMAGE_NAME FP_ANCHOR GRPC_PORT HTTP_PORT SM4_KEY LICENSE_DIR

log "=== deploy_verify 开始 ==="
log "RUN_ID: $RUN_ID"
log "IMAGE_NAME: $IMAGE_NAME"
log "SM4_KEY: ${SM4_KEY:0:8}...(一次性随机)"

log ""
log "--- 1. 拉起集群 ---"
cd "$DEPLOY_DIR"
docker compose up -d 2>&1 | tee -a "$LOG"

leader=""
for i in $(seq 1 30); do
    sleep 1
    s1=$(docker exec raft-node-1 curl -s http://127.0.0.1:9000/raft/stats 2>/dev/null || true)
    if echo "$s1" | grep -q "state=Leader"; then
        leader="raft-node-1"
        log "[verify] Leader=$leader after ${i}s"
        log "[verify] stats: $s1"
        break
    fi
    s2=$(docker exec raft-node-2 curl -s http://127.0.0.1:9000/raft/stats 2>/dev/null || true)
    if echo "$s2" | grep -q "state=Leader"; then
        leader="raft-node-2"
        log "[verify] Leader=$leader after ${i}s"
        log "[verify] stats: $s2"
        break
    fi
done

if [ -z "$leader" ]; then
    log "[verify] FAIL: Leader未在30s内当选"
    docker compose down -v 2>&1 | tee -a "$LOG"
    exit 1
fi

log ""
log "--- 2. gRPC探针 (两节点SERVING) ---"
if [ -f "$PROBE_BIN" ]; then
    for node in raft-node-1 raft-node-2; do
        docker cp "$PROBE_BIN" "${node}:/tmp/health-probe" 2>/dev/null || true
        docker exec "$node" chmod +x /tmp/health-probe 2>/dev/null || true
        if docker exec "$node" /tmp/health-probe localhost:9500 >/dev/null 2>&1; then
            assert_eq "SERVING" "SERVING" "${node} gRPC探针"
        else
            assert_eq "NOT_SERVING" "SERVING" "${node} gRPC探针"
        fi
    done
else
    log "  [SKIP] health-probe-bin不存在，跳过gRPC探针"
fi

log ""
log "--- 3. 写入10条数据 ---"
for i in $(seq 1 10); do
    resp=$(docker exec "$leader" curl -s -X POST http://127.0.0.1:9000/raft/propose -d "verify-entry-$i" 2>/dev/null || true)
    log "  write[$i]: $resp"
done
sleep 2

log ""
log "--- 4. 读回stats ---"
stats=$(docker exec "$leader" curl -s http://127.0.0.1:9000/raft/stats 2>/dev/null || true)
log "  stats: $stats"
commit=$(extract_stat "$stats" commit)
applied=$(extract_stat "$stats" applied)
assert_eq "$applied" "$commit" "applied==commit"
assert_ne "$commit" "0" "commit!=0"

log ""
log "--- 5. 读回entry ---"
entry=$(docker exec "$leader" curl -s "http://127.0.0.1:9000/raft/get?index=2" 2>/dev/null || true)
log "  entry[index=2]: $entry"
found=$(json_field "$entry" found)
assert_eq "$found" "true" "entry[index=2] found=true"

log ""
log "--- 6. Follower同步 ---"
follower="raft-node-2"
if [ "$leader" = "raft-node-2" ]; then follower="raft-node-1"; fi
fstats=$(docker exec "$follower" curl -s http://127.0.0.1:9000/raft/stats 2>/dev/null || true)
log "  follower stats: $fstats"
fcommit=$(extract_stat "$fstats" commit)
assert_eq "$fcommit" "$commit" "follower commit==leader commit"

log ""
log "--- 7. 干净关闭 (down -v) ---"
docker compose down -v 2>&1 | tee -a "$LOG"

log ""
log "--- 8. 无残留 ---"
remain_c=$(docker ps -a --filter "name=raft-node" --format "{{.Names}}" 2>/dev/null || true)
remain_v=$(docker volume ls --filter "name=deploy_wal" --format "{{.Name}}" 2>/dev/null || true)
remain_n=$(docker network ls --filter "name=deploy_raftkv" --format "{{.Name}}" 2>/dev/null || true)
assert_eq "$remain_c" "" "无残留容器"
assert_eq "$remain_v" "" "无残留卷"
assert_eq "$remain_n" "" "无残留网络"

log ""
log "=== 汇总: PASS=$PASS FAIL=$FAIL ==="

if [ "$FAIL" -gt 0 ]; then
    log "[verify] FAIL: $FAIL 个断言失败"
    exit 1
fi

log "[verify] PASS: 全部断言通过"
log "evidence: ${RUN_EVIDENCE}"
exit 0