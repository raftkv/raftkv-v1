#!/bin/bash
# suite_baseline.sh — 基线套件 t01-t06
# 覆盖: fail-closed(SM4), 重启回放, applied==commit(Bug G),
#       follower恢复(Bug D), 默认配置, 非法env拒绝(F3)
set -euo pipefail
source "$(dirname "$0")/harness.sh"

suite_begin "baseline"

RID="base-$(date +%s)"

# ════════════════════════════════════════════════════════════
# t01: fail_closed — SM4_KEY未设/非法 → 容器必须exit非0
# ════════════════════════════════════════════════════════════
t_begin "t01" "fail_closed: SM4_KEY missing/short → container exit non-zero"

# t01a: 无SM4_KEY
docker run -d --name "t01a-${RID}" --network none \
    -e RAFTKV_FP_ANCHOR="${FP_ANCHOR}" \
    -e NODE_ID=node-1 -e GRPC_PORT=9500 -e HTTP_PORT=9000 -e HTTP_BIND=0.0.0.0 \
    -v "${LICENSE_DIR}/node-1.key:/app/license.key:ro" \
    "$IMAGE_NAME" 2>&1 || true
wait_for_exit "t01a-${RID}" 15
t01a_state=$(docker inspect -f '{{.State.Status}}' "t01a-${RID}" 2>/dev/null || echo "missing")
t01a_logs=$(docker logs "t01a-${RID}" 2>&1 || true)
docker rm -f "t01a-${RID}" 2>/dev/null || true
echo "$t01a_logs" | log_evidence "t01a_no_key.log"
assert_ne "$t01a_state" "running" "t01a: no SM4_KEY → not running"

# t01b: SM4_KEY过短(5字符)
docker run -d --name "t01b-${RID}" --network none \
    -e RAFTKV_FP_ANCHOR="${FP_ANCHOR}" \
    -e SM4_KEY=abcde \
    -e NODE_ID=node-1 -e GRPC_PORT=9500 -e HTTP_PORT=9000 -e HTTP_BIND=0.0.0.0 \
    -v "${LICENSE_DIR}/node-1.key:/app/license.key:ro" \
    "$IMAGE_NAME" 2>&1 || true
wait_for_exit "t01b-${RID}" 15
t01b_state=$(docker inspect -f '{{.State.Status}}' "t01b-${RID}" 2>/dev/null || echo "missing")
t01b_logs=$(docker logs "t01b-${RID}" 2>&1 || true)
docker rm -f "t01b-${RID}" 2>/dev/null || true
echo "$t01b_logs" | log_evidence "t01b_short_key.log"
assert_ne "$t01b_state" "running" "t01b: short SM4_KEY → not running"

# ════════════════════════════════════════════════════════════
# t02: restart_replay — 写100条→重启→恢复100条
# ════════════════════════════════════════════════════════════
t_begin "t02" "restart_replay: write 100 → restart → recover 100"

up_cluster "${RID}-t02" || { echo "[t02] FAIL: cluster up failed"; suite_end; exit 1; }
leader=$(get_leader)
write_entries "$leader" 100 "t02"
sleep 2

s_before=$(stats "$leader")
commit_before=$(extract_stat "$s_before" commit)
assert_eq "$commit_before" "101" "t02: commit before restart (100 data + 1 no-op)"

restart_node 1; restart_node 2
sleep 10

s_after=$(stats "$leader")
commit_after=$(extract_stat "$s_after" commit)
logs_after=$(extract_stat "$s_after" logs)
applied_after=$(extract_stat "$s_after" applied)
assert_eq "$commit_after" "102" "t02: commit after restart (100 data + 2 no-op)"
assert_eq "$logs_after" "102" "t02: logs after restart"
assert_eq "$applied_after" "102" "t02: applied after restart"

docker logs "$(_c_name "$leader")" 2>&1 | grep "WAL回放" | log_evidence "t02_replay.log"
down_cluster "${RID}-t02"

# ════════════════════════════════════════════════════════════
# t03: applied_eq_commit — 写50条后 commit==applied (Bug G回归)
# ════════════════════════════════════════════════════════════
t_begin "t03" "applied_eq_commit: write 50 → commit==applied (Bug G)"

up_cluster "${RID}-t03" || { echo "[t03] FAIL: cluster up failed"; suite_end; exit 1; }
leader=$(get_leader)
write_entries "$leader" 50 "t03"
sleep 2

s1=$(stats 1); s2=$(stats 2)
c1=$(extract_stat "$s1" commit); a1=$(extract_stat "$s1" applied)
c2=$(extract_stat "$s2" commit); a2=$(extract_stat "$s2" applied)
assert_eq "$a1" "$c1" "t03: node1 applied==commit"
assert_eq "$a2" "$c2" "t03: node2 applied==commit"
echo "node1: commit=$c1 applied=$a1 | node2: commit=$c2 applied=$a2" | log_evidence "t03_stats.txt"
down_cluster "${RID}-t03"

# ════════════════════════════════════════════════════════════
# t04: follower_recovery — 停Follower 10s→重启→恢复,commit不回退
# ════════════════════════════════════════════════════════════
t_begin "t04" "follower_recovery: stop follower 10s → restart → recover (Bug D)"

up_cluster "${RID}-t04" || { echo "[t04] FAIL: cluster up failed"; suite_end; exit 1; }
leader=$(get_leader); follower=$(get_follower)
write_entries "$leader" 30 "t04"
sleep 2

s_mid=$(stats "$leader")
commit_mid=$(extract_stat "$s_mid" commit)
echo "commit before follower stop: $commit_mid"

stop_node "$follower"
sleep 10
start_node "$follower"
wait_for 20 grpc_serving "$(_c_name "$follower")"

s_end=$(stats "$leader")
commit_end=$(extract_stat "$s_end" commit)
applied_end=$(extract_stat "$s_end" applied)
assert_ne "$commit_end" "0" "t04: commit not regressed to 0"
assert_eq "$applied_end" "$commit_end" "t04: applied==commit after recovery"
echo "commit: before=$commit_mid after=$commit_end applied=$applied_end" | log_evidence "t04_recovery.txt"
down_cluster "${RID}-t04"

# ════════════════════════════════════════════════════════════
# t05: default_config — 不设任何额外env→写50+回放正常
# ════════════════════════════════════════════════════════════
t_begin "t05" "default_config: no extra env → write 50 + replay"

up_cluster "${RID}-t05" || { echo "[t05] FAIL: cluster up failed"; suite_end; exit 1; }
leader=$(get_leader)
write_entries "$leader" 50 "t05"
sleep 2

s=$(stats "$leader")
commit=$(extract_stat "$s" commit)
assert_eq "$commit" "51" "t05: commit=51 (50 data + 1 no-op)"

restart_node 1; restart_node 2
sleep 10
s2=$(stats "$leader")
commit2=$(extract_stat "$s2" commit)
assert_eq "$commit2" "52" "t05: commit after restart (50 data + 2 no-op)"
docker logs "$(_c_name "$leader")" 2>&1 | grep "WAL回放" | log_evidence "t05_replay.log"
down_cluster "${RID}-t05"

# ════════════════════════════════════════════════════════════
# t06: illegal_env — WAL_FLUSH_INTERVAL_MS=abc/0 → 拒绝启动
# ════════════════════════════════════════════════════════════
t_begin "t06" "illegal_env: WAL_FLUSH_INTERVAL_MS=abc/0 → reject (F3 fail-closed)"

key=$(openssl rand -hex 16)

# t06a: abc
docker run -d --name "t06a-${RID}" --network none \
    -e RAFTKV_FP_ANCHOR="${FP_ANCHOR}" -e SM4_KEY="$key" \
    -e NODE_ID=node-1 -e GRPC_PORT=9500 -e HTTP_PORT=9000 -e HTTP_BIND=0.0.0.0 \
    -e WAL_FLUSH_INTERVAL_MS=abc \
    -v "${LICENSE_DIR}/node-1.key:/app/license.key:ro" \
    "$IMAGE_NAME" 2>&1 || true
wait_for_exit "t06a-${RID}" 15
t06a_state=$(docker inspect -f '{{.State.Status}}' "t06a-${RID}" 2>/dev/null || echo "missing")
docker rm -f "t06a-${RID}" 2>/dev/null || true
assert_ne "$t06a_state" "running" "t06a: WAL_FLUSH_INTERVAL_MS=abc → not running"

# t06b: 0
docker run -d --name "t06b-${RID}" --network none \
    -e RAFTKV_FP_ANCHOR="${FP_ANCHOR}" -e SM4_KEY="$key" \
    -e NODE_ID=node-1 -e GRPC_PORT=9500 -e HTTP_PORT=9000 -e HTTP_BIND=0.0.0.0 \
    -e WAL_FLUSH_INTERVAL_MS=0 \
    -v "${LICENSE_DIR}/node-1.key:/app/license.key:ro" \
    "$IMAGE_NAME" 2>&1 || true
wait_for_exit "t06b-${RID}" 15
t06b_state=$(docker inspect -f '{{.State.Status}}' "t06b-${RID}" 2>/dev/null || echo "missing")
docker rm -f "t06b-${RID}" 2>/dev/null || true
assert_ne "$t06b_state" "running" "t06b: WAL_FLUSH_INTERVAL_MS=0 → not running"

suite_end