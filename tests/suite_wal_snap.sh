#!/bin/bash
# suite_wal_snap.sh — WAL快照+no-op套件 t20-t23
# 覆盖: 快照触发, no-op新term, 旧卷兼容(命门), 密钥泄露扫描
set -euo pipefail
source "$(dirname "$0")/harness.sh"

suite_begin "wal_snap"

RID="snap-$(date +%s)"

# ════════════════════════════════════════════════════════════
# t20: snapshot_trigger — THRESHOLD=50, 写60条→快照→restart回放
# ════════════════════════════════════════════════════════════
t_begin "t20" "snapshot_trigger: THRESHOLD=50, write 60 → snapshot → restart replay"

up_cluster "${RID}-t20" "WAL_SNAPSHOT_THRESHOLD=50" || { echo "[t20] FAIL: cluster up"; suite_end; exit 1; }
leader=$(get_leader)
write_entries "$leader" 60 "t20"
sleep 3

s=$(stats "$leader")
commit=$(extract_stat "$s" commit)
assert_eq "$commit" "61" "t20: commit=61 (60 data + 1 no-op)"

# 检查快照文件存在
snap_file=$(docker run --rm -v "$V1:/app/wal-data" alpine:3.21 \
    sh -c 'ls /app/wal-data/*.snapshot.gz 2>/dev/null | head -1' 2>/dev/null || echo "")
assert_ne "$snap_file" "" "t20: snapshot file exists"
if [ -n "$snap_file" ]; then
    snap_size=$(docker run --rm -v "$V1:/app/wal-data" alpine:3.21 \
        sh -c "wc -c < '$snap_file'" 2>/dev/null || echo "0")
    echo "snapshot: $snap_file size=${snap_size}B" | log_evidence "t20_snapshot.txt"
fi

# restart回放
restart_node 1; restart_node 2
sleep 10
s2=$(stats "$leader")
commit2=$(extract_stat "$s2" commit)
logs2=$(extract_stat "$s2" logs)
assert_eq "$commit2" "62" "t20: commit after restart (60 data + 2 no-op)"
assert_eq "$logs2" "62" "t20: logs after restart"
docker logs "$(_c_name "$leader")" 2>&1 | grep "WAL回放" | log_evidence "t20_replay.log"
down_cluster "${RID}-t20"

# ════════════════════════════════════════════════════════════
# t21: noop_term — 触发选举→新term→Command=nil条目→applied==commit
# ════════════════════════════════════════════════════════════
t_begin "t21" "noop_term: trigger election → new term → Command=nil entry"

up_cluster "${RID}-t21" || { echo "[t21] FAIL: cluster up"; suite_end; exit 1; }
leader=$(get_leader); follower=$(get_follower)
write_entries "$leader" 10 "t21"
sleep 2

s_before=$(stats "$leader")
term_before=$(extract_stat "$s_before" term)
commit_before=$(extract_stat "$s_before" commit)

# 重启Leader触发重新选举
restart_node "$leader"
sleep 15

# 新Leader选举成功
new_leader=$(get_leader)
s_after=$(stats "$new_leader")
term_after=$(extract_stat "$s_after" term)
commit_after=$(extract_stat "$s_after" commit)
applied_after=$(extract_stat "$s_after" applied)
logs_after=$(extract_stat "$s_after" logs)

assert_ne "$term_after" "$term_before" "t21: term advanced after election"
assert_eq "$applied_after" "$commit_after" "t21: applied==commit after new term"

# 检查新term的no-op条目(Command=nil)
# no-op在新Leader的logs末尾附近, index=commit_after
noop_entry=$(get_entry "$new_leader" "$commit_after")
noop_cmd=$(json_field "$noop_entry" command)
noop_term=$(json_field "$noop_entry" term)
assert_eq "$noop_cmd" "null" "t21: new term entry has Command=null (no-op)"
assert_eq "$noop_term" "$term_after" "t21: no-op entry term matches current term"
echo "term: $term_before→$term_after, commit=$commit_after, noop=$noop_entry" | log_evidence "t21.txt"
down_cluster "${RID}-t21"

# ════════════════════════════════════════════════════════════
# t22: old_volume_compat (命门) — 旧格式WAL→当前代码回放→恢复20条
# ════════════════════════════════════════════════════════════
t_begin "t22" "old_volume_compat: old-format WAL → current code replay → recover 20"

FIXTURE_DIR="${TESTS_DIR}/fixtures/oldfmt-wal-1"
FIXTURE_DIR_HOSTA="${FIXTURE_DIR_HOST:-${FIXTURE_DIR}}"
FIXTURE_WAL_GZ="${FIXTURE_DIR}/raftkv.wal.gz"
FIXTURE_KEY_FILE="${FIXTURE_DIR}/sm4_key.txt"

if [ ! -f "$FIXTURE_WAL_GZ" ]; then
    echo "  [SKIP] fixture not found, run make_oldfmt_fixture.sh first"
    echo "  expected: $FIXTURE_WAL_GZ"
    ASSERT_FAIL=$((ASSERT_FAIL + 1))
else
    fixture_key=$(cat "$FIXTURE_KEY_FILE")
    t22_net="net-${RID}-t22"
    t22_vol="vol-${RID}-t22"
    t22_c="c-${RID}-t22"
    
    docker network create "$t22_net" 2>/dev/null
    docker volume create "$t22_vol" 2>/dev/null
    
    # 解压旧格式WAL并复制到新卷 (FIXTURE_DIR_HOST用于Docker volume挂载)
    docker run --rm -v "${FIXTURE_DIR_HOSTA}:/fixture:ro" -v "$t22_vol:/app/wal-data" alpine:3.21 \
        sh -c 'gunzip -c /fixture/raftkv.wal.gz > /app/wal-data/raftkv.wal'
    
    # 用当前镜像启动, 挂载旧WAL
    docker run -d --name "$t22_c" --network "$t22_net" \
        -e RAFTKV_FP_ANCHOR="${FP_ANCHOR}" \
        -e SM4_KEY="$fixture_key" \
        -e NODE_ID=node-1 -e GRPC_PORT=9500 -e HTTP_PORT=9000 -e HTTP_BIND=0.0.0.0 \
        -v "${LICENSE_DIR}/node-1.key:/app/license.key:ro" \
        -v "${t22_vol}:/app/wal-data" \
        "$IMAGE_NAME" 2>&1
    
    sleep 8
    t22_logs=$(docker logs "$t22_c" 2>&1)
    echo "$t22_logs" | log_evidence "t22_replay.log"
    
    # 旧格式WAL无no-op, 20条数据, commit=20
    recovered=$(echo "$t22_logs" | grep "恢复" | sed -n 's/.*恢复 *\([0-9][0-9]*\) *条.*/\1/p' | head -1)
    assert_eq "$recovered" "20" "t22: recovered 20 entries from old-format WAL"
    
    t22_stats=$(docker exec "$t22_c" curl -s http://127.0.0.1:9000/raft/stats 2>/dev/null || echo "")
    t22_commit=$(extract_stat "$t22_stats" commit)
    assert_eq "$t22_commit" "20" "t22: commit=20 after old-format replay"
    
    docker rm -f "$t22_c" 2>/dev/null
    docker volume rm "$t22_vol" 2>/dev/null
    docker network rm "$t22_net" 2>/dev/null
fi

# ════════════════════════════════════════════════════════════
# t23: key_leak_scan — 运行时自检: 二进制strings搜旧key=0次
# ════════════════════════════════════════════════════════════
t_begin "t23" "key_leak_scan: runtime strings search for raftkv_sm4test01 → 0 hits"

up_cluster "${RID}-t23" || { echo "[t23] FAIL: cluster up"; suite_end; exit 1; }

# 在容器内扫描二进制
leak_count=$(docker exec "$(_c_name 1)" \
    sh -c 'grep -c "raftkv_sm4test01" /app/gateway 2>/dev/null || true')
assert_eq "$leak_count" "0" "t23: no hardcoded key in binary"

# 也扫描容器环境变量(不应包含旧key)
env_leak=$(docker exec "$(_c_name 1)" env 2>/dev/null | grep -c "raftkv_sm4test01" || true)
assert_eq "$env_leak" "0" "t23: no hardcoded key in env"

echo "binary_hits=$leak_count env_hits=$env_leak" | log_evidence "t23_leak.txt"
down_cluster "${RID}-t23"

suite_end