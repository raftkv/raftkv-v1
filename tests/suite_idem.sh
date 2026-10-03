#!/bin/bash
# suite_idem.sh — 幂等套件 t10-t14
# 覆盖: 串行去重, 并发去重(命门), LRU淘汰,
#       失败重试(命门), 无token兼容
set -euo pipefail
source "$(dirname "$0")/harness.sh"

suite_begin "idem"

RID="idem-$(date +%s)"

# ════════════════════════════════════════════════════════════
# t10: serial_dedup — 同token串行3次→增量=1, duplicate=true
# ════════════════════════════════════════════════════════════
t_begin "t10" "serial_dedup: same token x3 serial → increment=1, duplicate=true"

up_cluster "${RID}-t10" || { echo "[t10] FAIL: cluster up"; suite_end; exit 1; }
leader=$(get_leader)

s0=$(stats "$leader"); logs0=$(extract_stat "$s0" logs)

r1=$(propose "$leader" "tok-t10" "data-t10")
r2=$(propose "$leader" "tok-t10" "data-t10")
r3=$(propose "$leader" "tok-t10" "data-t10")

s1=$(stats "$leader"); logs1=$(extract_stat "$s1" logs)
delta=$((logs1 - logs0))
idx1=$(json_field "$r1" index)
idx2=$(json_field "$r2" index)
idx3=$(json_field "$r3" index)
dup2=$(json_field "$r2" duplicate)
dup3=$(json_field "$r3" duplicate)

assert_eq "$delta" "1" "t10: logs increment=1"
assert_eq "$idx1" "$idx2" "t10: r1.index==r2.index"
assert_eq "$idx2" "$idx3" "t10: r2.index==r3.index"
assert_eq "$dup2" "true" "t10: r2 duplicate=true"
assert_eq "$dup3" "true" "t10: r3 duplicate=true"
echo "r1=$r1 r2=$r2 r3=$r3 delta=$delta" | log_evidence "t10.txt"
down_cluster "${RID}-t10"

# ════════════════════════════════════════════════════════════
# t11: concurrent_dedup (命门) — 同token并发10次→增量=1
# ════════════════════════════════════════════════════════════
t_begin "t11" "concurrent_dedup: same token x10 concurrent → increment=1"

up_cluster "${RID}-t11" || { echo "[t11] FAIL: cluster up"; suite_end; exit 1; }
leader=$(get_leader)

s0=$(stats "$leader"); logs0=$(extract_stat "$s0" logs)

# 并发10次同token
tmpdir=$(mktemp -d)
for i in $(seq 1 10); do
    (propose "$leader" "tok-t11" "data-t11" > "${tmpdir}/r${i}.txt") &
done
wait

s1=$(stats "$leader"); logs1=$(extract_stat "$s1" logs)
delta=$((logs1 - logs0))
assert_eq "$delta" "1" "t11: concurrent logs increment=1"

# 所有响应index相同
first_idx=""
all_same=1
for i in $(seq 1 10); do
    idx=$(json_field "$(cat "${tmpdir}/r${i}.txt")" index)
    if [ -z "$first_idx" ]; then first_idx="$idx"; fi
    if [ "$idx" != "$first_idx" ]; then all_same=0; fi
done
assert_eq "$all_same" "1" "t11: all 10 responses same index=${first_idx}"
cat "${tmpdir}"/r*.txt | log_evidence "t11_concurrent.txt"
rm -rf "$tmpdir"
down_cluster "${RID}-t11"

# ════════════════════════════════════════════════════════════
# t12: lru_evict — CAPACITY=5, 写7token→size=5, 最早token重试→新条目
# ════════════════════════════════════════════════════════════
t_begin "t12" "lru_evict: CAPACITY=5, 7 tokens → size=5, evicted retry → new entry"

up_cluster "${RID}-t12" "IDEM_TOKEN_CAPACITY=5" || { echo "[t12] FAIL: cluster up"; suite_end; exit 1; }
leader=$(get_leader)

# 写7个不同token
for i in $(seq 1 7); do
    propose "$leader" "tok-t12-${i}" "data-t12-${i}" > /dev/null 2>&1
done
sleep 1

is=$(idem_stats "$leader")
size=$(json_field "$is" size)
assert_eq "$size" "5" "t12: idem size==5 after 7 tokens"

# 重试最早token(tok-t12-1, 应被淘汰)→产生新条目
s_before=$(stats "$leader"); logs_before=$(extract_stat "$s_before" logs)
r_evict=$(propose "$leader" "tok-t12-1" "data-t12-1-retry")
s_after=$(stats "$leader"); logs_after=$(extract_stat "$s_after" logs)
delta=$((logs_after - logs_before))
dup=$(json_field "$r_evict" duplicate)
assert_eq "$delta" "1" "t12: evicted token retry → new entry (increment=1)"
assert_ne "$dup" "true" "t12: evicted token retry → not duplicate"
echo "idem_stats=$is retry=$r_evict delta=$delta" | log_evidence "t12.txt"
down_cluster "${RID}-t12"

# ════════════════════════════════════════════════════════════
# t13: fail_retry (命门) — propose失败→entry移除→恢复→同token重发成功
# ════════════════════════════════════════════════════════════
t_begin "t13" "fail_retry: propose fails → entry removed → restore → same token succeeds"

up_cluster "${RID}-t13" || { echo "[t13] FAIL: cluster up"; suite_end; exit 1; }
leader=$(get_leader); follower=$(get_follower)

# 先写几条正常数据
write_entries "$leader" 5 "t13-warmup"
sleep 1

# 停Follower使Leader无法commit(2节点需多数派=2)
stop_node "$follower"
sleep 2

# 发propose带token→会timeout(Leader无法commit)
r_fail=$(propose "$leader" "tok-t13-fail" "data-t13-fail")
success_fail=$(json_field "$r_fail" success)
echo "fail response: $r_fail" | log_evidence "t13_fail_resp.txt"

# 检查idem/stats: entry应已移除(Remove on error)
sleep 1
is=$(idem_stats "$leader")
size=$(json_field "$is" size)
assert_eq "$size" "0" "t13: idem size==0 after failure (entry removed)"

# 恢复集群
start_node "$follower"
sleep 15

# 同token重发→应成功, 新index, 非缓存失败
s_before=$(stats "$leader"); logs_before=$(extract_stat "$s_before" logs)
r_retry=$(propose "$leader" "tok-t13-fail" "data-t13-fail")
s_after=$(stats "$leader"); logs_after=$(extract_stat "$s_after" logs)
delta=$((logs_after - logs_before))
success_retry=$(json_field "$r_retry" success)
assert_eq "$delta" "1" "t13: retry → new entry (increment=1)"
assert_eq "$success_retry" "true" "t13: retry success=true"
echo "retry response: $r_retry delta=$delta" | log_evidence "t13_retry_resp.txt"
down_cluster "${RID}-t13"

# ════════════════════════════════════════════════════════════
# t14: no_token — 无token→正常append, 无duplicate字段
# ════════════════════════════════════════════════════════════
t_begin "t14" "no_token: no token → normal append, no duplicate field"

up_cluster "${RID}-t14" || { echo "[t14] FAIL: cluster up"; suite_end; exit 1; }
leader=$(get_leader)

s0=$(stats "$leader"); logs0=$(extract_stat "$s0" logs)
r=$(propose "$leader" "data-t14")
s1=$(stats "$leader"); logs1=$(extract_stat "$s1" logs)
delta=$((logs1 - logs0))
success=$(json_field "$r" success)
has_dup=$(echo "$r" | grep -c "duplicate" || true)
assert_eq "$delta" "1" "t14: no-token increment=1"
assert_eq "$success" "true" "t14: no-token success=true"
assert_eq "$has_dup" "0" "t14: no duplicate field in response"
echo "response=$r delta=$delta" | log_evidence "t14.txt"
down_cluster "${RID}-t14"

suite_end