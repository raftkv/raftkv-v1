#!/bin/bash
# _audit.sh — D1-batch 机械验收
# 用法: bash tests/_audit.sh [seal_md5]
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TESTS_DIR="$(cd "$(dirname "$0")" && pwd)"

# 封印值：参数 > 文件 > 默认
SEAL_MD5="${1:-}"
if [ -z "$SEAL_MD5" ] && [ -f "${TESTS_DIR}/.seal_md5" ]; then
    SEAL_MD5=$(cat "${TESTS_DIR}/.seal_md5" | tr -d '[:space:]')
fi
if [ -z "$SEAL_MD5" ]; then
    SEAL_MD5="27929268daf6cdd5d46dfa8bde6aa768"
fi

# 找最新含DECISION.md的pipeline目录（跳过--soak等无DECISION的目录）
PDIR=""
for d in $(ls -dt "${TESTS_DIR}"/evidence/pipeline-* 2>/dev/null); do
    if [ -f "${d}/DECISION.md" ]; then
        PDIR="$d"
        break
    fi
done
if [ -z "${PDIR:-}" ] || [ ! -d "$PDIR" ]; then
    echo "AUDIT FAIL: 找不到含DECISION.md的pipeline目录"
    exit 1
fi
PLOG="${PDIR}/progress.log"
DECISION="${PDIR}/DECISION.md"
PIPELINE_SH="${TESTS_DIR}/run_pipeline.sh"

PASS_CNT=0
FAIL_CNT=0

audit_pass() { echo "[PASS] $1"; PASS_CNT=$((PASS_CNT + 1)); }
audit_fail() { echo "[FAIL] $1"; FAIL_CNT=$((FAIL_CNT + 1)); }

# ── 1. md5封印核验 ──
actual_md5=$(md5sum "$PIPELINE_SH" | cut -d' ' -f1)
if [ "$actual_md5" = "$SEAL_MD5" ]; then
    audit_pass "1. md5封印核验 ($actual_md5)"
else
    audit_fail "1. md5封印核验 (期望=$SEAL_MD5 实际=$actual_md5)"
fi

# ── 2. bash -n语法检查 ──
if bash -n "$PIPELINE_SH" 2>/dev/null; then
    audit_pass "2. bash -n语法检查"
else
    audit_fail "2. bash -n语法检查"
fi

# ── 3. grep '[$' = 0 ──
dollar_bracket=$(grep -cF '[$' "$PIPELINE_SH" 2>/dev/null || true)
if [ "$dollar_bracket" = "0" ]; then
    audit_pass "3. grep '[\$' = 0"
else
    audit_fail "3. grep '[\$' = $dollar_bracket"
fi

# ── 4. 断言计数对账 ──
# 对账规则:
#   a) DECISION.md "总计: PASS=N FAIL=M" 行数 = 4 (T1+T2+T3+终局)
#   b) 每行 FAIL=0
#   c) 所有行 PASS值相同
#   d) progress.log含T1/T2/T3/终局各至少一个PASS
#   e) "FAIL 冻结"数仅报告, 不判定
if [ -f "$DECISION" ]; then
    dec_total_lines=$(grep -c '总计: PASS=' "$DECISION" 2>/dev/null || true)
    dec_fail_nonzero=$(grep '总计: PASS=' "$DECISION" | grep -vE 'FAIL=0' | wc -l | tr -d ' ' || true)
    dec_pass_unique=$(grep '总计: PASS=' "$DECISION" | grep -oE 'PASS=[0-9]+' | sort -u | wc -l | tr -d ' ' || true)
    dec_pass=$(grep '总计: PASS=' "$DECISION" | head -1 | grep -oE 'PASS=[0-9]+' | cut -d= -f2 2>/dev/null || true)
    t1_ok=$(grep -c 'T1 PASS' "$PLOG" 2>/dev/null || true)
    t2_ok=$(grep -c 'T2 PASS' "$PLOG" 2>/dev/null || true)
    t3_ok=$(grep -c 'T3 PASS' "$PLOG" 2>/dev/null || true)
    final_ok=$(grep -c '终局PASS' "$PLOG" 2>/dev/null || true)
    freeze_count=$(grep -c 'FAIL 冻结' "$PLOG" 2>/dev/null || true)
    if [ "${dec_total_lines:-0}" = "4" ] && [ "${dec_fail_nonzero:-0}" = "0" ] \
       && [ "${dec_pass_unique:-0}" = "1" ] && [ "${t1_ok:-0}" -ge 1 ] \
       && [ "${t2_ok:-0}" -ge 1 ] && [ "${t3_ok:-0}" -ge 1 ] \
       && [ "${final_ok:-0}" -ge 1 ]; then
        audit_pass "4. 断言计数对账 (总计行=$dec_total_lines/4, PASS=$dec_pass, FAIL=0, T1=$t1_ok T2=$t2_ok T3=$t3_ok 终局=$final_ok, 冻结=$freeze_count)"
    else
        audit_fail "4. 断言计数对账 (总计行=${dec_total_lines:-0}/4, FAIL非0行=${dec_fail_nonzero:-0}, PASS唯一值=${dec_pass_unique:-0}/1, T1=${t1_ok:-0} T2=${t2_ok:-0} T3=${t3_ok:-0} 终局=${final_ok:-0}, 冻结=${freeze_count:-0})"
    fi
else
    audit_fail "4. 断言计数对账 (DECISION.md不存在)"
fi

# ── 5. SHA核对 ──
sha_ok=true
sha_detail=""
for task in T1 T2 T3; do
    plog_sha=$(grep "${task} PASS" "$PLOG" | grep -oE 'commit=[a-f0-9]{7}' | tail -1 | cut -d= -f2 2>/dev/null || true)
    if [ -n "$plog_sha" ] && [ "$plog_sha" != "unchanged" ]; then
        if git -C "$REPO_DIR" cat-file -e "${plog_sha}^{commit}" 2>/dev/null; then
            sha_detail="${sha_detail}${task}=${plog_sha}✓ "
        else
            sha_ok=false
            sha_detail="${sha_detail}${task}=${plog_sha}✗ "
        fi
    fi
done
if $sha_ok; then
    audit_pass "5. SHA核对 ($sha_detail)"
else
    audit_fail "5. SHA核对 ($sha_detail)"
fi

# ── 6. tag计数 ──
tag_count=$(git -C "$REPO_DIR" tag -l 'v1.0.0-*' | wc -l | tr -d ' ')
if [ "$tag_count" -ge 2 ]; then
    audit_pass "6. tag计数 ($tag_count)"
else
    audit_fail "6. tag计数 ($tag_count < 2)"
fi

# ── 总判定 ──
echo ""
echo "=============================="
echo "  AUDIT: PASS=$PASS_CNT  FAIL=$FAIL_CNT"
echo "=============================="
if [ "$FAIL_CNT" = "0" ]; then
    echo "  ALL PASS"
    exit 0
else
    echo "  HAS FAIL"
    exit 1
fi