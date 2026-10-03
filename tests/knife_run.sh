#!/bin/bash
# knife_run.sh — CI门禁状态机（入口）
#
# 用法: ./tests/knife_run.sh <branch> <tag> [套件列表] [--no-tag]
# 例:   ./tests/knife_run.sh fix/ci-knife v1.0.0-dev6 baseline,idem,wal_snap
#
# 流程: BUILD → SMOKE → VERIFY → GATE
# 失败: ROLLBACK（git reset --hard 最近绿tag + 重建 + 冒烟 + FAIL报告 + exit 1）
# 红线: 永不自动修、永不自动重试、FAIL即回滚+终止+保留现场

set -euo pipefail

TESTS_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$TESTS_DIR/.." && pwd)"
EVIDENCE_DIR="${TESTS_DIR}/evidence"

# ── 参数解析 ──
BRANCH="${1:?用法: knife_run.sh <branch> <tag> [suites] [--no-tag]}"
TAG="${2:?用法: knife_run.sh <branch> <tag> [suites] [--no-tag]}"
SUITES="${3:-baseline,idem,wal_snap,health}"
NO_TAG=false
if [ "${4:-}" = "--no-tag" ] || [ "${5:-}" = "--no-tag" ]; then
    NO_TAG=true
fi

# ── 配置 ──
LAST_GREEN_TAG="${LAST_GREEN_TAG:-v1.0.0-dev6}"
IMAGE_NAME="raftkv:latest-knife"
ROLLBACK_IMAGE="raftkv:latest-rollback"
LICENSE_DIR="${LICENSE_DIR:-/licenses}"
FP_ANCHOR="${FP_ANCHOR:-raftkv-v25-test}"
RUN_ID="run-$(date +%Y%m%d_%H%M%S)"
RUN_EVIDENCE="${EVIDENCE_DIR}/${RUN_ID}"

mkdir -p "$RUN_EVIDENCE"

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║  knife_run CI门禁                                          ║"
echo "║  branch=$BRANCH  tag=$TAG  suites=$SUITES                  ║"
echo "║  run_id=$RUN_ID                                            ║"
echo "║  last_green=$LAST_GREEN_TAG  no_tag=$NO_TAG                ║"
echo "╚═══════════════════════════════════════════════════════════╝"

# ════════════════════════════════════════════════════════════
# ROLLBACK流程（严格）
# ════════════════════════════════════════════════════════════
export IMAGE_NAME LICENSE_DIR FP_ANCHOR EVIDENCE_DIR TESTS_DIR
_knife_run_id="$RUN_ID"
source "${TESTS_DIR}/harness.sh"
RUN_ID="$_knife_run_id"
do_rollback() {
    local fail_phase="$1"
    local fail_detail="$2"
    
    echo ""
    echo "╔═══════════════════════════════════════════════════════════╗"
    echo "║  ROLLBACK触发: $fail_phase                                  ║"
    echo "║  原因: $fail_detail                                         ║"
    echo "╚═══════════════════════════════════════════════════════════╝"
    
    # 写FAIL报告
    cat > "${RUN_EVIDENCE}/FAIL" << EOF
knife_run FAIL
phase: $fail_phase
detail: $fail_detail
run_id: $RUN_ID
branch: $BRANCH
tag: $TAG
timestamp: $(date -Iseconds)
last_green: $LAST_GREEN_TAG
EOF
    
    # git reset --hard 到最近绿tag
    cd "$REPO_DIR"
    git reset --hard "$LAST_GREEN_TAG" 2>&1 | tee -a "${RUN_EVIDENCE}/rollback.log"
    
    # 重建已知好状态镜像
    echo "[rollback] 重建 ${LAST_GREEN_TAG} 镜像..." | tee -a "${RUN_EVIDENCE}/rollback.log"
    docker build --platform linux/amd64 -t "$ROLLBACK_IMAGE" \
        -f Dockerfile . 2>&1 | tee -a "${RUN_EVIDENCE}/rollback_build.log"
    
    
    # 冒烟确认：双节点集群选主
    local rbk_rid="rbk-${RUN_ID}"
    local _saved_image="$IMAGE_NAME"
    IMAGE_NAME="$ROLLBACK_IMAGE"
    if up_cluster "$rbk_rid" 2>&1 | tee -a "${RUN_EVIDENCE}/rollback_smoke.log"; then
        echo "[rollback] 冒烟绿: Leader当选" | tee -a "${RUN_EVIDENCE}/rollback.log"
    else
        echo "[rollback] 警告: 冒烟也失败! 基线可能已损坏!" | tee -a "${RUN_EVIDENCE}/rollback.log"
    fi
    down_cluster "$rbk_rid"
    IMAGE_NAME="$_saved_image"
    echo "[rollback] FAIL报告: ${RUN_EVIDENCE}/FAIL"
    echo "[rollback] 终止. 不修、不重试、不tag. 请人诊断."
    exit 1
}

# ════════════════════════════════════════════════════════════
# Phase 1: BUILD
# ════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════════════"
echo "  Phase 1: BUILD"
echo "═══════════════════════════════════════════════════"

cd "$REPO_DIR"
git checkout "$BRANCH" 2>&1 | tee "${RUN_EVIDENCE}/build.log"

echo "[build] docker build..." | tee -a "${RUN_EVIDENCE}/build.log"
docker build --platform linux/amd64 -t "$IMAGE_NAME" \
    -f Dockerfile . 2>&1 | tee -a "${RUN_EVIDENCE}/build.log"

if [ "${PIPESTATUS[0]}" -ne 0 ]; then
    do_rollback "BUILD" "docker build failed"
fi

# F1纪律: strings搜旧key必须0次命中
echo "[build] key leak scan..." | tee -a "${RUN_EVIDENCE}/build.log"
leak=$(docker run --rm --entrypoint sh "$IMAGE_NAME" \
    -c 'grep -c "raftkv_sm4test01" /app/gateway 2>/dev/null || true')
echo "key leak hits: $leak" | tee -a "${RUN_EVIDENCE}/build.log"
if [ "$leak" != "0" ]; then
    do_rollback "BUILD" "key leak: $leak hits of raftkv_sm4test01 in binary"
fi
echo "[build] PASS: build + key leak scan clean"

# ════════════════════════════════════════════════════════════
# Phase 2: SMOKE
# ════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════════════"
echo "  Phase 2: SMOKE"
echo "═══════════════════════════════════════════════════"


SMOKE_RID="smoke-${RUN_ID}"
if ! up_cluster "$SMOKE_RID" 2>&1 | tee "${RUN_EVIDENCE}/smoke.log"; then
    do_rollback "SMOKE" "cluster up failed or Leader not elected in 30s"
fi
echo "[smoke] PASS: Leader elected" | tee -a "${RUN_EVIDENCE}/smoke.log"
down_cluster "$SMOKE_RID"

# ════════════════════════════════════════════════════════════
# Phase 3: VERIFY
# ════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════════════"
echo "  Phase 3: VERIFY"
echo "═══════════════════════════════════════════════════"

TOTAL_PASS=0
TOTAL_FAIL=0

IFS=',' read -ra SUITE_LIST <<< "$SUITES"
for suite in "${SUITE_LIST[@]}"; do
    echo ""
    echo "▶ 套件: $suite"
    suite_log="${RUN_EVIDENCE}/suite_${suite}.log"
    
    case "$suite" in
        baseline)
            bash "${TESTS_DIR}/suite_baseline.sh" 2>&1 | tee "$suite_log"
            ;;
        idem)
            bash "${TESTS_DIR}/suite_idem.sh" 2>&1 | tee "$suite_log"
            ;;
        wal_snap)
            bash "${TESTS_DIR}/suite_wal_snap.sh" 2>&1 | tee "$suite_log"
            ;;
        health)
            bash "${TESTS_DIR}/suite_health.sh" 2>&1 | tee "$suite_log"
            ;;
        *)
            echo "  [ERROR] 未知套件: $suite"
            do_rollback "VERIFY" "unknown suite: $suite"
            ;;
    esac
    
    suite_rc=${PIPESTATUS[0]}
    if [ "$suite_rc" -ne 0 ]; then
        do_rollback "VERIFY" "suite $suite failed (exit=$suite_rc)"
    fi
    
    # 从suite日志提取断言计数
    suite_pass=$(grep -o 'PASS=[0-9]*' "$suite_log" | tail -1 | cut -d= -f2)
    suite_fail=$(grep -o 'FAIL=[0-9]*' "$suite_log" | tail -1 | cut -d= -f2)
    TOTAL_PASS=$((TOTAL_PASS + ${suite_pass:-0}))
    TOTAL_FAIL=$((TOTAL_FAIL + ${suite_fail:-0}))
    echo "  $suite: PASS=${suite_pass:-0} FAIL=${suite_fail:-0}"
done

echo ""
echo "  总计: PASS=$TOTAL_PASS FAIL=$TOTAL_FAIL"

# ════════════════════════════════════════════════════════════
# Phase 4: GATE
# ════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════════════"
echo "  Phase 4: GATE"
echo "═══════════════════════════════════════════════════"

if [ "$TOTAL_FAIL" -gt 0 ]; then
    do_rollback "GATE" "$TOTAL_FAIL assertions failed"
fi

# 写PASS报告
cat > "${RUN_EVIDENCE}/PASS" << EOF
knife_run PASS
run_id: $RUN_ID
branch: $BRANCH
tag: $TAG
timestamp: $(date -Iseconds)
suites: $SUITES
total_pass: $TOTAL_PASS
total_fail: $TOTAL_FAIL
EOF

echo ""
echo "╔═══════════════════════════════════════════════════════════╗"
echo "║  PASS REPORT                                               ║"
echo "║  run_id: $RUN_ID                                           ║"
echo "║  suites: $SUITES                                           ║"
echo "║  assertions: PASS=$TOTAL_PASS FAIL=$TOTAL_FAIL             ║"
echo "╚═══════════════════════════════════════════════════════════╝"

# 自动tag + merge
if [ "$NO_TAG" = "false" ]; then
    cd "$REPO_DIR"
    echo "[gate] 创建tag $TAG..."
    git tag -a "$TAG" -m "knife_run PASS: $TAG (suites=$SUITES, pass=$TOTAL_PASS)"
    
    echo "[gate] merge --no-ff 回 v1.0-dev..."
    git checkout v1.0-dev 2>&1 | tee -a "${RUN_EVIDENCE}/gate.log"
    git merge --no-ff "$BRANCH" -m "Merge $BRANCH: knife_run PASS ($TAG)" 2>&1 | tee -a "${RUN_EVIDENCE}/gate.log"
    
    echo "[gate] tag + merge 完成"
    git log --oneline -5 | tee -a "${RUN_EVIDENCE}/gate.log"
    git tag -l "v1.0.0-*" | tee -a "${RUN_EVIDENCE}/gate.log"
else
    echo "[gate] --no-tag 模式, 跳过 tag+merge"
fi

echo ""
echo "[knife_run] 完成. evidence: ${RUN_EVIDENCE}/"
exit 0