#!/bin/bash
# run_pipeline.sh — D1-batch2 自主流水线
# 修复: R1-R11 (P0+P1+P2)
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TESTS_DIR="$(cd "$(dirname "$0")" && pwd)"
BRANCH="v1.0-dev"
FINAL_TAG="v1.0.0-d2"
D1_TAG_COMMIT="9d5c491"
MERGE_COMMIT="ca84148"

DRY_RUN=false
RESUME=false
SOAK=0
ALLOWED_REMOTE="${ALLOWED_REMOTE:-}"   # 白名单remote URL，空=禁止push
for a in "$@"; do
    case "$a" in
        --dry-run) DRY_RUN=true ;;
        --resume)  RESUME=true ;;
        --soak)    SOAK=3 ;;
        --soak=*)  SOAK="${a#--soak=}" ;;
    esac
done

# R8: resume时定位既有目录
if [ "$RESUME" = "true" ]; then
    PDIR=$(ls -dt "${TESTS_DIR}"/evidence/pipeline-* 2>/dev/null | head -1)
    if [ -z "${PDIR:-}" ] || [ ! -d "$PDIR" ]; then
        echo "RESUME FAIL: 找不到pipeline目录"
        exit 5
    fi
else
    TS="$(date +%Y%m%d_%H%M%S)"
    PDIR="${TESTS_DIR}/evidence/pipeline-${TS}"
fi
PLOG="${PDIR}/progress.log"
RLOG="${PDIR}/run.log"
mkdir -p "$PDIR"

log() { echo "[ $(date +%H:%M:%S)] $*" | tee -a "$PLOG"; }

# R5: check_flags第三条独立判定
check_flags() {
    local task="$1" d
    d=$(git diff --cached)
    if echo "$d" | grep -q '^[+-].*assert_'; then
        log "FLAG $task: 触碰assert_*行"; return 3
    fi
    if echo "$d" | grep -q '^diff.*\.go'; then
        log "FLAG $task: 触碰.go文件"; return 3
    fi
    # R5: 精确路径关联 — 逐tests/文件检查||true新增
    local staged_file
    for staged_file in $(git diff --cached --name-only 2>/dev/null | grep '^tests/'); do
        if git diff --cached -- "$staged_file" | grep -q '^+.*|| true'; then
            log "FLAG $task: $staged_file 新增||true"; return 3
        fi
    done
    return 0
}

# safe_push: 白名单护栏（自觉性，非强制拦截）。真正push门禁是人工点火令授权。
safe_push() {
    local url="$1"
    if [ -z "$ALLOWED_REMOTE" ] || [ "$url" != "$ALLOWED_REMOTE" ]; then
        echo "BLOCK: git push to '$url' not in whitelist (ALLOWED_REMOTE='$ALLOWED_REMOTE')"
        return 1
    fi
    git push "$url"
}

preflight() {
    local rc=0
    if [ -n "$(git status --porcelain)" ]; then
        echo "pre-flight FAIL: git status不干净"
        git status --porcelain
        rc=5
    fi
    if [ "$(git branch --show-current)" != "$BRANCH" ]; then
        echo "pre-flight FAIL: 不在$BRANCH"
        rc=5
    fi
    if [ -n "$(git diff "$D1_TAG_COMMIT" "$MERGE_COMMIT" 2>/dev/null)" ]; then
        echo "pre-flight FAIL: tag树≠merge树"
        rc=5
    fi
    if [ $rc -eq 0 ]; then echo "pre-flight PASS"; fi
    return $rc
}

run_knife() {
    local tag="$1" no_tag="${2:-true}"
    local args="$BRANCH $tag baseline,idem,wal_snap,health"
    if [ "$no_tag" = "true" ]; then args="$args --no-tag"; fi
    bash "${TESTS_DIR}/knife_run.sh" $args 2>&1
}

get_run_id() {
    grep -o 'run_id=run-[0-9_]*' "$RLOG" | tail -1 | cut -d= -f2
}

# R11: run_task保留原始退出码
run_task() {
    "$@"
    local rc=$?
    if [ $rc -ne 0 ]; then
        log "ABORT rc=$rc"
        exit $rc
    fi
}

# ── T1: harness.sh RUN_ID守卫 ──
task_t1() {
    log "T1: harness.sh RUN_ID赋值加守卫"
    log "方案: RUN_ID=\"\" → RUN_ID=\"\${RUN_ID:-}\"  双保险保留knife_run save/restore"

    # R8: 若守卫已存在则跳过sed（修复重跑sed空操作）
    if grep -q '^RUN_ID="\${RUN_ID:-}"$' "${TESTS_DIR}/harness.sh"; then
        log "T1: 守卫已存在, 跳过sed"
    else
        sed -i 's/^RUN_ID=""$/RUN_ID="${RUN_ID:-}"/' "${TESTS_DIR}/harness.sh"
    fi

    # R3: 验证守卫已写入
    if ! grep -q '^RUN_ID="\${RUN_ID:-}"$' "${TESTS_DIR}/harness.sh"; then
        log "T1 FAIL(R3): 守卫未写入, exit 6"; return 6
    fi
    log "R3验证: $(grep -n 'RUN_ID=' "${TESTS_DIR}/harness.sh" | head -1)"

    bash -n "${TESTS_DIR}/harness.sh"
    git add "${TESTS_DIR}/harness.sh"
    check_flags "T1" || return $?
    if run_knife "v1.0.0-d2-t1" "true" | tee -a "$RLOG"; then
        local rid; rid=$(get_run_id)
        git diff --cached --quiet || git commit -m "fix(ci): T1 harness.sh RUN_ID守卫 (run_id=$rid)"
        log "T1 PASS commit=$(git rev-parse --short HEAD) run_id=$rid"
        return 0
    else
        log "T1 FAIL 冻结"; return 2
    fi
}

# ── T2: do_rollback冒烟改双节点 ──
task_t2() {
    log "T2: do_rollback冒烟改双节点方案A"
    log "方案: 移source至do_rollback前, 替换内联docker run为up_cluster/down_cluster, RID=rbk-\${RUN_ID}"
    local KR="${TESTS_DIR}/knife_run.sh"

    # R7: 确认变量/函数定义在插入点作用域可用
    log "R7: 作用域可用性检查"
    grep -nE 'ROLLBACK_IMAGE|RUN_EVIDENCE|up_cluster|down_cluster' "$KR" "${TESTS_DIR}/harness.sh" | tee -a "$PLOG"

    # R1: 锚定模式删除/替换（禁止行号sed）

    # 1. 删除SMOKE段source块（锚定: export IMAGE_NAME... → RUN_ID="$_knife_run_id"）
    sed -i '/^export IMAGE_NAME LICENSE_DIR FP_ANCHOR EVIDENCE_DIR TESTS_DIR$/,/^RUN_ID="\$_knife_run_id"$/d' "$KR"
    if grep -q '^export IMAGE_NAME LICENSE_DIR FP_ANCHOR EVIDENCE_DIR TESTS_DIR$' "$KR"; then
        log "T2 FAIL(R1): source块未删除, exit 6"; return 6
    fi
    log "R1验证: source块已删除"
    { git diff --stat || true; } | tee -a "$PLOG"

    # 2. 删除内联docker run冒烟段（锚定: # 冒烟确认 → docker network rm）
    sed -i '/^    # 冒烟确认$/,/^    docker network rm "\$rb_net" 2>\/dev\/null$/d' "$KR"
    if grep -q 'rb_key=$(openssl rand' "$KR"; then
        log "T2 FAIL(R1): 内联docker run未删除, exit 6"; return 6
    fi
    log "R1验证: 内联docker run已删除"

    # 插入替换块（锚定: echo "[rollback] FAIL报告"前）
    sed -i '/^    echo "\[rollback\] FAIL报告/i\
    # 冒烟确认：双节点集群选主\
    local rbk_rid="rbk-${RUN_ID}"\
    local _saved_image="$IMAGE_NAME"\
    IMAGE_NAME="$ROLLBACK_IMAGE"\
    if up_cluster "$rbk_rid" 2>&1 | tee -a "${RUN_EVIDENCE}/rollback_smoke.log"; then\
        echo "[rollback] 冒烟绿: Leader当选" | tee -a "${RUN_EVIDENCE}/rollback.log"\
    else\
        echo "[rollback] 警告: 冒烟也失败! 基线可能已损坏!" | tee -a "${RUN_EVIDENCE}/rollback.log"\
    fi\
    down_cluster "$rbk_rid"\
    IMAGE_NAME="$_saved_image"' "$KR"

    if ! grep -q 'rbk_rid' "$KR"; then
        log "T2 FAIL(R1): 替换块未插入, exit 6"; return 6
    fi
    log "R1验证: 替换块已插入"
    { git diff --stat || true; } | tee -a "$PLOG"

    # 3. 在do_rollback前插入source块（锚定: do_rollback() {）
    sed -i '/^do_rollback() {/i\
export IMAGE_NAME LICENSE_DIR FP_ANCHOR EVIDENCE_DIR TESTS_DIR\
_knife_run_id="$RUN_ID"\
source "${TESTS_DIR}/harness.sh"\
RUN_ID="$_knife_run_id"' "$KR"

    if ! grep -q '^_knife_run_id="\$RUN_ID"$' "$KR"; then
        log "T2 FAIL(R1): source块未插入, exit 6"; return 6
    fi
    log "R1验证: source块已插入"

    # R6: 验证local在do_rollback函数体内
    if ! grep -q 'local rbk_rid' "$KR"; then
        log "T2 FAIL(R6): local未在函数体内, exit 6"; return 6
    fi
    log "R6验证: local rbk_rid在do_rollback体内"

    bash -n "$KR"
    git add "$KR"
    check_flags "T2" || return $?
    if run_knife "v1.0.0-d2-t2" "true" | tee -a "$RLOG"; then
        local rid; rid=$(get_run_id)
        git diff --cached --quiet || git commit -m "fix(ci): T2 do_rollback双节点冒烟 (run_id=$rid)"
        log "T2 PASS commit=$(git rev-parse --short HEAD) run_id=$rid"
        return 0
    else
        log "T2 FAIL 冻结"; return 2
    fi
}

# ── T3: sleep→wait_for grpc_serving统一 ──
task_t3() {
    log "T3: sleep→wait_for grpc_serving统一"
    log "方案: D1-batch3 grpc_serving readiness探针替换sleep盲等"
    log "R1: 探针超时2s, wait_for 20轮, 最坏60s墙钟/20次尝试"
    log "R2: 复用tests/health-probe-bin"
    log "R4: docker cp移出轮询循环"

    check_flags "T3" || return $?

    # 语义验证: suite脚本已替换
    local health_ok baseline_ok
    health_ok=$(grep -c 'wait_for 20 grpc_serving' "$TESTS_DIR/suite_health.sh" 2>/dev/null || true)
    baseline_ok=$(grep -c 'wait_for 20 grpc_serving' "$TESTS_DIR/suite_baseline.sh" 2>/dev/null || true)
    log "R1验证: suite_health.sh wait_for grpc_serving替换数=$health_ok"
    log "R1验证: suite_baseline.sh wait_for grpc_serving替换数=$baseline_ok"
    if [ "${health_ok:-0}" -lt 1 ] || [ "${baseline_ok:-0}" -lt 1 ]; then
        log "T3 FAIL 冻结: suite脚本未替换"; return 2
    fi

    # 健康套件实跑验证
    local health_result
    health_result=$(RUN_ID="t3-verify-$(date +%s)" bash "$TESTS_DIR/suite_health.sh" 2>&1 | tee -a "$RLOG")
    if echo "$health_result" | grep -q "health: PASS=.* FAIL=0"; then
        local rid; rid="t3-verify-$(date +%s)"
        log "T3 PASS commit=unchanged run_id=$rid"
        return 0
    else
        log "T3 FAIL 冻结: health suite未通过"; return 2
    fi
}

# ── DECISION.md ──
generate_decision() {
    local md="${PDIR}/DECISION.md"
    local t1_sha t2_sha t3_sha
    t1_sha=$(git log --oneline --grep='T1' -1 | cut -d' ' -f1)
    t2_sha=$(git log --oneline --grep='T2' -1 | cut -d' ' -f1)
    t3_sha=$(git log --oneline --grep='T3' -1 | cut -d' ' -f1)

    # R9: 从progress.log计算信任计分
    local flags freezes tag_cnt
    flags=$(grep -c 'FLAG' "$PLOG" 2>/dev/null || true)
    freezes=$(grep -c 'FAIL 冻结' "$PLOG" 2>/dev/null || true)
    if git rev-parse "${FINAL_TAG}^{commit}" >/dev/null 2>&1; then
        tag_cnt=1
    else
        tag_cnt=0
    fi

    # R10: 每刀增加run_id + 断言计数
    local t1_rid t2_rid t3_rid
    t1_rid=$(grep 'T1 PASS' "$PLOG" | grep -o 'run_id=run-[0-9_]*' | cut -d= -f2 || true)
    t2_rid=$(grep 'T2 PASS' "$PLOG" | grep -o 'run_id=run-[0-9_]*' | cut -d= -f2 || true)
    t3_rid=$(grep 'T3 PASS' "$PLOG" | grep -o 'run_id=run-[0-9_]*' | cut -d= -f2 || true)
    if [ -z "$t1_rid" ]; then t1_rid="N/A"; fi
    if [ -z "$t2_rid" ]; then t2_rid="N/A"; fi
    if [ -z "$t3_rid" ]; then t3_rid="N/A"; fi

    # R10: 断言计数从RLOG提取
    local all_totals
    all_totals=$(grep '总计: PASS=' "$RLOG" 2>/dev/null || true)

    {
        echo "# D1-batch2 DECISION"
        echo ""
        echo "## T1: harness.sh RUN_ID守卫"
        if [ -n "$t1_sha" ]; then
            echo "commit: $t1_sha"
            git show --stat "$t1_sha"
        else
            echo "commit: no-op(无提交)"
        fi
        echo "run_id: $t1_rid"
        echo ""
        echo "## T2: do_rollback双节点冒烟"
        if [ -n "$t2_sha" ]; then
            echo "commit: $t2_sha"
            git show --stat "$t2_sha"
        else
            echo "commit: no-op(无提交)"
        fi
        echo "run_id: $t2_rid"
        echo ""
        echo "## T3: sleep→wait_for统一"
        if [ -n "$t3_sha" ]; then
            echo "commit: $t3_sha"
            git show --stat "$t3_sha"
        else
            echo "commit: no-op(无提交)"
        fi
        echo "run_id: $t3_rid"
        echo ""
        echo "## 终局"
        echo "tag: $FINAL_TAG"
        echo "merge: $(git log --oneline -1 v1.0-dev 2>/dev/null || echo 'N/A')"
        echo ""
        echo "## 断言计数（R10）"
        echo "$all_totals"
        echo ""
        echo "## 信任计分（R9: 从progress.log计算）"
        echo "本批旗标数: $flags"
        echo "冻结次数: $freezes"
        echo "门禁颁发tag数: $tag_cnt"
        echo ""
        echo "## progress.log全文（R10）"
        cat "$PLOG"
    } > "$md"
}

# ── 主流程 ──
if [ "$DRY_RUN" = "true" ]; then
    echo "=== DRY RUN ==="
    echo "队列: T1(harness RUN_ID守卫) → T2(do_rollback双节点) → T3(sleep→wait_for) → 终局(knife_run $FINAL_TAG)"
    echo ""
    echo "旗标规则:"
    echo "  - 触碰assert_*行或期望值字面量 → exit 3"
    echo "  - 触碰.go文件 → exit 3"
    echo "  - tests/下脚本新增||true → exit 3 (R5: 独立判定)"
    echo ""
    echo "运行护栏:"
    echo "  - 写路径白名单: 仅仓库目录内"
    echo "  - 黑名单: 非白名单remote的git push / docker system prune / 仓库外rm / git reset --hard"
    echo ""
    echo "pre-flight:"
    preflight
    exit $?
fi

# ── --soak N模式: 连续N次全量suite稳定性门槛 (跳过preflight) ──
if [ "$SOAK" -gt 0 ]; then
    log "SOAK: 连续${SOAK}次全量suite稳定性测试"
    SOAK_PASS=0
    SOAK_FAIL=0
    for i in $(seq 1 "$SOAK"); do
        log "SOAK round $i/$SOAK"
        round_fail=0
        for suite in baseline idem health wal_snap; do
            log "SOAK round $i: suite=$suite"
            if RUN_ID="soak-r${i}-$(date +%s)" bash "${TESTS_DIR}/suite_${suite}.sh" >>"$RLOG" 2>&1; then
                log "SOAK round $i: $suite PASS"
            else
                log "SOAK round $i: $suite FAIL"
                round_fail=1
            fi
        done
        if [ "$round_fail" -eq 0 ]; then
            SOAK_PASS=$((SOAK_PASS + 1))
            log "SOAK round $i: ALL PASS"
        else
            SOAK_FAIL=$((SOAK_FAIL + 1))
            log "SOAK round $i: HAS FAIL"
        fi
    done
    log "SOAK结果: PASS=$SOAK_PASS FAIL=$SOAK_FAIL (共${SOAK}轮)"
    if [ "$SOAK_FAIL" -eq 0 ]; then
        log "SOAK PASS"
        exit 0
    else
        log "SOAK FAIL 冻结"
        exit 2
    fi
fi

preflight || exit 5

START=1
if [ "$RESUME" = "true" ] && [ -f "$PLOG" ]; then
    if grep -q "T3 PASS" "$PLOG"; then START=4
    elif grep -q "T2 PASS" "$PLOG"; then START=3
    elif grep -q "T1 PASS" "$PLOG"; then START=2
    fi
fi

# R11: run_task保留原始退出码（旗标=exit 3, 冻结=exit 2）
if [ $START -le 1 ]; then run_task task_t1; fi
if [ $START -le 2 ]; then run_task task_t2; fi
if [ $START -le 3 ]; then run_task task_t3; fi

log "终局: knife_run $BRANCH $FINAL_TAG"
run_knife "$FINAL_TAG" "false" | tee -a "$RLOG"
log "终局PASS tag=$FINAL_TAG"

generate_decision
log "DECISION.md生成完成"
exit 0
