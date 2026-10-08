#!/usr/bin/env bash
# scan_internal.sh v3 — 内部标识符全量扫描（硬化版）
#
# v3变更:
#   1. 扫描基线改git grep(仅扫tracked文件), 修复照README构建后产物假红(P1-8)
#   2. 词表新增quorumbench(内部第三方平台名, v25 P1-3盲区)
#   3. 面24正则补ci-gate(原仅ci-knife, v25 P1-3盲区)
#
# v2变更:
#   1. 新增空树断言: git HEAD成功 + 文件数>1000
#   2. 新增文件名扫描面: 文件名本身过词表+模式
#   3. FP过滤修复: 废除行级grep -vE放水 → 改为按文件路径范围豁免
#   4. 词表新增: knife/ci-knife/raftkv_go_engine/D3-前缀/批次号文件名模式
#
# 固化v10/v12/v14二十面+v20新增四面, 每词强制-i (case-insensitive),
# 含衍生形态(TCX罗马数字/DAI JING空格变体/版本号模式).
#
# 用法: bash scripts/scan_internal.sh
# 退出码: 0=全绿, 1=有命中
#
# 历史根因(扫描器四次盲区演化史):
#   v10: 大小写漏检70处 (grep未加-i)
#   v14: 词表缺口漏检17处 (TCX罗马数字/batch模式未入词表)
#   v20: 行级过滤放水+文件名盲区 (grep -vE按行过滤误放水, 文件名从未扫过)
#   v26: 扫描基线grep -r扫工作树→改git grep仅扫tracked文件(构建产物假红)
#
# 假阳性排除说明(按文件路径范围豁免, 非行级):
#   go.mod/go.sum       — Go模块依赖版本号(v0.12.0等), 非内部版本号
#   vendor/             — 第三方代码 (pathspec排除)
#   echarts.min.js      — 第三方minified库, 数字巧合
#   CHANGELOG.md        — 版本历史记录, 允许旧版本号
#   tests/contracts/    — 验收契约基础设施, batch命名在YAML/Python内容中
#   tests/deploy/       — 部署脚本/配置, batch引用在注释中
#   *_test.go           — 测试函数名含batch编号(TestBatch15_*), 已知债
#   raft_storage.go     — 单行历史注释引用batch6根因
#   run_pipeline.sh     — CI流水线引用batch套件名
#   .gitignore          — 含证据目录排除规则

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# ════════════════════════════════════════════════════════════
# 空树断言: HEAD有效 + 文件数>1000
# ════════════════════════════════════════════════════════════
if ! git rev-parse HEAD >/dev/null 2>&1; then
  echo "❌ 空树断言失败: git rev-parse HEAD 失败 (非git仓库?)"
  exit 1
fi
FILE_COUNT=$(git ls-files | wc -l)
if [ "$FILE_COUNT" -lt 1000 ]; then
  echo "❌ 空树断言失败: 文件数 ${FILE_COUNT} < 1000 (空树或子集?)"
  exit 1
fi
echo "空树断言: HEAD=$(git rev-parse --short HEAD) files=${FILE_COUNT} ✓"
echo ""

# 排除目录/文件 (第三方代码/依赖/审计证据)
# v3: 改用git grep, 仅扫tracked文件, 不再需要--exclude-dir
# EXCLUDES保留供参考, scan_face改用git grep pathspec排除
EXCLUDES=(
  --exclude-dir=vendor
  --exclude-dir=.git
  --exclude-dir=audit_evidence
  --exclude-dir=node_modules
  --exclude=go.sum
  --exclude=scan_internal.sh
)

# 假阳性豁免: 按文件路径范围 (非行级内容)
# grep输出格式: ./path/to/file:line:content
# 只检查文件路径部分(第一个冒号前), 不检查行内容
FP_VERSION='go\.mod|CHANGELOG|echarts\.min\.js'          # 版本号相关文件
FP_TESTDEBT='tests/contracts/|tests/deploy/|_test\.go|raft_storage\.go|run_pipeline\.sh|\.gitignore'  # 测试基础设施
FP_BATCH='tests/contracts/|tests/deploy/|_test\.go|raft_storage\.go|run_pipeline\.sh|\.gitignore'     # batch内容已知债

PASS=0
FAIL=0

# ════════════════════════════════════════════════════════════
# 内容扫描面 (按文件路径范围豁免)
# ════════════════════════════════════════════════════════════
scan_face() {
  local num="$1"
  local name="$2"
  local pattern="$3"
  local exempt_paths="$4"

  local raw_hits
  # v3: git grep仅扫tracked文件, 修复构建产物假红(P1-8)
  raw_hits=$(git grep -inE "$pattern" -- ':!vendor/' ':!go.sum' ':!scripts/scan_internal.sh' 2>/dev/null || true)

  local hits="$raw_hits"
  if [ -n "$exempt_paths" ]; then
    # 按文件路径范围豁免: 只检查输出中文件路径部分(第一个冒号前)
    # v3: git grep输出无./前缀, 调整正则
    hits=$(echo "$raw_hits" | grep -vE "^([^:]*($exempt_paths))" || true)
  fi

  local count
  count=$(echo "$hits" | grep -c . || true)

  if [ "$count" -eq 0 ]; then
    echo "面${num} ${name}: 0 ✅"
    PASS=$((PASS + 1))
  else
    echo "面${num} ${name}: ${count} ❌"
    echo "$hits" | head -20
    FAIL=$((FAIL + 1))
  fi
}

# ════════════════════════════════════════════════════════════
# 文件名扫描面 (git ls-files 过词表+模式)
# ════════════════════════════════════════════════════════════
scan_filename_face() {
  local num="$1"
  local name="$2"
  local pattern="$3"

  local hits
  hits=$(git ls-files | grep -v "^vendor/" | grep -iE "$pattern" || true)

  local count
  count=$(echo "$hits" | grep -c . || true)

  if [ "$count" -eq 0 ]; then
    echo "面${num} ${name}(filename): 0 ✅"
    PASS=$((PASS + 1))
  else
    echo "面${num} ${name}(filename): ${count} ❌"
    echo "$hits" | head -20
    FAIL=$((FAIL + 1))
  fi
}

echo "======================================"
echo "内部标识符全量扫描 v3 (case-insensitive, git grep基线)"
echo "======================================"
echo ""

# ── 原始十面词表 (v10/v12) ──
scan_face 1 "姜总裁" '姜总裁' ''
scan_face 2 "甲方" '甲方' ''
scan_face 3 "DAI JING 235" 'DAI[ _-]?JING[ _-]?235|daijin235' ''
scan_face 4 "旧版本号v0.5/v2.x" 'v0\.[0-9]|v2\.[0-5]' "$FP_VERSION"
scan_face 5 "S235/0x235DAI" 'S235|0x235DAI|235引擎' ''
scan_face 6 "双轨三总台五级联动" '双轨三总台五级联动分布式管控系统' ''
scan_face 7 "确定性管控中枢" '确定性管控中枢' ''
scan_face 8 "generate_license_tool" 'generate_license_tool' ''
scan_face 9 "7.6s声明" '7\.6s' 'echarts\.min\.js'
scan_face 10 "V0.x/V2.x大写" 'V0\.[0-9]|V2\.[0-5]' "$FP_VERSION"

# ── 扩展词表 (v14新增) ──
scan_face 11 "TCX罗马数字" 'TCX[-−―—–]?[ⅠⅡⅢⅣⅤ12345]' ''
scan_face 12 "内部产品全称" '双轨三总台五级联动|确定性管控' ''
scan_face 13 "D3内部批次" 'D[23][- ]?(BATCH|batch|F[0-9]|SEC)' "$FP_TESTDEBT"
scan_face 14 "R-XX缺陷号" '(^|[^a-zA-Z])R-[0-9]{2}' "$FP_TESTDEBT"
scan_face 15 "晨报/晨批" '晨报|晨批' "$FP_TESTDEBT"
scan_face 16 "战役设计器" '战役[IVX0-9]+' "$FP_TESTDEBT"
scan_face 17 "d3-batchXX路径" 'd3-batch' "$FP_TESTDEBT"
scan_face 18 "batchXX批次号" 'batch[0-9]{2}' "$FP_BATCH|batch_pipeline|batch_sync|batcher\.go|batch=|batch size|batching"
scan_face 19 "kunpeng-evidence" 'kunpeng-evidence' ''
scan_face 20 "post-batch版本" 'post-batch[0-9]' ''

# ── v20新增词表 (四类残留清偿后硬化) ──
scan_face 21 "knife代号" 'knife' ''
scan_face 22 "raftkv_go_engine" 'raftkv_go_engine' ''
scan_face 23 "D3-前缀" 'D3-' "$FP_TESTDEBT"
scan_face 24 "ci-knife/ci-gate" 'ci-knife|ci-gate' ''

# ── v20新增文件名扫描面 ──
scan_filename_face 25 "批次号文件名" 'batch[0-9]'
scan_filename_face 26 "knife文件名" 'knife'
scan_filename_face 27 "D3-文件名" 'D3-'
scan_filename_face 28 "raftkv_go_engine文件名" 'raftkv_go_engine'

# ── v26新增词表 (v25陌生人审计盲区) ──
scan_face 29 "quorumbench" 'quorumbench' ''

echo ""
echo "======================================"
echo "总计: ${PASS}面绿, ${FAIL}面红"
echo "======================================"
echo ""
echo "假阳性排除(按文件路径范围, 非行级):"
echo "  go.mod/go.sum       — Go模块依赖版本号"
echo "  vendor/             — 第三方代码 (--exclude-dir)"
echo "  echarts.min.js      — 第三方minified库"
echo "  CHANGELOG.md        — 版本历史记录"
echo "  tests/contracts/    — 验收契约(batch命名在YAML/Python内容中)"
echo "  tests/deploy/       — 部署脚本/配置"
echo "  *_test.go           — 测试函数名含batch编号(已知债)"
echo "  raft_storage.go     — 单行历史注释"
echo "  run_pipeline.sh     — CI流水线套件名"
echo "  .gitignore          — 证据目录排除规则"

if [ "$FAIL" -gt 0 ]; then
  echo ""
  echo "❌ 扫描未通过, 存在内部标识符残留"
  exit 1
else
  echo ""
  echo "✅ 全绿, 可安全发布"
  exit 0
fi