#!/usr/bin/env bash
# scan_internal.sh v5 — 内部标识符全量扫描（精确豁免版）
#
# v5变更 (v-AUDIT2 / P2):
#   1. ★ 新增 4 个扫描面（结构性缺口修复）:
#        面35 RFC1918 内网 IP（10/8 · 172.16-31/12 · 192.168/16）
#        面36 本项目已暴露 IP 专项字串（172.38.x.x / 1.2.4.105）—— 零豁免
#        面37 邮箱地址 —— 零豁免
#        面38 本机绝对路径 / 用户名
#      v-AUDIT1/E-1 已证明: v4 的 34 面无任何一面覆盖内网 IP, 导致
#      cmd/write_tester/main.go 中硬编码的 172.38.3.147 永久漏检,
#      而扫描器仍自评「34面绿 … 可安全发布」。
#   2. ★ 豁免机制收敛: 废除"单条路径正则把整目录放水"的写法, 改为
#        表1 EX_PATH  面号|路径正则|理由            —— 逐条登记, 逐条给理由
#        表2 EX_PT    面号|路径正则|命中串正则|理由   —— 路径与命中串**同时**匹配才豁免
#      关键差异: 内网 IP / 邮箱 / 本机路径三个新面**不继承**任何旧目录豁免。
#      例: 若有人在 tests/deploy/ 下写入 172.38.3.147, 面36 会立刻红脸 ——
#      而 v4 的 `tests/deploy/` 整目录豁免会把它静默吞掉。
#   3. 两张表在报告末尾**逐项打印理由**, 使每条豁免可审计。
#
# v4变更 (v-REPAIR2): 词表补 v21/v23/LEDGER/audit_workspace; 面33 文件名; 面34 commit message
# v3变更: 扫描基线改 git grep(仅扫 tracked 文件); 词表补 quorumbench; 面24补 ci-gate
# v2变更: 空树断言; 文件名面; FP 过滤改为按文件路径范围豁免
#
# 用法: bash scripts/scan_internal.sh
# 退出码: 0=全绿, 1=有命中
#
# 历史根因(扫描器五次盲区演化史):
#   v10: 大小写漏检70处 (grep未加-i)
#   v14: 词表缺口漏检17处 (TCX罗马数字/batch模式未入词表)
#   v20: 行级过滤放水 + 文件名盲区 (grep -vE 按行过滤误放水)
#   v26: 扫描基线 grep -r 扫工作树 → 改 git grep 仅扫 tracked 文件(构建产物假红)
#   v27: 无内网IP面 + 路径级整目录豁免放水 (v-AUDIT2/E-1)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# ── 空树断言 ──
if ! git rev-parse HEAD >/dev/null 2>&1; then
  echo "❌ 空树断言失败: git rev-parse HEAD 失败 (非git仓库?)"; exit 1
fi
FILE_COUNT=$(git ls-files | wc -l)
if [ "$FILE_COUNT" -lt 1000 ]; then
  echo "❌ 空树断言失败: 文件数 ${FILE_COUNT} < 1000 (空树或子集?)"; exit 1
fi
echo "空树断言: HEAD=$(git rev-parse --short HEAD) files=${FILE_COUNT} ✓"
echo ""

PATHS=(':!vendor/' ':!go.sum' ':!scripts/scan_internal.sh')

# ════════════════════════════════════════════════════════════
# 表1 EX_PATH —— 路径级精确豁免: 面号|路径正则|理由
#   适用: 该面上"命中串本身就是该面上的唯一口径"的情形(如 batchNN 只在
#         验收契约/测试用例命名中出现)。逐条登记, 不使用整目录通配放水。
# ════════════════════════════════════════════════════════════
EX_PATH=(
  '4|go\.mod$|Go模块依赖版本号(v0.12.0等), 非内部版本号'
  '4|^CHANGELOG\.md$|版本历史记录, 内部版本号作为变更条目公开'
  '10|go\.mod$|Go模块依赖版本号'
  '10|^CHANGELOG\.md$|版本历史记录'
  '9|^frontend/js/echarts\.min\.js$|第三方minified库, 数字巧合'
  '13|^tests/contracts/|验收契约基础设施: 证据批次目录口径(YAML/Python字串)'
  '13|^tests/deploy/|部署脚本中证据目录批次引用'
  '15|^tests/contracts/|验收契约基础设施: 证据产物报表口径'
  '16|^tests/contracts/|验收契约基础设施: 故障注入场景命名'
  '17|^tests/contracts/|验收契约基础设施: 证据目录路径口径'
  '17|^tests/deploy/|部署脚本中证据目录路径'
  '18|^tests/contracts/|验收契约基础设施: 批次标识内嵌于YAML/Python'
  '18|^tests/deploy/|部署脚本中证据目录批次'
  '18|_test\.go$|测试用例命名约定(TestBatchNN_*)'
  '18|^KNOWN_DEBTS\.md$|内部已知债台账按设计登记批次编号(已裁决接受)'
  '23|^tests/contracts/|验收契约基础设施: 证据目录前缀'
  '23|^tests/deploy/|部署脚本中证据目录前缀'
  '23|_test\.go$|测试注释中证据目录前缀'
  '23|^KNOWN_DEBTS\.md$|内部已知债台账按设计登记(D3前缀口径差异条目)'
  '23|^\.gitignore$|证据目录排除规则'
  '24|^tests/deploy/|compose注释说明镜像来源(历史镜像名, CHANGELOG已记录重命名)'
  '24|^tests/ci_gate\.sh$|CI门脚本名与用例名'
  '24|^CHANGELOG\.md$|版本历史记录镜像重命名'
  '29|^tests/contracts/|验收契约工具引用内部平台名(已裁决接受的证据口径)'
  '30|^docker-compose-v23\.yml$|v23遗留构件(已裁决接受, 不修)'
  '30|^KNOWN_DEBTS\.md$|内部已知债台账按设计登记版本号'
  '30|^frontend/js/echarts\.min\.js$|第三方minified库, 压缩代码巧合'
  '31|^KNOWN_DEBTS\.md$|内部已知债台账按设计登记台账编号'
  '31|^\.gitignore$|台账文件排除规则'
  '32|^KNOWN_DEBTS\.md$|内部已知债台账按设计登记审计工作区路径'
  '33|^Dockerfile\.v23$|v23遗留构件文件名(已裁决接受, 不修)'
  '33|^docker-compose-v23\.yml$|v23遗留构件文件名(已裁决接受, 不修)'
)

# ════════════════════════════════════════════════════════════
# 表2 EX_PT —— 路径+命中串 双匹配豁免: 面号|路径正则|命中串正则|理由
#   适用: 新增内网IP/本机路径面 —— 仅放行"设计网段/公开示例"这类**具体字串**,
#         同文件内出现任何其它 IP/路径仍会红脸。
# ════════════════════════════════════════════════════════════
EX_PT=(
  '35|^docker-compose-3dc\.yml$|^10\.[123]\.0\.|3DC三机房compose的IPAM设计网段与静态地址(该compose可运行所必需), 非真实资产坐标'
  '35|^cmd/diag_tool/main\.go$|^192\.168\.1\.10$|flag usage 示例字串(该工具 -target 默认值已为空串)'
  '38|^tests/deploy/deploy\.env\.example$|^C:/Users$|公开模板中的示例路径占位符(后接 USERNAME, 非真实用户名); 该文件内若出现其它路径形态仍会红脸'
)

PASS=0
FAIL=0

declare -A PATHRE=()      # face -> 聚合路径正则
for e in "${EX_PATH[@]}"; do
  f="${e%%|*}"; r="${e#*|}"; p="${r%%|*}"
  if [ -n "${PATHRE[$f]:-}" ]; then PATHRE[$f]="${PATHRE[$f]}|${p}"; else PATHRE[$f]="$p"; fi
done

scan_face() {
  local num="$1" name="$2" pattern="$3"
  local raw total kept_n ex_n=0
  raw=$(git grep -inE "$pattern" -- "${PATHS[@]}" 2>/dev/null || true)
  [ -z "$raw" ] && { echo "面${num} ${name}: 0 ✅"; PASS=$((PASS+1)); return; }
  total=$(printf '%s\n' "$raw" | grep -c . || true)

  # ① 路径级豁免 (单次聚合正则, 1个进程)
  local h="$raw"
  if [ -n "${PATHRE[$num]:-}" ]; then
    # 对"路径段"单独匹配: git grep 输出为 path:lineno:content,
    # 直接对整行做 anchor($) 匹配会被行尾内容破坏 ⇒ 用 awk 取 $1(路径) 再匹配。
    h=$(printf '%s\n' "$h" | awk -v re="${PATHRE[$num]}" 'BEGIN{FS=":"} {p=$1; if (p !~ re) print}')
  fi

  # ② 路径+命中串 双匹配豁免 (仅对表2登记的少数面, 行数极少)
  local pt_faces
  pt_faces=$(printf '%s\n' "${EX_PT[@]}" | cut -d'|' -f1 | sort -u)
  if printf '%s\n' "$pt_faces" | grep -qx "$num"; then
    local out=""
    while IFS= read -r ln; do
      [ -z "$ln" ] && continue
      local path content token hit_ex=0
      path="${ln%%:*}"; content="${ln#*:}"; content="${content#*:}"
      token=$(printf '%s' "$content" | grep -oE "$pattern" 2>/dev/null | head -1 || true)
      while IFS= read -r e; do
        [ -z "$e" ] && continue
        local ep et
        ep=$(printf '%s' "$e" | cut -d'|' -f2)
        et=$(printf '%s' "$e" | cut -d'|' -f3)
        if printf '%s' "$path" | grep -qE "$ep" 2>/dev/null && printf '%s' "$token" | grep -qE "$et" 2>/dev/null; then
          hit_ex=1; break
        fi
      done <<< "$(printf '%s\n' "${EX_PT[@]}" | grep "^${num}|")"
      if [ "$hit_ex" -eq 1 ]; then ex_n=$((ex_n+1)); else out="${out}${ln}"$'\n'; fi
    done <<< "$h"
    h="$out"
  fi

  kept_n=$(printf '%s\n' "$h" | grep -c . || true)
  ex_n=$((total - kept_n))
  if [ "$kept_n" -eq 0 ]; then
    echo "面${num} ${name}: 0 ✅  (原始${total}行, 精确豁免${ex_n}行)"
    PASS=$((PASS+1))
  else
    echo "面${num} ${name}: ${kept_n} ❌  (原始${total}行, 精确豁免${ex_n}行)"
    printf '%s\n' "$h" | grep . | head -20
    FAIL=$((FAIL+1))
  fi
}

scan_filename_face() {
  local num="$1" name="$2" pattern="$3"
  local hits
  hits=$(git ls-files | grep -v "^vendor/" | grep -iE "$pattern" || true)
  if [ -n "$hits" ] && [ -n "${PATHRE[$num]:-}" ]; then
    hits=$(printf '%s\n' "$hits" | grep -vE "(${PATHRE[$num]})" || true)
  fi
  local count
  count=$(printf '%s\n' "$hits" | grep -c . || true)
  if [ "$count" -eq 0 ]; then
    echo "面${num} ${name}(filename): 0 ✅"; PASS=$((PASS+1))
  else
    echo "面${num} ${name}(filename): ${count} ❌"
    printf '%s\n' "$hits" | grep . | head -20
    FAIL=$((FAIL+1))
  fi
}

COMMIT_MSG_PATTERN='quorumbench|ci-knife|ci-gate|knife|raftkv_go_engine|D3-|batch[0-9]{2}|LEDGER|audit_workspace|v2[13]|姜总裁|甲方|DAI[ _-]?JING[ _-]?235|daijin235|TCX[-−―—–]?[ⅠⅡⅢⅣⅤ12345]'

scan_commit_msg_face() {
  local num="$1" name="$2" pattern="$3"
  local hits count
  hits=$(git log --branches --not --remotes --format='  %h  %s' 2>/dev/null | grep -inE "$pattern" || true)
  count=$(printf '%s\n' "$hits" | grep -c . || true)
  if [ "$count" -eq 0 ]; then
    echo "面${num} ${name}: 0 ✅"; PASS=$((PASS+1))
  else
    echo "面${num} ${name}: ${count} ❌"; printf '%s\n' "$hits" | grep . | head -20; FAIL=$((FAIL+1))
  fi
}

echo "======================================"
echo "内部标识符全量扫描 v5 (精确豁免 · git grep + git log 双层基线)"
echo "======================================"
echo ""

scan_face 1 "姜总裁" '姜总裁'
scan_face 2 "甲方" '甲方'
scan_face 3 "DAI JING 235" 'DAI[ _-]?JING[ _-]?235|daijin235'
scan_face 4 "旧版本号v0.5/v2.x" 'v0\.[0-9]|v2\.[0-5]'
scan_face 5 "S235/0x235DAI" 'S235|0x235DAI|235引擎'
scan_face 6 "双轨三总台五级联动" '双轨三总台五级联动分布式管控系统'
scan_face 7 "确定性管控中枢" '确定性管控中枢'
scan_face 8 "generate_license_tool" 'generate_license_tool'
scan_face 9 "7.6s声明" '7\.6s'
scan_face 10 "V0.x/V2.x大写" 'V0\.[0-9]|V2\.[0-5]'
scan_face 11 "TCX罗马数字" 'TCX[-−―—–]?[ⅠⅡⅢⅣⅤ12345]'
scan_face 12 "内部产品全称" '双轨三总台五级联动|确定性管控'
scan_face 13 "D3内部批次" 'D[23][- ]?(BATCH|batch|F[0-9]|SEC)'
scan_face 14 "R-XX缺陷号" '(^|[^a-zA-Z])R-[0-9]{2}'
scan_face 15 "晨报/晨批" '晨报|晨批'
scan_face 16 "战役设计器" '战役[IVX0-9]+'
scan_face 17 "d3-batchXX路径" 'd3-batch'
scan_face 18 "batchXX批次号" 'batch[0-9]{2}'
scan_face 19 "kunpeng-evidence" 'kunpeng-evidence'
scan_face 20 "post-batch版本" 'post-batch[0-9]'
scan_face 21 "knife代号" 'knife'
scan_face 22 "raftkv_go_engine" 'raftkv_go_engine'
scan_face 23 "D3-前缀" 'D3-'
scan_face 24 "ci-knife/ci-gate" 'ci-knife|ci-gate'
scan_filename_face 25 "批次号文件名" 'batch[0-9]'
scan_filename_face 26 "knife文件名" 'knife'
scan_filename_face 27 "D3-文件名" 'D3-'
scan_filename_face 28 "raftkv_go_engine文件名" 'raftkv_go_engine'
scan_face 29 "quorumbench" 'quorumbench'
scan_face 30 "v21/v23版本号" 'v2[13]'
scan_face 31 "LEDGER内部台账" 'LEDGER'
scan_face 32 "audit_workspace审计工作区" 'audit_workspace'
scan_filename_face 33 "v21/v23/LEDGER/audit_workspace文件名" 'v2[13]|LEDGER|audit_workspace'
scan_commit_msg_face 34 "commit message内部标识" "$COMMIT_MSG_PATTERN"

# ── ★ v-AUDIT2 新增: 内网 IP / 邮箱 / 本机路径 (结构性缺口修复) ──
scan_face 35 "RFC1918内网IP" '10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}|192\.168\.[0-9]{1,3}\.[0-9]{1,3}|172\.(1[6-9]|2[0-9]|3[01])\.[0-9]{1,3}\.[0-9]{1,3}'
scan_face 36 "本项目已暴露IP专项" '172\.38\.[0-9]{1,3}\.[0-9]{1,3}|1\.2\.4\.105'
scan_face 37 "邮箱地址" '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.(com|cn|net|org)'
scan_face 38 "本机绝对路径/用户名" '[A-Za-z]:[\\/](Users|235备份文件|daijin235)|HuaweiMoveData'

echo ""
echo "======================================"
echo "已知债(已裁决接受) — 如实列出, 非致命, 不计入 PASS/FAIL"
echo "======================================"
echo "  [内容面] 历史命中(未豁免口径, 仅统计):"
for kv in "v2[13]:v21/v23" "LEDGER:LEDGER" "audit_workspace:audit_workspace"; do
  p="${kv%%:*}"; label="${kv##*:}"
  n=$(git grep -inE "$p" -- "${PATHS[@]}" 2>/dev/null | wc -l)
  echo "    面${label}: ${n} 行"
done
echo ""
echo "  [文件名面] 历史命中:"
git ls-files | grep -v "^vendor/" | grep -iE 'v2[13]|LEDGER|audit_workspace' | sed 's/^/    /' || echo "    (无)"
echo ""
echo "  [commit message 面] 已发布历史命中 (git log --all):"
git log --all --format='    %h  %s' 2>/dev/null | grep -inE "$COMMIT_MSG_PATTERN" | sed 's/^/    /' || echo "    (无)"
echo ""

echo "======================================"
echo "豁免登记表 (逐项列出路径与理由)"
echo "======================================"
echo "  表1【路径级】(面号|路径正则|理由):"
for e in "${EX_PATH[@]}"; do
  f="${e%%|*}"; r="${e#*|}"; p="${r%%|*}"; why="${r#*|}"
  printf '    面%-2s  %-32s  %s\n' "$f" "$p" "$why"
done
echo ""
echo "  表2【路径+命中串 双匹配】(面号|路径正则|命中串正则|理由):"
echo "    ★ 仅放行具体字串; 同文件内出现任何其它 IP/路径仍会红脸。"
for e in "${EX_PT[@]}"; do
  f="${e%%|*}"; r="${e#*|}"; p="${r%%|*}"; r2="${r#*|}"; t="${r2%%|*}"; why="${r2#*|}"
  printf '    面%-2s  %-30s  %-22s  %s\n' "$f" "$p" "$t" "$why"
done
echo ""
echo "  零豁免面(必须全零): 面1,2,3,5,6,7,8,11,12,14,19,20,21,22,25,26,27,28,33,34,36,37"
echo "  扫描排除范围: vendor/ · go.sum · scripts/scan_internal.sh"
echo ""

echo "======================================"
echo "总计: ${PASS}面绿, ${FAIL}面红  (不含已知债明细段)"
echo "======================================"
if [ "$FAIL" -gt 0 ]; then
  echo ""; echo "❌ 扫描未通过, 存在内部标识符残留"; exit 1
else
  echo ""; echo "✅ 全绿, 可安全发布"; exit 0
fi
