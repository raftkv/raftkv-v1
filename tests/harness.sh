#!/bin/bash
# harness.sh — CI门禁公共函数库
# 提供集群启停、请求发送、断言、证据落盘等基础能力
# 所有suite脚本通过 source harness.sh 引入

# ── 配置（可通过环境变量覆盖） ──
TESTS_DIR="${TESTS_DIR:-$(cd "$(dirname "$0")" && pwd)}"
REPO_DIR="${REPO_DIR:-$(cd "$TESTS_DIR/.." && pwd)}"
IMAGE_NAME="${IMAGE_NAME:-raftkv:latest-test}"
LICENSE_DIR="${LICENSE_DIR:-/licenses}"
FP_ANCHOR="${FP_ANCHOR:-raftkv-v25-test}"
EVIDENCE_DIR="${EVIDENCE_DIR:-${TESTS_DIR}/evidence}"

# ── 全局状态 ──
RUN_ID="${RUN_ID:-}"
NET_NAME=""
C1=""  # container 1 name
C2=""  # container 2 name
V1=""  # volume 1 name
V2=""  # volume 2 name
SM4_KEY=""

# ── 断言计数器 ──
ASSERT_PASS=0
ASSERT_FAIL=0
SUITE_NAME=""

# ── 辅助：容器/卷/网络命名 ──
_c_name() { echo "n${1}-${RUN_ID}"; }
_v_name() { echo "wal${1}-${RUN_ID}"; }
_n_name() { echo "net-${RUN_ID}"; }

# ── 辅助：提取stats字段 ──
# 用法: extract_stat <stats_string> <field_name>
# 例: extract_stat "id=node-1 state=Leader commit=10" commit  → 10
extract_stat() {
    echo "$1" | tr ' ' '\n' | grep "^${2}=" | cut -d= -f2
}

# ── 辅助：JSON字段提取（无jq依赖） ──
# 用法: json_field <json_string> <field_name>
# 例: json_field '{"index":5,"success":true}' index  → 5
json_field() {
    echo "$1" | sed -n 's/.*"'"$2"'":\([^,}]*\).*/\1/p' | tr -d '"' | tr -d ' '
}

# ════════════════════════════════════════════════════════════
# up_cluster <run_id> [extra_env...]
# 起2节点集群，等Leader出现（超时30s fail）
# extra_env 格式: "KEY=VALUE" 可多个
# ════════════════════════════════════════════════════════════
up_cluster() {
    RUN_ID="$1"
    shift
    local extra_env=("$@")
    
    NET_NAME=$(_n_name)
    C1=$(_c_name 1)
    C2=$(_c_name 2)
    V1=$(_v_name 1)
    V2=$(_v_name 2)
    SM4_KEY=$(openssl rand -hex 16)
    
    docker network create "$NET_NAME" 2>/dev/null
    docker volume create "$V1" 2>/dev/null
    docker volume create "$V2" 2>/dev/null
    
    local env_args=(-e "RAFTKV_FP_ANCHOR=${FP_ANCHOR}"
                    -e "SM4_KEY=${SM4_KEY}"
                    -e "NODE_ID=node-1"
                    -e "GRPC_PORT=9500"
                    -e "HTTP_PORT=9000"
                    -e "HTTP_BIND=0.0.0.0"
                    -e "PEERS=node-2=${C2}:9500")
    for kv in "${extra_env[@]}"; do
        env_args+=(-e "$kv")
    done
    
    docker run -d --name "$C1" --network "$NET_NAME" \
        "${env_args[@]}" \
        -v "${LICENSE_DIR}/node-1.key:/app/license.key:ro" \
        -v "${V1}:/app/wal-data" \
        "$IMAGE_NAME" 2>&1
    
    env_args[6]=-e"PEERS=node-1=${C1}:9500"  # fix: override PEERS for node-2
    # 重建env_args for node-2
    local env_args2=(-e "RAFTKV_FP_ANCHOR=${FP_ANCHOR}"
                     -e "SM4_KEY=${SM4_KEY}"
                     -e "NODE_ID=node-2"
                     -e "GRPC_PORT=9500"
                     -e "HTTP_PORT=9000"
                     -e "HTTP_BIND=0.0.0.0"
                     -e "PEERS=node-1=${C1}:9500")
    for kv in "${extra_env[@]}"; do
        env_args2+=(-e "$kv")
    done
    
    docker run -d --name "$C2" --network "$NET_NAME" \
        "${env_args2[@]}" \
        -v "${LICENSE_DIR}/node-2.key:/app/license.key:ro" \
        -v "${V2}:/app/wal-data" \
        "$IMAGE_NAME" 2>&1
    
    # 等Leader出现（30s超时）
    local i
    for i in $(seq 1 30); do
        local s
        s=$(docker exec "$C1" curl -s http://127.0.0.1:9000/raft/stats 2>/dev/null)
        if echo "$s" | grep -q "state=Leader"; then
            echo "[harness] Leader=${C1} after ${i}s"
            return 0
        fi
        s=$(docker exec "$C2" curl -s http://127.0.0.1:9000/raft/stats 2>/dev/null)
        if echo "$s" | grep -q "state=Leader"; then
            echo "[harness] Leader=${C2} after ${i}s"
            return 0
        fi
        sleep 1
    done
    echo "[harness] FAIL: Leader not elected within 30s"
    return 1
}

# ════════════════════════════════════════════════════════════
# down_cluster [run_id]
# 清容器/卷/网络
# ════════════════════════════════════════════════════════════
down_cluster() {
    local rid="${1:-$RUN_ID}"
    if [ -z "$rid" ]; then return 0; fi
    docker rm -f $(_c_name 1 "$rid") $(_c_name 2 "$rid") 2>/dev/null || true
    docker volume rm $(_v_name 1 "$rid") $(_v_name 2 "$rid") 2>/dev/null || true
    docker network rm $(_n_name "$rid") 2>/dev/null || true
}

# ── 单节点控制 ──
stop_node() { docker stop "$(_c_name "$1")" 2>/dev/null; }
start_node() { docker start "$(_c_name "$1")" 2>/dev/null; }
restart_node() { docker restart "$(_c_name "$1")" 2>/dev/null; }
rm_node() { docker rm -f "$(_c_name "$1")" 2>/dev/null; }

# ════════════════════════════════════════════════════════════
# propose <node_num> [idem_token] <data>
# 返回JSON响应
# ════════════════════════════════════════════════════════════
propose() {
    local node_num="$1"
    shift
    local container=$(_c_name "$node_num")
    local token="" data=""
    
    # 判断是否有token参数：如果剩余2个参数，第一个是token
    if [ "$#" -eq 2 ]; then
        token="$1"; data="$2"
    else
        data="$1"
    fi
    
    if [ -n "$token" ]; then
        docker exec "$container" curl -s -X POST \
            "http://127.0.0.1:9000/raft/propose?idem_token=${token}" -d "$data"
    else
        docker exec "$container" curl -s -X POST \
            "http://127.0.0.1:9000/raft/propose" -d "$data"
    fi
}

# ════════════════════════════════════════════════════════════
# stats <node_num>
# 返回stats原文: id=... state=... commit=N applied=N logs=N ...
# ════════════════════════════════════════════════════════════
stats() {
    docker exec "$(_c_name "$1")" curl -s http://127.0.0.1:9000/raft/stats 2>/dev/null
}

# ── 获取日志条目 ──
get_entry() {
    docker exec "$(_c_name "$1")" curl -s "http://127.0.0.1:9000/raft/get?index=$2" 2>/dev/null
}

# ── 获取idem统计 ──
idem_stats() {
    docker exec "$(_c_name "$1")" curl -s http://127.0.0.1:9000/idem/stats 2>/dev/null
}

# ── 判断哪个节点是Leader ──
get_leader() {
    local s1 s2
    s1=$(stats 1)
    if echo "$s1" | grep -q "state=Leader"; then echo 1; return; fi
    s2=$(stats 2)
    if echo "$s2" | grep -q "state=Leader"; then echo 2; return; fi
    echo 0
}

# ── 获取follower编号 ──
get_follower() {
    local leader
    leader=$(get_leader)
    if [ "$leader" = "1" ]; then echo 2; return; fi
    if [ "$leader" = "2" ]; then echo 1; return; fi
    echo 0
}

# ════════════════════════════════════════════════════════════
# assert_eq <actual> <expected> <assert_name>
# 数字断言：实际==期望 → PASS，否则FAIL
# ════════════════════════════════════════════════════════════
assert_eq() {
    local actual="$1" expected="$2" name="$3"
    if [ "$actual" = "$expected" ]; then
        echo "  [PASS] $name: $actual == $expected"
        ASSERT_PASS=$((ASSERT_PASS + 1))
        return 0
    else
        echo "  [FAIL] $name: actual=$actual expected=$expected"
        ASSERT_FAIL=$((ASSERT_FAIL + 1))
        return 1
    fi
}

# ── assert_ne <actual> <not_expected> <name> ──
assert_ne() {
    local actual="$1" not_expected="$2" name="$3"
    if [ "$actual" != "$not_expected" ]; then
        echo "  [PASS] $name: $actual != $not_expected"
        ASSERT_PASS=$((ASSERT_PASS + 1))
        return 0
    else
        echo "  [FAIL] $name: actual=$actual should != $not_expected"
        ASSERT_FAIL=$((ASSERT_FAIL + 1))
        return 1
    fi
}

# ── assert_contains <haystack> <needle> <name> ──
assert_contains() {
    if echo "$1" | grep -q "$2"; then
        echo "  [PASS] $3"
        ASSERT_PASS=$((ASSERT_PASS + 1))
        return 0
    else
        echo "  [FAIL] $3: missing '$2' in '$1'"
        ASSERT_FAIL=$((ASSERT_FAIL + 1))
        return 1
    fi
}

# ════════════════════════════════════════════════════════════
# log_evidence <filename>
# 从stdin读取，落盘到 evidence/<run_id>/<filename>
# ════════════════════════════════════════════════════════════
log_evidence() {
    local dir="${EVIDENCE_DIR}/${RUN_ID}"
    mkdir -p "$dir"
    cat > "${dir}/$1"
}

# ── evidence_path <filename>：返回evidence文件路径 ──
evidence_path() {
    echo "${EVIDENCE_DIR}/${RUN_ID}/$1"
}

# ── save_container_logs <node_num> <filename> ──
save_container_logs() {
    local dir="${EVIDENCE_DIR}/${RUN_ID}"
    mkdir -p "$dir"
    docker logs "$(_c_name "$1")" 2>&1 > "${dir}/$2"
}

# ════════════════════════════════════════════════════════════
# suite_begin <suite_name>
# 初始化套件运行
# ════════════════════════════════════════════════════════════
suite_begin() {
    SUITE_NAME="$1"
    ASSERT_PASS=0
    ASSERT_FAIL=0
    echo "═══════════════════════════════════════════════════"
    echo "  SUITE: $SUITE_NAME"
    echo "═══════════════════════════════════════════════════"
}

# ── suite_end：输出汇总，返回0=全绿 1=有fail ──
suite_end() {
    echo "──────────────────────────────────────────────────"
    echo "  $SUITE_NAME: PASS=$ASSERT_PASS FAIL=$ASSERT_FAIL"
    echo "──────────────────────────────────────────────────"
    if [ "$ASSERT_FAIL" -gt 0 ]; then
        return 1
    fi
    return 0
}

# ── t_begin <test_id> <description> ──
t_begin() {
    echo ""
    echo "▶ $1: $2"
}

# ── 写入N条数据（不带token） ──
write_entries() {
    local node="$1" count="$2" prefix="${3:-entry}"
    local i
    for i in $(seq 1 "$count"); do
        propose "$node" "${prefix}-${i}" > /dev/null 2>&1
    done
}

# ── 等待条件满足 <timeout_sec> <check_cmd>
# check_cmd返回0=满足 ──
wait_for() {
    local timeout="$1"
    shift
    local i
    for i in $(seq 1 "$timeout"); do
        if "$@" 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# ── 检查容器是否在运行 ──
is_running() {
    docker inspect -f '{{.State.Running}}' "$(_c_name "$1")" 2>/dev/null | grep -q true
}

# ── gRPC serving readiness probe ──
# 用法: grpc_serving <container_name>
# 返回0=SERVING, 非0=NOT_SERVING/不可达
# R2: 复用 $PROBE_BIN (tests/health-probe-bin), 无新建构建路径
# R4: 首次调用拷贝探针二进制, 后续仅 docker exec (docker cp 移出轮询循环)
# R1: 探针超时2s, wait_for 20轮 → 最坏60s墙钟/20次尝试
grpc_serving() {
    local container="$1"
    local probe_bin="${TESTS_DIR}/health-probe-bin"
    if ! docker exec "$container" test -x /tmp/health-probe 2>/dev/null; then
        docker cp "$probe_bin" "${container}:/tmp/health-probe" 2>/dev/null || true
        docker exec "$container" chmod +x /tmp/health-probe 2>/dev/null || true
    fi
    docker exec "$container" /tmp/health-probe localhost:9500 >/dev/null 2>&1
}

# ── 检查容器是否已退出 ──
is_exited() {
    local state
    state=$(docker inspect -f '{{.State.Status}}' "$1" 2>/dev/null)
    [ "$state" = "exited" ] || [ "$state" = "" ]
}

# ── 等待容器退出 <container_name> [timeout_sec] ──
# 每秒检查一次，容器exited或missing则返回0，超时返回1
wait_for_exit() {
    local container="$1"
    local timeout="${2:-15}"
    local i
    for i in $(seq 1 "$timeout"); do
        if is_exited "$container"; then
            return 0
        fi
        sleep 1
    done
    return 1
}