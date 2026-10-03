#!/bin/bash
# make_oldfmt_fixture.sh — 生成旧格式WAL夹具
#
# 用v1.0.0-dev4镜像创建含20条数据的WAL，保存到fixtures/
# 用法: ./tests/make_oldfmt_fixture.sh
#
# 产物:
#   fixtures/oldfmt-wal-1/raftkv.wal.gz  (压缩WAL)
#   fixtures/oldfmt-wal-1/sm4_key.txt            (生成时用的密钥)
#   fixtures/oldfmt-wal-1/entry_count.txt        (条目数=20)

set -euo pipefail

TESTS_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$TESTS_DIR/.." && pwd)"
FIXTURE_DIR="${TESTS_DIR}/fixtures/oldfmt-wal-1"
LICENSE_DIR="${LICENSE_DIR:-/licenses}"
FP_ANCHOR="${FP_ANCHOR:-raftkv-v25-test}"
DEV4_IMAGE="raftkv:latest-fixture"
ENTRY_COUNT=20

mkdir -p "$FIXTURE_DIR"

echo "═══════════════════════════════════════════════════"
echo "  生成旧格式WAL夹具 (v1.0.0-dev4, ${ENTRY_COUNT}条)"
echo "═══════════════════════════════════════════════════"

cd "$REPO_DIR"

# 1. checkout dev4 并构建
echo "[1/5] checkout v1.0.0-dev4..."
git stash 2>/dev/null || true
git checkout v1.0.0-dev4 2>&1

echo "[2/5] build dev4 image..."
docker build --platform linux/amd64 -t "$DEV4_IMAGE" -f Dockerfile . 2>&1 | tail -5

# 2. 生成随机SM4密钥
SM4_KEY=$(openssl rand -hex 16)
echo "[3/5] SM4_KEY=$SM4_KEY"

# 3. 启动容器写数据
echo "[4/5] write ${ENTRY_COUNT} entries..."
FIX_NET="net-fixture-$$"
FIX_VOL="vol-fixture-$$"
FIX_C="c-fixture-$$"

docker network create "$FIX_NET" 2>/dev/null
docker volume create "$FIX_VOL" 2>/dev/null

docker run -d --name "$FIX_C" --network "$FIX_NET" \
    -e RAFTKV_FP_ANCHOR="$FP_ANCHOR" -e SM4_KEY="$SM4_KEY" \
    -e NODE_ID=node-1 -e GRPC_PORT=9500 -e HTTP_PORT=9000 -e HTTP_BIND=0.0.0.0 \
    -v "${LICENSE_DIR}/node-1.key:/app/license.key:ro" \
    -v "${FIX_VOL}:/app/wal-data" \
    "$DEV4_IMAGE" 2>&1

sleep 10

# 写入ENTRY_COUNT条数据
for i in $(seq 1 "$ENTRY_COUNT"); do
    docker exec "$FIX_C" curl -s -X POST \
        "http://127.0.0.1:9000/raft/propose" -d "fixture-entry-${i}" > /dev/null 2>&1
done
sleep 2

# 验证写入成功
stats=$(docker exec "$FIX_C" curl -s http://127.0.0.1:9000/raft/stats 2>/dev/null)
commit=$(echo "$stats" | tr ' ' '\n' | grep '^commit=' | cut -d= -f2)
echo "  commit=${commit} (expected=${ENTRY_COUNT})"

if [ "$commit" != "$ENTRY_COUNT" ]; then
    echo "  ERROR: commit mismatch!"
    docker rm -f "$FIX_C" 2>/dev/null
    docker volume rm "$FIX_VOL" 2>/dev/null
    docker network rm "$FIX_NET" 2>/dev/null
    exit 1
fi

# 4. 导出WAL并压缩
echo "[5/5] export WAL..."
docker run --rm -v "${FIX_VOL}:/app/wal-data" -v "${FIXTURE_DIR}:/out" alpine:3.21 \
    sh -c 'gzip -c /app/wal-data/raftkv.wal > /out/raftkv.wal.gz'

echo "$SM4_KEY" > "${FIXTURE_DIR}/sm4_key.txt"
echo "$ENTRY_COUNT" > "${FIXTURE_DIR}/entry_count.txt"

# 清理
docker rm -f "$FIX_C" 2>/dev/null
docker volume rm "$FIX_VOL" 2>/dev/null
docker network rm "$FIX_NET" 2>/dev/null

# 5. 恢复原始分支
echo "[cleanup] restore branch..."
git checkout - 2>&1 || true
git stash pop 2>/dev/null || true

# 验证产物
WAL_GZ_SIZE=$(wc -c < "${FIXTURE_DIR}/raftkv.wal.gz")
echo ""
echo "═══════════════════════════════════════════════════"
echo "  夹具生成完成"
echo "  ${FIXTURE_DIR}/raftkv.wal.gz  (${WAL_GZ_SIZE} bytes)"
echo "  ${FIXTURE_DIR}/sm4_key.txt"
echo "  ${FIXTURE_DIR}/entry_count.txt (${ENTRY_COUNT})"
echo "═══════════════════════════════════════════════════"