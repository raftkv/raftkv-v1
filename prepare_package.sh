#!/bin/bash
# ============================================================
# prepare_package.sh — RaftKV 部署包打包前预处理脚本
#
# 功能:
#   1. 强制将所有 .sh 脚本转换为 LF 换行符 (防止 Windows CRLF 导致 Linux 执行失败)
#   2. 自动检测: 如果转换后仍含 CRLF, 终止打包并报错
#   3. 赋予所有 .sh 脚本可执行权限
#   4. 校验二进制文件存在性
#
# 用法:
#   bash prepare_package.sh [打包目录]
#   默认打包目录: 当前目录
#
# 退出码:
#   0 = 全部通过, 可以打包
#   1 = 检测失败, 禁止打包
# ============================================================

set -euo pipefail

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  RaftKV 部署包打包前预处理${NC}"
echo -e "${GREEN}========================================${NC}"

# 确定打包目录
PKG_DIR="${1:-.}"
echo -e "打包目录: ${PKG_DIR}"

# --- 步骤1: 强制 LF 换行符转换 ---
echo -e "\n${YELLOW}[步骤1] 强制 LF 换行符转换${NC}"

SHELL_SCRIPTS=$(find "$PKG_DIR" -maxdepth 1 -name "*.sh" -type f)
if [ -z "$SHELL_SCRIPTS" ]; then
    echo -e "${YELLOW}  警告: 未找到任何 .sh 脚本${NC}"
else
    for script in $SHELL_SCRIPTS; do
        # 检测是否含 CRLF
        if grep -rl $'\r' "$script" >/dev/null 2>&1; then
            echo -e "  转换: $(basename "$script") (CRLF → LF)"
            sed -i 's/\r$//' "$script"
        else
            echo -e "  跳过: $(basename "$script") (已是 LF)"
        fi
    done
fi

# --- 步骤2: CRLF 残留检测 (门禁) ---
echo -e "\n${YELLOW}[步骤2] CRLF 残留检测 (打包门禁)${NC}"

CRLF_FILES=""
for script in $SHELL_SCRIPTS; do
    if grep -rl $'\r' "$script" >/dev/null 2>&1; then
        CRLF_FILES="$CRLF_FILES $(basename "$script")"
    fi
done

if [ -n "$CRLF_FILES" ]; then
    echo -e "${RED}  ❌ 检测失败: 以下文件仍含 CRLF:${NC}"
    echo -e "${RED}     $CRLF_FILES${NC}"
    echo -e "${RED}  ❌ 禁止打包! 请手动执行 dos2unix 修复${NC}"
    exit 1
else
    echo -e "${GREEN}  ✅ 全部 .sh 脚本均为 LF 格式, 通过门禁${NC}"
fi

# --- 步骤3: 赋予可执行权限 ---
echo -e "\n${YELLOW}[步骤3] 赋予可执行权限${NC}"
for script in $SHELL_SCRIPTS; do
    chmod +x "$script"
    echo -e "  chmod +x $(basename "$script")"
done

# --- 步骤4: 校验关键文件 ---
echo -e "\n${YELLOW}[步骤4] 关键文件校验${NC}"

# 校验二进制
BINARY=$(find "$PKG_DIR" -maxdepth 1 -name "gateway_arm64*" -o -name "raftkv_gateway" -type f | head -1)
if [ -n "$BINARY" ]; then
    BIN_SIZE=$(du -h "$BINARY" | cut -f1)
    echo -e "${GREEN}  ✅ 二进制: $(basename "$BINARY") ($BIN_SIZE)${NC}"
else
    echo -e "${RED}  ❌ 未找到网关二进制文件${NC}"
    exit 1
fi

# 校验 daemon.sh
if [ -f "$PKG_DIR/daemon.sh" ]; then
    echo -e "${GREEN}  ✅ daemon.sh 存在${NC}"
else
    echo -e "${RED}  ❌ daemon.sh 不存在${NC}"
    exit 1
fi

# 校验 config.toml
if [ -f "$PKG_DIR/config.toml" ]; then
    echo -e "${GREEN}  ✅ config.toml 存在${NC}"
else
    echo -e "${YELLOW}  ⚠ config.toml 不存在 (非致命)${NC}"
fi

# --- 步骤5: 校验 manage_node.sh (如果存在) ---
if [ -f "$PKG_DIR/manage_node.sh" ]; then
    echo -e "${GREEN}  ✅ manage_node.sh 存在${NC}"
else
    echo -e "${YELLOW}  ⚠ manage_node.sh 不存在 (非致命)${NC}"
fi

# --- 完成 ---
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}  ✅ 预处理全部通过, 可以打包${NC}"
echo -e "${GREEN}========================================${NC}"

exit 0