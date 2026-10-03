#!/bin/bash
# RaftKV V2.2-S mTLS 证书生成脚本（红线2：部署前必须先执行 bash gen_certs.sh）
set -euo pipefail
CERT_DIR="${CERT_DIR:-./certs}"
NODES="${NODES:-node-1 node-2 node-3 node-4 node-5}"
DAYS="${DAYS:-3650}"
mkdir -p "$CERT_DIR"
cd "$CERT_DIR"
if [ ! -f ca-key.pem ]; then
  openssl genrsa -out ca-key.pem 4096 2>/dev/null
  openssl req -new -x509 -key ca-key.pem -out ca-cert.pem -days "$DAYS" -subj "/CN=raft-ca" 2>/dev/null
  echo "✅ CA 证书已生成 (ca-cert.pem / ca-key.pem)"
fi
for node in $NODES; do
  if [ ! -f "${node}-key.pem" ]; then
    openssl genrsa -out "${node}-key.pem" 2048 2>/dev/null
    openssl req -new -key "${node}-key.pem" -out "${node}.csr" -subj "/CN=${node}" 2>/dev/null
    openssl x509 -req -in "${node}.csr" -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out "${node}-cert.pem" -days "$DAYS" 2>/dev/null
    rm -f "${node}.csr"
    echo "✅ ${node} 证书已生成 (${node}-cert.pem / ${node}-key.pem)"
  fi
done
echo "============================================================"
echo "  mTLS 证书目录: $(pwd)"
echo "  节点数: $(echo $NODES | wc -w) | 有效期: ${DAYS} 天"
echo "============================================================"
ls -la