#!/bin/bash
set -e

export MSYS_NO_PATHCONV=1

CERTS_DIR=${1:-./certs}
NODES=${2:-node-1,node-2,node-3,node-4,node-5}

mkdir -p "$CERTS_DIR"

echo "[gen_certs] 生成 CA 证书..."
openssl req -x509 -newkey rsa:2048 -keyout "$CERTS_DIR/ca-key.pem" \
  -out "$CERTS_DIR/ca-cert.pem" -days 3650 -nodes \
  -subj "/CN=raft-ca" 2>/dev/null

IFS=',' read -ra NODE_ARRAY <<< "$NODES"
for node in "${NODE_ARRAY[@]}"; do
  echo "[gen_certs] 生成 $node 证书（含 DNS SAN）..."
  openssl req -newkey rsa:2048 -keyout "$CERTS_DIR/${node}-key.pem" \
    -out "$CERTS_DIR/${node}-csr.pem" -nodes \
    -subj "/CN=${node}" 2>/dev/null

  echo "subjectAltName=DNS:${node},DNS:localhost,IP:127.0.0.1" > "$CERTS_DIR/ext.tmp"
  openssl x509 -req -in "$CERTS_DIR/${node}-csr.pem" \
    -CA "$CERTS_DIR/ca-cert.pem" -CAkey "$CERTS_DIR/ca-key.pem" \
    -CAcreateserial -out "$CERTS_DIR/${node}-cert.pem" -days 3650 \
    -extfile "$CERTS_DIR/ext.tmp" 2>/dev/null
  rm -f "$CERTS_DIR/${node}-csr.pem" "$CERTS_DIR/ext.tmp"
done

rm -f "$CERTS_DIR/ca-cert.srl"

echo "[gen_certs] 证书生成完成:"
ls -la "$CERTS_DIR/"
