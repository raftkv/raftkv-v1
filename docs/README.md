# RaftKV — 确定性共识引擎

> v1.0.0 Performance Sandbox
> 日期: 2026-09-13
> 口径修订: 2026-10-10（TPS/P99 对齐主 README；审计判例数对齐 CONTRIBUTING）

## 概览

Raft 共识引擎实现，含 pre-vote、pipeline 批处理、group commit、WAL 持久化、SM4 加密。

## 性能

以下数值与主 [`README.md`](../README.md#tps-benchmark) 的 TPS Benchmark 表同口径
（5 节点集群，concurrency=128，write ratio=20%）。此前的
"TPS: 796.7 → 9020 (11.3x)" 与 "P99: 50ms/100ms" 属早期沙盒口径，已作废。

- TPS: 841 (pre-Raft baseline) → 10,579 (post-Raft, 30min smoke) → 14,770 (post-Raft+snapshot, 1h soak)
- P99: 3,100ms (pre-Raft) → 67.71ms (post-Raft) → 40.46ms (post-Raft+snapshot)

## 治理

- 审计判例: 内部审计库 26 判例（**未随本仓库发布**；`docs/governance/` 在本仓库中不存在）
- 失败模式: 内部失败模式库 8 条（**未随本仓库发布**）
- 回归门: tests/contracts/regression.yaml (10 线)
- 路线图: docs/ROADMAP.md

## 部署

```bash
docker compose -p deploy5 -f tests/deploy/docker-compose-5node.yml \
  -f tests/deploy/docker-compose-5node-ports.yml \
  -f tests/deploy/docker-compose-5node-tls.yml \
  --env-file tests/deploy/deploy.env up -d
```

## 关键文件

- `raft.go` — Raft 状态机
- `main.go` — HTTP 端点
- `types.go` — RaftStats 等类型
- `tests/contracts/judge_fault3.py` — 判定脚本（引用 regression.yaml 线 ID）
- `tests/contracts/regression_gate.py` — 回归门检查
- `cmd/chaos_injector/` — 故障注入工具