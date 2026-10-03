# RaftKV — 确定性共识引擎

> V2.4 Performance Sandbox
> 日期: 2026-09-13

## 概览

Raft 共识引擎实现，含 pre-vote、pipeline 批处理、group commit、WAL 持久化、SM4 加密。

## 性能

- TPS: 796.7 → 9020 (11.3x, batch11→batch19)
- P99: 50ms (c=128) / 100ms (c=512)
- 详见: docs/reports/performance_campaign_final.md

## 治理

- 审计判例: docs/governance/AUDIT.md (12 判例)
- 失败模式: docs/governance/MISBEHAVIOR.md (8 条)
- 回归门: tests/contracts/regression.yaml (8 线)
- 路线图: docs/ROADMAP.md

## 部署

```bash
docker compose -p deploy5 -f tests/deploy/docker-compose-5node.yml \
  -f tests/deploy/docker-compose-5node-ports.yml \
  -f tests/deploy/docker-compose-5node-batch16.yml \
  --env-file tests/deploy/deploy.env up -d
```

## 关键文件

- `raft.go` — Raft 状态机
- `main.go` — HTTP 端点
- `types.go` — RaftStats 等类型
- `tests/contracts/judge_batch23.py` — 判定脚本（引用 regression.yaml 线 ID）
- `tests/contracts/regression_gate.py` — 回归门检查
- `cmd/chaos_injector/` — 故障注入工具