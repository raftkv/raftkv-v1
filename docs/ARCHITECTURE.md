# 架构 — ARCHITECTURE

> 摘编自既有 evidence，禁止新编造

## 组件

| 组件 | 文件 | 职责 |
|------|------|------|
| Raft 状态机 | raft.go | 选举/日志复制/提交 |
| HTTP 端点 | main.go | /raft/stats, /raft/entry, /raft/election_metrics 等 |
| 类型定义 | types.go | RaftStats, RaftLog, NetworkPartitionMetrics 等 |
| 故障注入 | cmd/chaos_injector/ | chaos_injector 工具 (steady/cascading/disk_full/network_partition) |
| 判定脚本 | tests/contracts/judge_batch23.py | verdict 判定（引用 regression.yaml 线 ID + stat 定义） |
| NP 判定脚本 | tests/contracts/judge_batch28.py | 网络分区 NP-1~5 验收判定 |
| 回归门 | tests/contracts/regression_gate.py | 跨批回归检查 (9 线) |
| 回归契约 | tests/contracts/regression.yaml | 9 线定义 + stat 字段 (median/max/min/count) |

## Raft 配置

- 选举超时: 800-1200ms
- 心跳间隔: 50ms
- pre-vote: 启用
- pipeline: 深度 8, batch=64, window=2ms

## 可观测性端点

| 端点 | 用途 |
|------|------|
| /raft/stats | 节点状态 |
| /raft/entry | 日志采样 |
| /raft/pre_vote | pre-vote 状态 |
| /latency/decomp | 延迟分解 |
| /raft/election_metrics | 选举/心跳计数 |
## 回归门 (9 线)

| 线 ID | 名称 | stat | 阈值 |
|-------|------|------|------|
| REG-1 | tps_baseline | max | ≥8000 |
| REG-2 | p99_baseline | max | ≤50ms |
| REG-3 | no_split_brain | max | ≤1 |
| REG-4 | reject_rate | max | ≤30% |
| REG-5 | write_survival | min | ==100% |
| REG-6 | cascading_election | median | ≤3.5s |
| REG-7 | disk_full_survival | min | ==100% |
| REG-8 | prevote_effective | count | >0 |
| REG-9 | partition_safety | max | ≤1 |

## 网络分区场景

| 场景 | 分区方式 | 预期 |
|------|---------|------|
| np_symmetric_01 | {1,2} vs {3,4,5} | 多数派可用 |
| np_symmetric_02 | {1,2,3} vs {4,5} | 多数派可用 |
| np_asymmetric_01 | {1} vs {2,3,4,5} | 多数派正常 |
| np_bridge_01 | {1,2} - 3 - {4,5} | 无脑裂 |
| np_recovery_01 | 分区→恢复 | 30s 内追平 |
| np_cascading_01 | 分区→恢复→再分区 | term 单调 |