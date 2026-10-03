# 运维 — OPERATIONS

> 摘编自既有 evidence，禁止新编造

## 集群部署

5 节点 Docker Compose 部署:
- 镜像: raftkv:latest
- 端口: 9001-9005 (HTTP), 9501-9505 (gRPC)
- 网络: deploy5_raft-net

## 健康检查

```bash
curl.exe http://localhost:9001/health/live
curl.exe http://localhost:9001/raft/stats
```

## 故障注入

```bash
external/tools/chaos_injector.exe \
  -cluster-config config.toml \
  -contract tests/contracts/batch21.yaml \
  -evidence-dir tests/evidence/d3-batchXX \
  -scenario-type cascading
```

场景类型: steady | under_load | cascading | disk_full | network_partition | all

## 网络分区注入

```bash
external/tools/chaos_injector.exe \
  --contract tests/contracts/batch23.yaml \
  --evidence-dir tests/evidence/d3-batchXX \
  --cluster-config config.toml \
  --scenario-type network_partition \
  --timebox 30m
```

6 场景: np_symmetric_01/02, np_asymmetric_01, np_bridge_01, np_recovery_01, np_cascading_01

## 回归门

```bash
python tests/contracts/regression_gate.py \
  --regression tests/contracts/regression.yaml \
  --verdict tests/evidence/d3-batchXX/verdict.json
```

9 线全绿方可推进本批门。

## 判定

```bash
python tests/contracts/judge_batch23.py \
  --contract tests/contracts/batch23.yaml \
  --evidence-dir tests/evidence/d3-batchXX \
  --output tests/evidence/d3-batchXX/verdict.json
```

verdict 引用 regression.yaml 线 ID (batch27 改造) + stat 定义 (batch28)。

## NP 验收判定

```bash
python tests/contracts/judge_batch28.py \
  --evidence-dir tests/evidence/d3-batchXX \
  --output tests/evidence/d3-batchXX/np_verdict.json
```

NP-1~5: 无脑裂 / 多数派可用 / 少数派不选举 / 恢复追平 / term 单调

## 红线

- RL-01: protoc 不可用
- RL-02: 证据目录 gitignore
- RL-07: proto 定义不可修改
- RL-11: 构建产物禁入库
- RL-new-1: N=3 中位数 + CV>15% 重测
- RL-new-2: 改线仅限提案制