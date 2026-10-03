# 双轨对比基线 — Dual Track Baseline

> 版本: v2.4-batch26
> 日期: 2026-09-13
> 状态: 本平台基线已产出，quorumbench 侧待外部执行

---

## 1. 本平台（本仓库 HEAD = v2.4-post-batch25）

| 验收项 | 判定 | 实测值 | 阈值 | 证据 |
|--------|------|--------|------|------|
| F1 选举收敛 | PASS | 2.6853s | ≤5.0s | d3-batch25/verdict.json |
| F2 拒载率 | PASS | 20.00% | ≤30% | d3-batch25/verdict.json |
| F3 存活率 | PASS | 100.00% | =100% | d3-batch25/verdict.json |
| F4 无脑裂 | PASS | 1 | ≤1 | d3-batch25/verdict.json |
| F5 可回放 | PASS | true | true | d3-batch25/verdict.json |
| E1 稳态选举 | PASS | 1.8233s | ≤2.0s | d3-batch25/verdict.json |
| E4 级联选举 | FAIL | 3.2849s | ≤2.0s | d3-batch25/verdict.json |
| PV1 pre-vote | PASS | 85 | >0 | d3-batch25/verdict.json |
| PV2 term膨胀 | PASS | 5 | ≤5 | d3-batch25/verdict.json |
| LAN 历史数字 | PASS | 存在且非空 | — | d3-batch25/verdict.json |

**本平台 verdict: FAIL**（E4 未达标，待 D-24-1 晨批裁决）

## 2. quorumbench 平台（独立判定）

| 验收项 | 判定 | 实测值 | 备注 |
|--------|------|--------|------|
| — | — | — | **降级为本地双轨模拟** |

> **降级理由**: quorumbench 平台不在本环境可用（授权表预裁决: "quorumbench 不可达 → 降级为本地双轨模拟并申报降级理由"）
> **降级方式**: 使用本仓库 judge_batch23.py + regression_gate.py 作为独立判定轨道，与 batch27 verdict 对比

## 3. 双轨对比（本地双轨模拟）

| 验收项 | batch27 verdict (轨道 A) | regression_gate (轨道 B) | 一致? | 差异说明 |
|--------|--------------------------|-------------------------|-------|---------|
| E1 稳态选举 | PASS (1.8233s ≤ 2.0s) | PASS (REG-1~8 无 E1 线) | ✓ | 轨道 B 无 E1 独立线，引用 batch verdict |
| E4 级联选举 | FAIL (3.5166s > 3.5s) | FAIL (REG-6 E4b ≤3.5s) | ✓ | 两轨一致判 FAIL |
| F4 无脑裂 | PASS (max_leaders=1) | PASS (REG-3 ≤1) | ✓ | 一致 |
| PV1 pre-vote | PASS (rounds=85) | PASS (REG-8 >0) | ✓ | 一致 |

**差异分析**:
- 轨道 A (verdict): 引用 batch23.yaml + regression.yaml 线 ID (batch27 任务一改造)
- 轨道 B (regression_gate): 引用 regression.yaml 线 ID 独立判定
- **差异根因**: 无差异——batch27 任务一将 verdict 改为引用 regression.yaml 线 ID 后，两轨同源
- **改造前差异**: 改造前 verdict 硬编码 E4≤2.0s，regression_gate REG-6 E4b≤3.5s，同一 E4 值在两轨结论不同（MB-008 口径不一致）

## 4. 后续操作

1. ~~在 quorumbench 平台对本仓库 HEAD 跑标准验收~~ → 降级为本地双轨模拟（已完成）
2. ~~将结果填入本文件 §2/§3~~ → 已用本地双轨模拟填充
3. 比对双轨判定一致性 → ✓ 一致（batch27 任务一改造后同源）