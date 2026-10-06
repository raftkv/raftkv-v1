# ROADMAP 晨审批阅包 — v1.0.0 后续'路线图复核

> 版本: v1.0.0-batch29-review
> 日期: 2026-09-14
> 来源: docs/ROADMAP.md (v1.0.0-post-batch26)
> 用途: 晨审逐方向核对现状，补全 quorumbench 侧状态

---

## 方向一：可观测性增强（P0）

| 字段 | ROADMAP 原文 | batch29 核对 |
|------|-------------|--------------|
| 价值 | 高：战役III 需仪表先行 | ✅ 已确认——batch27 任务五预置可观测性端点 |
| 成本 | 中：约 2 批次 | ✅ batch27 已完成 |
| 依赖 | 无 | ✅ 无 |
| quorumbench 侧 | quorumbench 有独立指标采集，本仓库需对齐 | ⚠️ **未对齐**——quorumbench 不可达（L-29-1 挂账），无法核对指标对齐 |
| 优先级 | P0 | ✅ 已完成 |
| **现状** | **已完成**（batch27） | |

## 方向二：网络分区注入/战役III（P1）

| 字段 | ROADMAP 原文 | batch29 核对 |
|------|-------------|--------------|
| 价值 | 高：核心共识协议验证 | ✅ 已确认 |
| 成本 | 高：约 3-4 批次 | ✅ batch28 T2/T3 + batch29 T4/T5 复合场景 |
| 依赖 | 可观测性增强（P0） | ✅ batch27 已完成 |
| quorumbench 侧 | quorumbench 有网络分区测试套件，可作双轨对照 | ⚠️ **未对照**——quorumbench 不可达（L-29-1 挂账） |
| 优先级 | P1 | ✅ 进行中 |
| **现状** | **进行中**——batch28 6场景全PASS + batch29 复合场景 N=3 全PASS | |

### 战役III 进度

| 阶段 | 状态 | 批次 |
|------|------|------|
| T1 SDD 设计 | ✅ 完成 | batch27 |
| T2 网络分区执行器 | ✅ 完成 | batch28 |
| T3 NP 验收判定 | ✅ 完成 | batch28 |
| T4 复合场景首探 | ✅ 完成 | batch29 |
| T5 复合场景验收 | ✅ 完成 | batch29 |
| T6 quorumbench 双轨对照 | ❌ 挂账 | L-29-1 |

## 方向三：回归体系扩充（P2）

| 字段 | ROADMAP 原文 | batch29 核对 |
|------|-------------|--------------|
| 价值 | 中：5→8→9→10 条已扩 | ✅ batch29 REG-10 入线，10 条 |
| 成本 | 低：渐进式 | ✅ 每批追加 1 条 |
| 依赖 | 无 | ✅ |
| quorumbench 侧 | quorumbench 有独立回归门，可双轨校验 | ⚠️ **未校验**——quorumbench 不可达 |
| 优先级 | P2 | ✅ 持续渐进 |
| **现状** | **10 线全绿**（REG-1~10, version 3.2） | |

### 回归门线族

| 线 ID | 名称 | 入线批次 | stat |
|-------|------|---------|------|
| REG-1 | tps_baseline | batch25 | max |
| REG-2 | p99_baseline | batch25 | max |
| REG-3 | no_split_brain | batch25 | max |
| REG-4 | reject_rate | batch25 | max |
| REG-5 | write_survival | batch25 | min |
| REG-6 | cascading_election | batch25 | median |
| REG-7 | disk_full_survival | batch25 | min |
| REG-8 | prevote_effective | batch25 | count |
| REG-9 | partition_safety | batch28 | max |
| REG-10 | partition_recovery_timeliness | batch29 | min |

## 方向四：quorumbench 双轨（P2）

| 字段 | ROADMAP 原文 | batch29 核对 |
|------|-------------|--------------|
| 价值 | 中：独立平台交叉验证 | ✅ 价值确认 |
| 成本 | 低：每批跑一轮 | ⚠️ 实际成本=∞（平台不可达） |
| 依赖 | quorumbench 平台可用性 | ❌ **依赖不满足** |
| quorumbench 侧 | 已有标准验收套件，首份基线 dual_track_baseline.md | ⚠️ 基线已产出但 quorumbench 侧空 |
| 优先级 | P2 | ⚠️ **阻塞**——依赖不满足 |
| **现状** | **阻塞**——L-29-1 挂账转晨审，本地双轨模拟=regression_gate 10线 | |

## 方向五：文档整理（P3）

| 字段 | ROADMAP 原文 | batch29 核对 |
|------|-------------|--------------|
| 价值 | 低 | ✅ |
| 成本 | 低：约 1 批次 | ✅ |
| 依赖 | 无 | ✅ |
| quorumbench 侧 | 不涉及 | ✅ |
| 优先级 | P3 | ✅ 未开始 |
| **现状** | **未开始**——建议 batch31 | |

## 方向六：开源准备（P3）

| 字段 | ROADMAP 原文 | batch29 核对 |
|------|-------------|--------------|
| 价值 | 低（当前阶段） | ✅ |
| 成本 | 中 | ✅ |
| 依赖 | 文档整理 | ✅ |
| quorumbench 侧 | 不涉及 | ✅ |
| 优先级 | P3 | ✅ 未开始 |
| **现状** | **未开始**——建议 batch32+ | |

---

## 晨审批阅结论

| 方向 | 优先级 | 现状 | quorumbench 侧 | 阻塞? |
|------|--------|------|----------------|-------|
| 1. 可观测性增强 | P0 | ✅ 已完成 | ⚠️ 未对齐 | 否（L-29-1 挂账） |
| 2. 网络分区注入 | P1 | ✅ 进行中（T1~)~T5 完成） | ⚠️ 未对照 | 否（L-29-1 挂账） |
| 3. 回归体系扩充 | P2 | ✅ 10 线全绿 | ⚠️ 未校验 | 否（L-29-1 挂账） |
| 4. quorumbench 双轨 | P2 | ❌ 阻塞 | ❌ 不可达 | **是**（L-29-1） |
| 5. 文档整理 | P3 | 未开始 | 不涉及 | 否 |
| 6. 开源准备 | P3 | 未开始 | 不涉及 | 否 |

### 待晨审裁决项

1. **L-29-1**: quorumbench 真平台不可达，双轨复核挂账转晨审——是否接受本地双轨模拟（regression_gate 10线）作为持续替代，或要求外部 quorumbench 平台接入？
2. **战役III T6**: quorumbench 双轨对照阻塞——是否标记战役III T1~T5 完成、T6 挂账？
3. **ROADMAP 更新**: 方向四优先级是否降级或保持 P2（阻塞中）？

### 建议更新

| 方向 | 建议变更 |
|------|---------|
| 2. 网络分区注入 | T1~T5 完成，T6 挂账 L-29-1 |
| 3. 回归体系扩充 | 10 线（REG-10 入线） |
| 4. quorumbench 双轨 | 阻塞，依赖 L-29-1 晨审裁决 |