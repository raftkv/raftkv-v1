# 晨报首屏模板 — 性能指标摘要

> 更新频率：每批次完工后更新
> 数据来源：docs/specs/latency_decomp/decomp_c512_raw.json

## P99 延迟分解摘要（c=512, batch18 埋点实测）

| 构成项 | P50 (μs) | P99 (μs) | P50 占比 | P99 占比 |
|--------|----------|----------|----------|----------|
| **quorum_wait** | 12,436 | **63,465** | 74.4% | **81.3%** |
| queue (排队) | 1,563 | 32,537 | 9.3% | 41.7% |
| fsync_wait | 47 | 18,165 | 0.3% | 23.3% |
| RPC (估算) | ~5,000 | ~25,000 | ~29.9% | ~32.0% |

> **quorum_wait P99 占比最高（81.3%），为尾延迟主攻方向**
> batch19 优化后 quorum_wait P99 已从 63.5ms 降至 29.5ms（-53.4%）

## fsync 合并状态

| 指标 | 值 |
|------|-----|
| 压测窗口时长 | 180s |
| 窗口内 fsync 总次数 | 4,673 |
| fsync 频率 | 25.96/s（≈26/s，几十次/秒，符合预设标准） |
| 合并比 | 437:1（avg 438 entries/fsync） |
| 判定 | INTRINSIC_CONFIRMED |

## 数据来源标注

- 延迟分解：batch18 埋点实测（tests/evidence/d3-batch18/p99_decomp.json）
- fsync 取证：batch20（tests/evidence/d3-batch20/fsync_forensics.json）
- 四构成项补全：docs/specs/latency_decomp/decomp_c512_raw.json