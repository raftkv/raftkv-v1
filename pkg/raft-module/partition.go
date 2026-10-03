// =========================================================================
// RaftKV Module01 — 网络分区行为子系统接口契约（PartitionGuard）
//
// 定义网络分区行为子系统的抽象契约，RaftNode 通过方法集隐式实现本接口。
// 与 design.md 1.2.4 接口清单一致（chain2-campaign4-design-v1）。
//
// 设计约束：
//   - 多数派分区可用，少数派分区不可提交、不脑裂
//   - 分区恢复后检测更高 term 降级、接受日志覆盖、时效 ≤ 10s（REG-10）
//   - 弱网自适应心跳：连续 3 次超时调大、连续 3 次成功调回
// =========================================================================

package raft

import (
	"time"
)

// PartitionGuard 网络分区行为子系统接口契约。
//
// 职责：分区期间强制降级、quorum 检查（多数派可用性）、弱网自适应心跳。
type PartitionGuard interface {
	// stepDown 强制降级为 Follower（发现更高 term 时立即调用）。
	// 更新 term、清空 votedFor，终止 Leader 专用循环。
	stepDown(higherTerm int64)

	// advanceCommit 推进 commitIdx（quorum 检查视角）。
	// 仅当 matchIdx 达 quorum 且日志为当前 term 时推进，
	// 少数派分区的旧 Leader 无法达 quorum → 不推进（不脑裂保证）。
	advanceCommit(term int64)

	// adaptiveHeartbeatAdjust 弱网自适应心跳间隔。
	// 连续 3 次心跳超时 → 调大间隔；连续 3 次成功 → 调回原始间隔。
	adaptiveHeartbeatAdjust() time.Duration
}
