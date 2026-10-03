// =========================================================================
// RaftKV Module01 — 日志复制子系统接口契约（LogReplicator）
//
// 定义日志复制子系统的抽象契约，RaftNode 通过方法集隐式实现本接口。
// 与 design.md 2.2.2 接口清单一致（chain2-campaign4-design-v1）。
//
// 设计约束：
//   - Propose 唯一入口，Follower 收到 Propose 返回 NotLeader
//   - WAL 先行持久化（复制前 fsync）
//   - AppendEntries 一致性检查 + 快速回退（ConflictIndex，O(1)）
//   - commit 推进遵守 Figure 8 约束（仅提交当前 term 日志）
//   - Pipeline 提交重叠 quorum 等待
// =========================================================================

package raft

// LogReplicator 日志复制子系统接口契约。
//
// 职责：Propose 唯一入口、WAL 先行、AppendEntries 一致性检查、快速回退、
// commit 推进（Figure 8）、Pipeline 提交、Follower 日志截断与 commitIdx 同步。
type LogReplicator interface {
	// Propose 提交状态机命令到集群，仅在 Leader 节点可用。
	// 前置条件：节点状态为 Leader，WAL 门禁正常。
	// 后置条件：日志追加到内存日志 + WAL 持久化，quorum 复制后提交。
	Propose(command []byte) (index int64, err error)

	// HandleAppendEntries 处理 Leader 的日志复制请求（含心跳）。
	// 校验 term → 一致性检查 → 追加/截断日志 → 推进 commitIdx → 快速回退。
	// 异常：term 倒退返回 success=false；prevLogIndex 不匹配返回 success=false + ConflictIndex。
	HandleAppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error)

	// advanceCommit 推进 commitIdx 至首个达 quorum 且为当前 term 的日志索引。
	// Figure 8 约束：仅当 logs[N].Term == term 时直接提交，旧 term 日志间接提交。
	advanceCommit(term int64)

	// sendHeartbeats Leader 向所有 peers 发送心跳/日志复制，流水线并行发送、
	// 异步收集 matchIdx，重叠 quorum 等待时间。
	sendHeartbeats()
}
