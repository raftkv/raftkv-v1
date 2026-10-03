// =========================================================================
// RaftKV Module01 — 快照子系统接口契约（SnapshotManager）
//
// 定义快照子系统的抽象契约，RaftNode 通过方法集隐式实现本接口。
// 与 design.md 2.2.2 接口清单一致（chain2-campaign4-design-v1）。
//
// 设计约束：
//   - 快照 lastIncludedIndex ≤ commitIdx（禁止对未提交索引生成快照）
//   - 分片传输（SnapshotChunk），传输期间不阻塞正常复制（限流）
//   - 压缩后请求 Index < logStartIndex 返回 ErrCompacted
// =========================================================================

package raft

// SnapshotManager 快照子系统接口契约。
//
// 职责：快照触发、日志压缩、InstallSnapshot 触发与分片传输、
// Follower 快照安装、快照后日志追赶、快照限流。
type SnapshotManager interface {
	// HandleInstallSnapshot 处理 Leader 的快照安装请求（服务端侧）。
	// 校验 term → 丢弃 Index ≤ LastIncludedIndex 日志 → 写入分片 →
	// done=true 时应用状态机 → 重置 commitIdx/logStartIndex。
	HandleInstallSnapshot(req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)

	// sendInstallSnapshot 向指定 peer 发送快照（分片 + 限流）。
	// 前置条件：nextIdx[peer] < logStartIndex。
	// 后置条件：分片发送完成 → nextIdx[peer] = lastIncludedIndex + 1 → 切换至 AppendEntries 追赶。
	sendInstallSnapshot(peerID string, snapshotData []byte, lastIncludedIndex int64, lastIncludedTerm int64) error

	// CompactLogs 压缩 upToIndex 及之前的日志条目。
	// 切片截断物理删除（非置 nil），释放内存，logStartIndex 前移。
	CompactLogs(upToIndex int64)

	// ReloadFromSnapshot 从快照数据重载节点状态，重置日志与 commitIdx。
	// 用于节点重启恢复或快照安装后追赶。
	ReloadFromSnapshot(snapshotData []byte, lastIncludedIndex int64, lastIncludedTerm int64) error
}
