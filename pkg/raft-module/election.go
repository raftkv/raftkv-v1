// =========================================================================
// RaftKV Module01 — 选举子系统接口契约（ElectionManager）
//
// 定义选举子系统的抽象契约，RaftNode 通过方法集隐式实现本接口。
// 与 design.md 2.2.2 接口清单一致（chain2-campaign4-design-v1）。
//
// 设计约束：
//   - 随机化选举超时 [electionTimeoutMin, electionTimeoutMax] = [800ms, 1200ms]
//   - electionTimeoutMin > rpcTimeout（防投票期间触发新选举）
//   - pre-vote 纯探测，无副作用（不递增 term / 不更新 state / votedFor）
// =========================================================================

package raft

// ElectionManager 选举子系统接口契约。
//
// 职责：Follower→Candidate→Leader 状态流转、随机化超时、pre-vote 探测、
// 投票约束、quorum 当选、Leader 初始化与强制降级。
type ElectionManager interface {
	// handleElectionTimeout 处理选举超时信号：Follower 转为 Candidate 并递增 term，
	// 随后发起 pre-vote 探测（获 quorum 预支持后进入正式选举）。
	handleElectionTimeout()

	// preVoteProbe 发起 pre-vote 探测，询问 peers 是否会在正式选举中投票给自己。
	// 纯探测无副作用：不递增 term、不更新 state/votedFor。
	preVoteProbe(term int64, lastLogIdx int64, lastLogTm int64, peers []PeerInfo) bool

	// HandlePreVote 处理候选者的 pre-vote 探测（服务端侧）。
	// 校验日志 up-to-date → 授予或拒绝预支持，纯探测无副作用。
	HandlePreVote(term int64, candidateId string, lastLogIndex int64, lastLogTerm int64) (respTerm int64, granted bool)

	// HandleRequestVote 处理候选者的正式投票请求（服务端侧）。
	// 校验链：term 倒退 → 配置外 → term 飙升拦截 → WAL 门禁 → votedFor 冲突 → logCaughtUp → 日志 up-to-date。
	HandleRequestVote(req *RequestVoteRequest) (*RequestVoteResponse, error)

	// requestVotes 候选者向所有 peers 广播正式投票请求，汇总投票结果，
	// 获 quorum 后转为 Leader（初始化 nextIdx/matchIdx 并立即发心跳）。
	requestVotes(term int64, peers []PeerInfo)
}
