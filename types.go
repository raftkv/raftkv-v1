// =========================================================================
// RaftKV 确定性引擎 — 共享类型定义
// 纯 Go 实现，不依赖 hashicorp/raft
// =========================================================================

package main

import (
	"sync"
)

// NodeState Raft 节点状态枚举
type NodeState int32

const (
	StateFollower  NodeState = 0 // 跟随者
	StateCandidate NodeState = 1 // 候选者
	StateLeader    NodeState = 2 // 领导者
)

func (s NodeState) String() string {
	switch s {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// GatewayConfig 网关启动配置
type GatewayConfig struct {
	ID            string   `json:"id"`
	Peers         []string `json:"peers"`
	ListenAddress string   `json:"listen_address"`
	GossipPort    int      `json:"gossip_port"`
}

// PeerInfo 对等节点连接信息
type PeerInfo struct {
	ID      string // 节点 ID
	Address string // gRPC 地址 (host:port)
}

// RaftLog 内部 Raft 日志条目（纯 Go 实现）
type RaftLog struct {
	Index   int64  // 全局递增索引
	Term    int64  // 任期号
	Command []byte // 状态机命令
	SM3Hash []byte // SM3 国密哈希（链式校验）
}

// VoteRecord 投票记录
type VoteRecord struct {
	Term     int64
	VotedFor string
}

// RaftStats 节点运行时统计（通过 HTTP API 暴露）
type RaftStats struct {
	ID                 string `json:"id"`
	State              string `json:"state"`
	Term               int64  `json:"term"`
	LeaderID           string `json:"leader_id"`
	CommitIndex        int64  `json:"commit_index"`
	LastApplied        int64  `json:"last_applied"`
	LogCount           int    `json:"log_count"`
	PeerCount          int    `json:"peer_count"`
	VotedFor           string `json:"voted_for"`
	ElectionTime       string `json:"election_time"`
	ElectionRoundCount int64  `json:"election_round_count"`
	HeartbeatLostCount int64  `json:"heartbeat_lost_count"`
	PreVoteRoundCount  int64  `json:"prevote_round_count"`
	mu                 sync.RWMutex
}

func (s *RaftStats) RLock()   { s.mu.RLock() }
func (s *RaftStats) RUnlock() { s.mu.RUnlock() }
func (s *RaftStats) Lock()    { s.mu.Lock() }
func (s *RaftStats) Unlock()  { s.mu.Unlock() }

func (s *RaftStats) Update(
	state string, term int64, leaderID string,
	commitIndex, lastApplied int64, logCount, peerCount int,
	votedFor string,
) {
	s.Lock()
	defer s.Unlock()
	s.State = state
	s.Term = term
	s.LeaderID = leaderID
	s.CommitIndex = commitIndex
	s.LastApplied = lastApplied
	s.LogCount = logCount
	s.PeerCount = peerCount
	s.VotedFor = votedFor
}

func (s *RaftStats) Snapshot() map[string]any {
	s.RLock()
	defer s.RUnlock()
	return map[string]any{
		"id":                   s.ID,
		"state":                s.State,
		"term":                 s.Term,
		"leader_id":            s.LeaderID,
		"commit_index":         s.CommitIndex,
		"last_applied":         s.LastApplied,
		"log_count":            s.LogCount,
		"peer_count":           s.PeerCount,
		"voted_for":            s.VotedFor,
		"election_round_count": s.ElectionRoundCount,
		"heartbeat_lost_count": s.HeartbeatLostCount,
		"prevote_round_count":  s.PreVoteRoundCount,
	}
}

// Logger 简明日志接口
type Logger interface {
	Printf(format string, v ...interface{})
}

// =========================================================================
// 快照 RPC 请求/响应结构体（本地定义，HTTP 分片传输，不修改 proto —— RL-07）
// 与 pkg/raft-module/types.go 保持同步（chain2-campaign4-design-v1）
// =========================================================================

// InstallSnapshotRequest 快照安装请求（本地结构体，不修改 proto）
type InstallSnapshotRequest struct {
	Term              int64  `json:"term"`                // Leader 任期
	LeaderId          string `json:"leader_id"`           // Leader ID
	LastIncludedIndex int64  `json:"last_included_index"` // 快照最后包含的日志索引
	LastIncludedTerm  int64  `json:"last_included_term"`  // 快照最后包含的日志任期
	Offset            int64  `json:"offset"`              // 当前分片在快照中的偏移
	Data              []byte `json:"data"`                // 快照分片数据
	Done              bool   `json:"done"`                // 是否最后一片
}

// InstallSnapshotResponse 快照安装响应
type InstallSnapshotResponse struct {
	Term    int64 `json:"term"`    // 响应方当前任期
	Success bool  `json:"success"` // 接收成功
}

// SnapshotChunk 快照分片（Leader 侧分片切割单元）
type SnapshotChunk struct {
	Offset int64  `json:"offset"` // 分片在快照中的偏移
	Data   []byte `json:"data"`   // 分片数据
	Last   bool   `json:"last"`   // 是否最后一片
}
