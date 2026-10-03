// =========================================================================
// RaftKV Module01 — 纯 Go Raft 强一致性共识引擎
//
// 设计原则：
//   1. 零外部 Raft 库依赖 — 纯 Go + goroutine + channel 实现
//   2. 严格遵循 Raft 论文状态机（Follower → Candidate → Leader）
//   3. 三节点模式，通过 Transport 接口进行 RPC 通信（HTTP/channel 可替换）
//   4. 随机选举超时避免脑裂（150-300ms 范围）
//   5. Leader 心跳间隔 50ms，同时承担日志复制
//
// 本文件从原 raftkv/raft.go 剥离并改造：
//   - gRPC proto (pb.*) → 本地结构体 (RequestVoteRequest 等)
//   - pb.RaftServiceClient → Transport 接口
//   - 新增 Propose / ProposeSync 客户端提案接口
//   - 心跳循环改造为同时复制日志条目（标准 Raft AppendEntries）
// =========================================================================

package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// =========================================================================
// 编译时常量
// =========================================================================

const (
	// 选举超时范围（毫秒）
	electionTimeoutMin = 800  // 最小选举超时（batch34 T016: 150→800，与主模块对齐）
	electionTimeoutMax = 1200 // 最大选举超时（batch34 T016: 300→1200，与主模块对齐）

	// Leader 心跳间隔
	heartbeatIntervalMin = 50 * time.Millisecond
	heartbeatIntervalMax = 500 * time.Millisecond

	// 弱网自适应阈值
	adaptiveTimeoutThreshold = 3
	adaptiveRecoverySuccess  = 3

	// RPC 调用超时
	rpcTimeout = 500 * time.Millisecond

	// 投票请求并发度
	voteRequestConcurrency = 3
)

// 错误定义
var (
	ErrNotLeader      = errors.New("当前节点不是 Leader")
	ErrNodeShutdown   = errors.New("节点已关闭")
	ErrProposeTimeout = errors.New("提案提交超时")
)

// =========================================================================
// RaftNode — 纯 Go Raft 节点
// =========================================================================

type RaftNode struct {
	// --- 持久状态（所有节点）---
	id        string            // 节点唯一 ID
	peers     []PeerInfo        // 集群成员列表（不含自身）
	peerAddrs map[string]string // peerID → RPC address 快速索引

	mu       sync.RWMutex // 保护以下所有字段
	state    NodeState    // Follower / Candidate / Leader
	term     int64        // 当前任期
	votedFor string       // 本任期投票给谁（空=未投票）
	leaderID string       // 当前已知 Leader

	// --- 日志 ---
	logs        []RaftLog // 日志条目（索引从 1 开始，0 为哨兵）
	commitIdx   int64     // 已提交的最高索引
	lastApplied int64     // 已应用到状态机的最高索引

	// --- Leader 专用 ---
	nextIdx  map[string]int64 // peerID → 下一条要发给该 peer 的日志索引
	matchIdx map[string]int64 // peerID → 已知已复制的最高日志索引

	// --- 通信通道 ---
	electionTimer *time.Timer     // 选举超时计时器
	heartbeatStop chan struct{}   // 停止心跳循环
	voteResultCh  chan voteResult // 投票结果汇总通道
	stateChangeCh chan NodeState  // 状态变更通知
	shutdownCh    chan struct{}   // 优雅关闭信号
	shutdownOnce  sync.Once       // 确保只关闭一次

	// --- 传输层 ---
	// 每个 peer 一个 Transport 连接，由外部注入
	transports map[string]Transport

	// --- 统计 ---
	stats  *RaftStats
	logger Logger

	// --- 日志提交回调（管线钩子）---
	// 当 commitIdx 前进时调用，用于接入 WAL 持久化
	onCommit func(RaftLog)

	// 选举开始时间（用于日志）
	electionStart time.Time

	// --- 新节点日志追赶标志 ---
	logCaughtUp bool

	// --- 最近收到 Leader 心跳时间 ---
	lastHeartbeat time.Time

	// --- 弱网自适应心跳 ---
	consecutiveTimeouts      int32
	consecutiveSuccess       int32
	currentHeartbeatInterval time.Duration

	// --- 提案等待通知 ---
	commitCond *sync.Cond // 当 commitIdx 前进时广播，唤醒 ProposeSync 等待者
}

type voteResult struct {
	peerID      string
	term        int64
	voteGranted bool
	err         error
}

// =========================================================================
// 节点构造函数
// =========================================================================

// NewRaftNode 创建 Raft 节点
//
//	id: 节点唯一 ID
//	peerAddrs: 集群成员地址映射 (peerID → address)，不含自身
//	transports: 传输层映射 (peerID → Transport)，不含自身
//	logger: 日志接口（可为 nil）
func NewRaftNode(
	id string,
	peerAddrs map[string]string,
	transports map[string]Transport,
	logger Logger,
) *RaftNode {
	peers := make([]PeerInfo, 0, len(peerAddrs))
	for pid, addr := range peerAddrs {
		peers = append(peers, PeerInfo{ID: pid, Address: addr})
	}

	rn := &RaftNode{
		id:            id,
		peers:         peers,
		peerAddrs:     peerAddrs,
		state:         StateFollower,
		term:          0,
		votedFor:      "",
		leaderID:      "",
		logs:          make([]RaftLog, 0),
		commitIdx:     0,
		lastApplied:   0,
		nextIdx:       make(map[string]int64),
		matchIdx:      make(map[string]int64),
		electionTimer: time.NewTimer(randomElectionTimeout()),
		heartbeatStop: make(chan struct{}),
		voteResultCh:  make(chan voteResult, 256),
		stateChangeCh: make(chan NodeState, 8),
		shutdownCh:    make(chan struct{}),
		transports:    transports,
		stats: &RaftStats{
			ID:        id,
			State:     "Follower",
			Term:      0,
			LeaderID:  "",
			PeerCount: len(peers),
		},
		logger:                   logger,
		consecutiveTimeouts:      0,
		consecutiveSuccess:       0,
		currentHeartbeatInterval: heartbeatIntervalMin,
		logCaughtUp:              true, // 全新集群默认已追上
	}
	rn.commitCond = sync.NewCond(&rn.mu)

	return rn
}

// SetLogCaughtUp 设置日志追赶状态
func (rn *RaftNode) SetLogCaughtUp(caughtUp bool) {
	rn.mu.Lock()
	rn.logCaughtUp = caughtUp
	rn.mu.Unlock()
}

// =========================================================================
// 公共接口
// =========================================================================

func (rn *RaftNode) ID() string        { return rn.id }
func (rn *RaftNode) Stats() *RaftStats { return rn.stats }

func (rn *RaftNode) State() NodeState {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.state
}

func (rn *RaftNode) Term() int64 {
	return atomic.LoadInt64(&rn.term)
}

func (rn *RaftNode) LeaderID() string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.leaderID
}

func (rn *RaftNode) IsLeader() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.state == StateLeader
}

// CommitIndex 返回当前已提交日志索引
func (rn *RaftNode) CommitIndex() int64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.commitIdx
}

// LogCount 返回当前日志条目总数
func (rn *RaftNode) LogCount() int {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return len(rn.logs)
}

// GetCommittedLogs 返回已提交的所有日志（拷贝）
func (rn *RaftNode) GetCommittedLogs() []RaftLog {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	result := make([]RaftLog, rn.commitIdx)
	copy(result, rn.logs[:rn.commitIdx])
	return result
}

// SetOnCommit 设置日志提交回调
func (rn *RaftNode) SetOnCommit(fn func(RaftLog)) {
	rn.mu.Lock()
	rn.onCommit = fn
	rn.mu.Unlock()
}

// SetTransports 注入传输层映射（允许延迟注入，用于服务端先启动再连接的场景）
//
//	transports: peerID → Transport 映射（不含自身）
func (rn *RaftNode) SetTransports(transports map[string]Transport) {
	rn.mu.Lock()
	rn.transports = transports
	rn.mu.Unlock()
}

// collectCommittedLogs 收集从 oldCommit+1 到 commitIdx 的已提交日志
// 调用前必须持有 rn.mu 锁
func (rn *RaftNode) collectCommittedLogs(oldCommit int64) []RaftLog {
	if rn.commitIdx <= oldCommit {
		return nil
	}
	var logs []RaftLog
	for i := oldCommit + 1; i <= rn.commitIdx; i++ {
		if int(i-1) >= 0 && int(i-1) < len(rn.logs) {
			logs = append(logs, rn.logs[i-1])
		}
	}
	return logs
}

// fireOnCommit 触发已提交日志的回调
func (rn *RaftNode) fireOnCommit(logs []RaftLog) {
	if len(logs) == 0 {
		return
	}
	rn.mu.RLock()
	fn := rn.onCommit
	rn.mu.RUnlock()
	if fn == nil {
		return
	}
	for _, log := range logs {
		fn(log)
	}
}

// =========================================================================
// 状态机主循环
// =========================================================================

func (rn *RaftNode) Run() {
	rn.logf("[raft/%s] 启动，初始状态: %s, peers: %v", rn.id, rn.state, rn.peerIDs())
	for {
		select {
		case <-rn.shutdownCh:
			rn.logf("[raft/%s] 收到关闭信号，退出主循环", rn.id)
			return

		case <-rn.electionTimer.C:
			rn.handleElectionTimeout()

		case newState := <-rn.stateChangeCh:
			rn.transitionTo(newState)
		}
	}
}

// =========================================================================
// 选举超时处理 — Follower/Candidate → Candidate
// =========================================================================

func (rn *RaftNode) handleElectionTimeout() {
	rn.mu.Lock()
	if rn.state == StateLeader {
		rn.mu.Unlock()
		return
	}

	// 新节点日志未追上 Leader 时，不发起选举
	if !rn.logCaughtUp && !rn.lastHeartbeat.IsZero() && time.Since(rn.lastHeartbeat) < 10*time.Second {
		rn.mu.Unlock()
		rn.logf("[raft/%s] 日志尚未追上 Leader，跳过选举 (logs=%d)", rn.id, len(rn.logs))
		rn.electionTimer.Reset(randomElectionTimeout() * 3)
		return
	}

	// 进入 Candidate 状态
	rn.state = StateCandidate
	currentTerm := atomic.AddInt64(&rn.term, 1)
	rn.votedFor = rn.id
	rn.leaderID = ""
	rn.electionStart = time.Now()

	peers := make([]PeerInfo, len(rn.peers))
	copy(peers, rn.peers)
	rn.mu.Unlock()

	rn.logf("[raft/%s] 选举超时触发 → Candidate, term=%d", rn.id, currentTerm)

	// 重置选举计时器
	rn.electionTimer.Reset(randomElectionTimeout())

	// 向所有 peer 并发发送 RequestVote
	rn.requestVotes(currentTerm, peers)
}

// =========================================================================
// pre-vote 探测 — 防止日志落后节点干扰集群（batch34 T016: 从主模块迁移）
// =========================================================================

// preVoteProbe 发起 pre-vote 探测，询问 peers 是否会在正式选举中投票给自己。
// 纯探测无副作用：不递增 term、不更新 state/votedFor。
func (rn *RaftNode) preVoteProbe(term int64, lastLogIdx int64, lastLogTm int64, peers []PeerInfo) bool {
	votesNeeded := (len(peers)+1)/2 + 1
	votesGranted := int32(1) // 自己预投自己

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(p PeerInfo) {
			defer func() {
				if r := recover(); r != nil {
					rn.logf("[raft/%s] pre-vote RPC panic recovered: %v", rn.id, r)
				}
			}()
			defer wg.Done()

			tp, ok := rn.transports[p.ID]
			if !ok {
				return
			}

			req := &RequestVoteRequest{
				Term:         term,
				CandidateId:  rn.id,
				LastLogIndex: lastLogIdx,
				LastLogTerm:  lastLogTm,
			}

			resp, err := tp.PreVote(req)
			if err != nil {
				return
			}

			if resp.VoteGranted {
				atomic.AddInt32(&votesGranted, 1)
			}
		}(peer)
	}
	wg.Wait()

	granted := atomic.LoadInt32(&votesGranted) >= int32(votesNeeded)
	rn.logf("[raft/%s] pre-vote 探测: 得票 %d/%d (quorum=%d) → %v",
		rn.id, votesGranted, len(peers)+1, votesNeeded, granted)
	return granted
}

// HandlePreVote 处理候选者的 pre-vote 探测（服务端侧）。
// 校验日志 up-to-date → 授予或拒绝预支持，纯探测无副作用（不更新 term/state/votedFor）。
func (rn *RaftNode) HandlePreVote(term int64, candidateId string, lastLogIndex int64, lastLogTerm int64) (respTerm int64, granted bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	respTerm = rn.term
	granted = false

	// 【检查1】term 倒退拒绝
	if term < rn.term {
		return respTerm, false
	}

	// 注意：跳过 stepDown — pre-vote 不更新 term/state
	// 注意：跳过 votedFor 冲突 — pre-vote 不检查 votedFor

	// 【检查3】logCaughtUp 检查
	if !rn.logCaughtUp && !rn.lastHeartbeat.IsZero() {
		return respTerm, false
	}

	// 【检查4】空日志候选者禁止当选
	if lastLogIndex == 0 && rn.commitIdx > 0 {
		return respTerm, false
	}

	// 【检查5】log up-to-date 检查
	lastIdx := int64(len(rn.logs))
	var localLastTerm int64
	if lastIdx > 0 {
		localLastTerm = rn.logs[lastIdx-1].Term
	}
	if lastLogTerm < localLastTerm ||
		(lastLogTerm == localLastTerm && lastLogIndex < lastIdx) {
		return respTerm, false
	}

	// 授予 pre-vote（不更新 votedFor/term/state）
	granted = true
	return respTerm, granted
}

// =========================================================================
// 并发投票请求
// =========================================================================

func (rn *RaftNode) requestVotes(term int64, peers []PeerInfo) {
	votesNeeded := (len(peers)+1)/2 + 1 // 多数派（含自身）
	votesGranted := int32(1)            // 自己投自己一票
	peersCount := int32(len(peers))

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(p PeerInfo) {
			defer func() {
				if r := recover(); r != nil {
					rn.logf("[raft/%s] RequestVote RPC panic recovered: %v", rn.id, r)
				}
			}()
			defer wg.Done()
			tp, ok := rn.transports[p.ID]
			if !ok {
				rn.logf("[raft/%s] 未找到 peer %s 的传输层", rn.id, p.ID)
				return
			}

			req := &RequestVoteRequest{
				Term:         term,
				CandidateId:  rn.id,
				LastLogIndex: rn.getLastLogIndex(),
				LastLogTerm:  rn.getLastLogTerm(),
			}

			resp, err := tp.RequestVote(req)
			if err != nil {
				rn.logf("[raft/%s] RequestVote → %s 失败: %v", rn.id, p.ID, err)
				return
			}

			if resp.Term > term {
				rn.stepDown(resp.Term)
				return
			}

			if resp.VoteGranted {
				atomic.AddInt32(&votesGranted, 1)
			}
		}(peer)
	}
	wg.Wait()

	// 检查是否赢得选举
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if atomic.LoadInt32(&votesGranted) >= int32(votesNeeded) &&
		rn.state == StateCandidate &&
		atomic.LoadInt64(&rn.term) == term {
		rn.logf("[raft/%s] 赢得选举！得票 %d/%d (term=%d, elapsed=%v)",
			rn.id, votesGranted, peersCount+1, term, time.Since(rn.electionStart))

		// 切换到 Leader
		rn.state = StateLeader
		rn.leaderID = rn.id

		// 初始化 Leader 状态
		lastLogIdx := int64(len(rn.logs))
		for _, p := range rn.peers {
			rn.nextIdx[p.ID] = lastLogIdx + 1
			rn.matchIdx[p.ID] = 0
		}

		// 启动心跳 + 日志复制循环
		go rn.heartbeatLoop()

		rn.updateStats()
	} else {
		rn.logf("[raft/%s] 选举失败 (得票 %d/%d, term=%d)",
			rn.id, votesGranted, peersCount+1, term)
	}
}

// =========================================================================
// 降级为 Follower（收到更高 term 的消息时）
// =========================================================================

func (rn *RaftNode) stepDown(higherTerm int64) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if higherTerm <= rn.term {
		return
	}

	rn.logf("[raft/%s] stepDown: term %d → %d (当前状态: %s)",
		rn.id, rn.term, higherTerm, rn.state)

	oldState := rn.state
	rn.term = higherTerm
	rn.state = StateFollower
	rn.votedFor = ""
	rn.leaderID = ""

	// 如果之前是 Leader，停止心跳
	if oldState == StateLeader {
		select {
		case rn.heartbeatStop <- struct{}{}:
		default:
		}
	}

	// 重置选举计时器
	rn.electionTimer.Reset(randomElectionTimeout())

	rn.updateStats()
}

// =========================================================================
// Leader 心跳 + 日志复制循环
// =========================================================================

func (rn *RaftNode) heartbeatLoop() {
	ticker := time.NewTicker(rn.currentHeartbeatInterval)
	defer ticker.Stop()

	rn.logf("[raft/%s] 心跳+复制循环启动 (间隔: %v)", rn.id, rn.currentHeartbeatInterval)

	for {
		select {
		case <-rn.shutdownCh:
			rn.logf("[raft/%s] 心跳循环关闭 (shutdown)", rn.id)
			return

		case <-rn.heartbeatStop:
			rn.logf("[raft/%s] 心跳循环停止 (降级)", rn.id)
			return

		case <-ticker.C:
			rn.replicateAll()
			newInterval := rn.adaptiveHeartbeatAdjust()
			if newInterval != rn.currentHeartbeatInterval {
				rn.currentHeartbeatInterval = newInterval
				ticker.Reset(newInterval)
			}
		}
	}
}

// replicateAll 向所有 peer 发送 AppendEntries（心跳 + 日志复制）
// 这是标准 Raft 的核心：Leader 携带 entries 复制日志，空 entries 即心跳
func (rn *RaftNode) replicateAll() {
	rn.mu.RLock()
	if rn.state != StateLeader {
		rn.mu.RUnlock()
		return
	}
	term := rn.term
	leaderCommit := rn.commitIdx
	peers := make([]PeerInfo, len(rn.peers))
	copy(peers, rn.peers)

	// 为每个 peer 准备快照
	type peerSnapshot struct {
		id       string
		nextIdx  int64
		prevIdx  int64
		prevTerm int64
		entries  []RaftLog
	}
	snapshots := make([]peerSnapshot, 0, len(peers))
	for _, p := range peers {
		ni := rn.nextIdx[p.ID]
		prevIdx := ni - 1
		var prevTerm int64
		if prevIdx > 0 && int(prevIdx-1) < len(rn.logs) {
			prevTerm = rn.logs[prevIdx-1].Term
		}
		// 从 nextIdx 开始的所有日志
		var entries []RaftLog
		if int(ni-1) < len(rn.logs) && ni >= 1 {
			entries = make([]RaftLog, len(rn.logs)-int(ni-1))
			copy(entries, rn.logs[ni-1:])
		}
		snapshots = append(snapshots, peerSnapshot{
			id:       p.ID,
			nextIdx:  ni,
			prevIdx:  prevIdx,
			prevTerm: prevTerm,
			entries:  entries,
		})
	}
	rn.mu.RUnlock()

	var timeoutCount int32
	var successCount int32

	var wg sync.WaitGroup
	for _, snap := range snapshots {
		wg.Add(1)
		go func(s peerSnapshot) {
			defer func() {
				if r := recover(); r != nil {
					rn.logf("[raft/%s] AppendEntries RPC panic recovered: %v", rn.id, r)
				}
			}()
			defer wg.Done()
			tp, ok := rn.transports[s.id]
			if !ok {
				return
			}

			req := &AppendEntriesRequest{
				Term:         term,
				LeaderId:     rn.id,
				PrevLogIndex: s.prevIdx,
				PrevLogTerm:  s.prevTerm,
				Entries:      s.entries,
				LeaderCommit: leaderCommit,
			}

			resp, err := tp.AppendEntries(req)
			if err != nil {
				atomic.AddInt32(&timeoutCount, 1)
				return
			}

			atomic.AddInt32(&successCount, 1)

			if resp.Term > term {
				rn.stepDown(resp.Term)
				return
			}

			if resp.Success {
				// 更新 nextIdx / matchIdx
				rn.mu.Lock()
				if rn.state == StateLeader && atomic.LoadInt64(&rn.term) == term {
					newMatch := s.prevIdx + int64(len(s.entries))
					if newMatch > rn.matchIdx[s.id] {
						rn.matchIdx[s.id] = newMatch
					}
					rn.nextIdx[s.id] = rn.matchIdx[s.id] + 1
				}
				rn.mu.Unlock()
			} else {
				// 日志不一致，回退 nextIdx
				rn.mu.Lock()
				if rn.state == StateLeader {
					// T017: 快速回退 — 使用 ConflictIndex 直接跳到冲突点（O(1)），否则逐条递减
					if resp.ConflictIndex > 0 && resp.ConflictIndex < rn.nextIdx[s.id] {
						rn.nextIdx[s.id] = resp.ConflictIndex
					} else if rn.nextIdx[s.id] > 1 {
						rn.nextIdx[s.id]--
					}
				}
				rn.mu.Unlock()
			}
		}(snap)
	}
	wg.Wait()

	// 推进 commitIdx
	rn.advanceCommit()

	if timeoutCount > 0 {
		atomic.AddInt32(&rn.consecutiveTimeouts, int32(timeoutCount))
		atomic.StoreInt32(&rn.consecutiveSuccess, 0)
	} else {
		atomic.AddInt32(&rn.consecutiveSuccess, 1)
		atomic.StoreInt32(&rn.consecutiveTimeouts, 0)
	}
}

// advanceCommit 推进 commitIdx（Leader 专用）
// 找最大的 N > commitIdx，使得 logs[N-1].Term == 当前 term 且多数派 matchIdx >= N
func (rn *RaftNode) advanceCommit() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != StateLeader {
		return
	}

	lastIdx := int64(len(rn.logs))
	majority := (len(rn.peers)+1)/2 + 1

	for N := lastIdx; N > rn.commitIdx; N-- {
		// 只提交当前任期的日志（Raft 安全性）
		if int(N-1) >= 0 && int(N-1) < len(rn.logs) && rn.logs[N-1].Term != rn.term {
			continue
		}
		count := 1 // Leader 自己
		for _, m := range rn.matchIdx {
			if m >= N {
				count++
			}
		}
		if count >= majority {
			oldCommit := rn.commitIdx
			rn.commitIdx = N
			rn.lastApplied = N
			rn.logf("[raft/%s] commitIdx 推进: %d → %d", rn.id, oldCommit, N)
			rn.updateStats()
			committedLogs := rn.collectCommittedLogs(oldCommit)
			// 唤醒 ProposeSync 等待者
			rn.commitCond.Broadcast()
			// 在锁内收集完，锁外触发回调
			go rn.fireOnCommit(committedLogs)
			return
		}
	}
}

// =========================================================================
// Propose — 客户端提案接口（Leader 追加日志）
// =========================================================================

// Propose 向集群提交一条命令（仅 Leader 可调用）
// 返回日志索引。日志的提交（commitIdx 前进）由后台心跳+复制循环异步完成。
// 调用者可通过 ProposeSync 等待提交确认。
func (rn *RaftNode) Propose(command []byte) (int64, error) {
	rn.mu.Lock()
	if rn.state != StateLeader {
		rn.mu.Unlock()
		return 0, ErrNotLeader
	}
	index := int64(len(rn.logs)) + 1
	entry := RaftLog{
		Index:   index,
		Term:    rn.term,
		Command: command,
	}
	rn.logs = append(rn.logs, entry)
	rn.updateStats()
	rn.mu.Unlock()

	rn.logf("[raft/%s] 提案追加 index=%d term=%d (cmd_len=%d)", rn.id, index, rn.term, len(command))
	return index, nil
}

// ProposeSync 提交一条命令并同步等待其被提交（commitIdx >= index）
//
//	timeout: 等待超时（<=0 表示不超时，永久等待）
func (rn *RaftNode) ProposeSync(command []byte, timeout time.Duration) (int64, error) {
	index, err := rn.Propose(command)
	if err != nil {
		return 0, err
	}

	// 立即触发一次复制（不必等下一个心跳 tick）
	go rn.replicateAll()

	// 等待 commitIdx >= index
	deadline := time.Now().Add(timeout)
	if timeout <= 0 {
		deadline = time.Time{} // zero = 无限
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()
	for rn.commitIdx < index {
		if rn.state != StateLeader {
			return index, ErrNotLeader
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return index, ErrProposeTimeout
		}
		// 带超时的等待：释放锁，sleep 一小段再检查
		rn.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		rn.mu.Lock()
	}
	rn.logf("[raft/%s] 提案 index=%d 已提交确认", rn.id, index)
	return index, nil
}

// =========================================================================
// gRPC 服务端处理 — RequestVote（改为 Transport 服务端处理）
// =========================================================================

// HandleRequestVote 处理 RequestVote RPC（由传输层服务端调用）
func (rn *RaftNode) HandleRequestVote(req *RequestVoteRequest) (*RequestVoteResponse, error) {
	rn.mu.Lock()

	resp := &RequestVoteResponse{Term: rn.term, VoteGranted: false}

	if req.Term < rn.term {
		resp.Term = rn.term
		rn.mu.Unlock()
		return resp, nil
	}

	if req.Term > rn.term {
		rn.term = req.Term
		rn.state = StateFollower
		rn.votedFor = ""
		rn.leaderID = ""
		rn.electionTimer.Reset(randomElectionTimeout())
		resp.Term = req.Term
	}

	if rn.votedFor != "" && rn.votedFor != req.CandidateId {
		rn.mu.Unlock()
		return resp, nil
	}

	// 拦截规则 1：自身日志未追上 Leader 的新节点不参与投票
	if !rn.logCaughtUp && !rn.lastHeartbeat.IsZero() {
		rn.logf("[raft/%s] 拒绝投票给 %s: 自身日志尚未追上 Leader", rn.id, req.CandidateId)
		rn.mu.Unlock()
		return resp, nil
	}

	// 拦截规则 2：空日志候选者在集群已有提交日志时禁止当选
	if req.LastLogIndex == 0 && rn.commitIdx > 0 {
		rn.logf("[raft/%s] 拒绝投票给 %s: 候选者日志为空但集群已提交到 index=%d",
			rn.id, req.CandidateId, rn.commitIdx)
		rn.mu.Unlock()
		return resp, nil
	}

	lastIdx := int64(len(rn.logs))
	var lastTerm int64
	if lastIdx > 0 {
		lastTerm = rn.logs[lastIdx-1].Term
	}

	if req.LastLogTerm < lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex < lastIdx) {
		rn.mu.Unlock()
		return resp, nil
	}

	rn.votedFor = req.CandidateId
	rn.electionTimer.Reset(randomElectionTimeout())
	resp.VoteGranted = true

	rn.updateStats()
	rn.mu.Unlock()

	return resp, nil
}

// =========================================================================
// 服务端处理 — AppendEntries (心跳 + 日志复制)
// =========================================================================

// HandleAppendEntries 处理 AppendEntries RPC（由传输层服务端调用）
func (rn *RaftNode) HandleAppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	rn.mu.Lock()

	resp := &AppendEntriesResponse{Term: rn.term, Success: false}

	if req.Term < rn.term {
		resp.Term = rn.term
		rn.mu.Unlock()
		return resp, nil
	}

	if req.Term > rn.term {
		rn.term = req.Term
		rn.state = StateFollower
		rn.votedFor = ""
	}

	rn.electionTimer.Reset(randomElectionTimeout())
	rn.leaderID = req.LeaderId
	rn.lastHeartbeat = time.Now()

	// 心跳（无 entries）
	if len(req.Entries) == 0 {
		var committedLogs []RaftLog
		if req.LeaderCommit > rn.commitIdx {
			oldCommit := rn.commitIdx
			lastLogIdx := int64(len(rn.logs))
			if req.LeaderCommit < lastLogIdx {
				rn.commitIdx = req.LeaderCommit
			} else {
				rn.commitIdx = lastLogIdx
			}
			rn.lastApplied = rn.commitIdx
			committedLogs = rn.collectCommittedLogs(oldCommit)
			rn.commitCond.Broadcast()
		}
		if !rn.logCaughtUp && req.LeaderCommit > 0 && int64(len(rn.logs)) >= req.LeaderCommit {
			rn.logCaughtUp = true
		}
		resp.Success = true
		rn.updateStats()
		rn.mu.Unlock()
		go rn.fireOnCommit(committedLogs)
		return resp, nil
	}

	// 日志复制
	lastLogIdx := int64(len(rn.logs))

	// 一致性检查 1：prevLogIndex 超出本地日志长度
	if req.PrevLogIndex > lastLogIdx {
		// T017: 快速回退 — ConflictIndex = lastLogIdx + 1（Leader 直接回退到此）
		resp.ConflictIndex = lastLogIdx + 1
		rn.mu.Unlock()
		return resp, nil
	}
	// 一致性检查 2：prevLogIndex 处的 term 不匹配
	if req.PrevLogIndex > 0 {
		if rn.logs[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			// T017: 快速回退 — 找到冲突 term 的首个索引
			conflictTerm := rn.logs[req.PrevLogIndex-1].Term
			conflictIdx := req.PrevLogIndex
			for i := req.PrevLogIndex - 1; i > 0; i-- {
				if rn.logs[i-1].Term != conflictTerm {
					break
				}
				conflictIdx = i
			}
			resp.ConflictIndex = conflictIdx
			rn.mu.Unlock()
			return resp, nil
		}
	}

	// 追加/覆盖日志
	for _, entry := range req.Entries {
		if entry.Index <= lastLogIdx {
			// 冲突：截断
			if entry.Index > 0 && int(entry.Index-1) < len(rn.logs) {
				if rn.logs[entry.Index-1].Term != entry.Term {
					rn.logs = rn.logs[:entry.Index-1]
					lastLogIdx = int64(len(rn.logs))
				}
			}
		}
		if entry.Index > int64(len(rn.logs)) {
			rn.logs = append(rn.logs, RaftLog{
				Index:   entry.Index,
				Term:    entry.Term,
				Command: entry.Command,
				Hash:    entry.Hash,
			})
		}
	}

	// 推进 commitIdx
	var committedLogs []RaftLog
	if req.LeaderCommit > rn.commitIdx {
		oldCommit := rn.commitIdx
		newLast := int64(len(rn.logs))
		if req.LeaderCommit < newLast {
			rn.commitIdx = req.LeaderCommit
		} else {
			rn.commitIdx = newLast
		}
		rn.lastApplied = rn.commitIdx
		committedLogs = rn.collectCommittedLogs(oldCommit)
		rn.commitCond.Broadcast()
	}

	if !rn.logCaughtUp && req.LeaderCommit > 0 && int64(len(rn.logs)) >= req.LeaderCommit {
		rn.logCaughtUp = true
		rn.logf("[raft/%s] 日志已追上 Leader (logs=%d, leaderCommit=%d)",
			rn.id, len(rn.logs), req.LeaderCommit)
	}

	resp.Success = true
	rn.updateStats()
	rn.mu.Unlock()
	go rn.fireOnCommit(committedLogs)

	return resp, nil
}

// =========================================================================
// 状态转换
// =========================================================================

func (rn *RaftNode) transitionTo(newState NodeState) {
	rn.mu.Lock()
	oldState := rn.state
	rn.state = newState
	rn.mu.Unlock()

	rn.logf("[raft/%s] 状态转换: %s → %s", rn.id, oldState, newState)
}

// =========================================================================
// 安全关闭
// =========================================================================

func (rn *RaftNode) Shutdown() {
	rn.shutdownOnce.Do(func() {
		close(rn.shutdownCh)
		rn.logf("[raft/%s] 节点已安全关闭", rn.id)
	})
}

// =========================================================================
// 内部辅助方法
// =========================================================================

func (rn *RaftNode) getLastLogIndex() int64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return int64(len(rn.logs))
}

func (rn *RaftNode) getLastLogTerm() int64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	if len(rn.logs) == 0 {
		return 0
	}
	return rn.logs[len(rn.logs)-1].Term
}

func (rn *RaftNode) updateStats() {
	s := rn.stats
	s.State = rn.state.String()
	s.Term = rn.term
	s.LeaderID = rn.leaderID
	s.CommitIndex = rn.commitIdx
	s.LastApplied = rn.lastApplied
	s.LogCount = len(rn.logs)
	s.PeerCount = len(rn.peers)
	s.VotedFor = rn.votedFor
}

func (rn *RaftNode) peerIDs() []string {
	ids := make([]string, len(rn.peers))
	for i, p := range rn.peers {
		ids[i] = p.ID
	}
	return ids
}

func (rn *RaftNode) logf(format string, args ...interface{}) {
	if rn.logger != nil {
		rn.logger.Printf(format, args...)
	}
}

// =========================================================================
// 工具函数
// =========================================================================

func randomElectionTimeout() time.Duration {
	ms := electionTimeoutMin + rand.Intn(electionTimeoutMax-electionTimeoutMin)
	return time.Duration(ms) * time.Millisecond
}

func (rn *RaftNode) adaptiveHeartbeatAdjust() time.Duration {
	timeouts := atomic.LoadInt32(&rn.consecutiveTimeouts)
	successes := atomic.LoadInt32(&rn.consecutiveSuccess)

	if timeouts >= int32(adaptiveTimeoutThreshold) {
		return heartbeatIntervalMax
	}

	if successes >= int32(adaptiveRecoverySuccess) {
		return heartbeatIntervalMin
	}

	return rn.currentHeartbeatInterval
}

// =========================================================================
// 集群辅助：等待 Leader 选举完成
// =========================================================================

// WaitForLeader 轮询等待直到集群选出 Leader（或超时）
func (rn *RaftNode) WaitForLeader(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if leader := rn.LeaderID(); leader != "" {
			return leader, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return "", fmt.Errorf("等待 Leader 超时 (%v)", timeout)
}
