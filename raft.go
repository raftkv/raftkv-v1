// =========================================================================
// RaftKV 确定性引擎 — 纯 Go Raft 共识引擎
//
// 设计原则：
//   1. 零外部 Raft 库依赖 — 纯 Go + goroutine + channel 实现
//   2. 严格遵循 Raft 论文状态机（Follower → Candidate → Leader）
//   3. 三节点模式，通过 gRPC 进行 RPC 通信
//   4. 随机选举超时避免脑裂（150-300ms 范围）
//   5. Leader 心跳间隔 50ms，快速检测故障
// =========================================================================

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pb "raftkv/proto"
)

// ErrCompacted 日志已压缩：请求的索引 < logStartIndex，调用方应走快照路径
var ErrCompacted = errors.New("log compacted: requested index < logStartIndex")

// batch24: pre-vote 共享 HTTP 客户端（连接池 + 200ms 超时，减少选举开销）
var preVoteHTTPClient = &http.Client{
	Timeout: 200 * time.Millisecond,
	Transport: &http.Transport{
		MaxIdleConns:        5,
		MaxIdleConnsPerHost: 1,
		IdleConnTimeout:     30 * time.Second,
	},
}

// =========================================================================
// 编译时常量
// =========================================================================

const (
	// 选举超时范围（毫秒）— batch22: 降至 800-1200ms 以实现选举完成 ≤2s
	// Fix #7 约束维持: 800ms > rpcTimeout(500ms)，防止投票期间其他节点触发新选举
	electionTimeoutMin = 800  // 最小选举超时
	electionTimeoutMax = 1200 // 最大选举超时

	// Leader 心跳间隔
	heartbeatIntervalMin = 20 * time.Millisecond
	heartbeatIntervalMax = 500 * time.Millisecond

	// 弱网自适应阈值
	adaptiveTimeoutThreshold = 3
	adaptiveRecoverySuccess  = 3

	// gRPC 调用超时
	rpcTimeout = 500 * time.Millisecond
)

// =========================================================================
// RaftNode — 纯 Go Raft 节点
// =========================================================================

type RaftNode struct {
	// --- 持久状态（所有节点）---
	id        string            // 节点唯一 ID
	peers     []PeerInfo        // 集群成员列表（不含自身）
	peerAddrs map[string]string // peerID → gRPC address 快速索引

	mu       sync.RWMutex // 保护以下所有字段
	state    NodeState    // Follower / Candidate / Leader
	term     int64        // 当前任期
	votedFor string       // 本任期投票给谁（空=未投票）
	leaderID string       // 当前已知 Leader

	// --- 日志 ---
	logs          []RaftLog // 日志条目（索引从 1 开始，0 为哨兵）
	logStartIndex int64     // 日志压缩后的起始索引（< 此索引的条目已被压缩，Command/SM3Hash 为 nil）
	commitIdx     int64     // 已提交的最高索引
	lastApplied   int64     // 已应用到状态机的最高索引

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
	// --- V2.5.1 B1修复: 提交推进通知（Propose 去自旋等待）---
	// commitIdx 前进时非阻塞投递，Propose 通过它等待提交，替代 20ms 自旋 + sendHeartbeats。
	commitNotify chan struct{} // 容量 1，select+default 非阻塞投递

	// --- gRPC 客户端工厂 ---
	// 每个 peer 一个 gRPC 客户端连接，由外部注入
	peerClients map[string]pb.RaftServiceClient

	// --- 统计 ---
	stats  *RaftStats
	logger Logger

	// --- 日志提交回调（管线钩子）---
	// 当 commitIdx 前进时调用，用于接入 WAL 持久化 + TiDB 落盘
	// 设置方式: node.SetOnCommit(pipeline.OnCommit)
	onCommit func(RaftLog)

	// 选举开始时间（用于日志）
	electionStart time.Time

	// --- 新节点日志追赶标志 ---
	// false = 新加入节点，日志尚未追上 Leader，不发起选举也不接收投票
	// true  = 日志已追上 Leader 的 commitIdx，可正常参与选举
	logCaughtUp bool

	// --- V2.3: 动态成员变更配置 ---
	config *ClusterConfig

	// --- 最近收到 Leader 心跳时间（zero = 从未见过 Leader，全新集群） ---
	lastHeartbeat time.Time

	// --- 弱网自适应心跳 ---
	consecutiveTimeouts      int32
	consecutiveSuccess       int32
	currentHeartbeatInterval time.Duration

	// --- batch15 可观测性埋点（atomic，不进入写路径热区）---
	heartbeatIntervalAtomic atomic.Int64 // 心跳间隔纳秒（镜像，供 metrics 采集）
	electionEventCount      atomic.Int64 // 选举事件计数

	// --- 选举风暴自愈：logCaughtUp 死锁突破 ---
	candidateFailCount int32     // 连续选举失败次数
	firstCandidateTime time.Time // 首次进入 Candidate 的时间窗口起点

	// --- TCX-Ⅳ 硬修复：WAL重放门禁 ---
	walReplayCompleted bool           // WAL重放是否完成
	replayStats        WALReplayStats // 重放统计

	// --- TCX-Ⅳ 硬修复：WAL物理剥离门禁 ---
	walGateClosed      bool     // WAL门禁是否关闭
	walGate            *WALGate // WAL门禁实例
	rejectedWriteCount int64    // 被拒绝的写入计数

	// --- TCX-Ⅳ 硬修复：批量闪电同步 ---
	batchSyncMgr *BatchSyncManager // 批量同步管理器

	// --- R-04修复A: peerClients 线程安全访问 ---
	peerClientsMu sync.RWMutex // 保护 peerClients map 并发更新（重连回调写入）

	// --- R-04修复B: follower 降级标记 ---
	degradedFollowers map[string]bool // peerID → 批量同步放弃后标记降级

	// --- R-04修复C: gap 持续告警 ---
	gapSince map[string]time.Time // peerID → gap首次超过阈值的时间戳

	// --- 刀三: 快照兜底路径 ---
	peerHttpAddrs   map[string]string                    // peerID → HTTP address（快照传输用）
	getSnapshotData func() ([]byte, int64, int64, error) // 回调：返回 (snapshotData, lastIncludedIndex, lastIncludedTerm, error)
	installSnapshot func([]byte, int64, int64) error     // 回调：参数 (snapshotData, lastIncludedIndex, lastIncludedTerm)

	// --- batch35 T031/T032: 分片快照传输 ---
	snapshotThrottle *SnapshotThrottle                  // T033: 令牌桶限流器
	snapRecvBuf      map[string]*snapshotReceiveSession // T031: 分片接收会话（leaderID → session）
	snapRecvMu       sync.Mutex                         // 保护 snapRecvBuf

	// --- batch11: group commit 攒批层 ---
	proposeBatchCh    chan *proposeRequest // Propose 请求通道
	replicateCh       chan struct{}        // 立即复制触发信号
	sendHBInFlight    int32                // sendHeartbeats 防重入标志
	proposeBatchClose chan struct{}        // 攒批循环关闭信号
	proposeBatchWg    sync.WaitGroup       // 攒批循环 WaitGroup
	proposeBatchOn    bool                 // 攒批是否已启用
	proposeBatchSize  int                  // 攒批最大条数
	proposeBatchWin   time.Duration        // 攒批时间窗口
	batchStatsMu      sync.Mutex           // 保护 batch 统计
	batchCountTotal   int64                // 总批次数
	batchSizeTotal    int64                // 总攒批条数
	batchSizeHist     [65]int64            // 批大小直方图 (0=1条, 63=64条, 64=溢出)
	inFlightUtilFn    func() float64       // batch18: 在途利用率查询（自适应 flush 用）

	// --- batch19: 组提交广播 + 专用复制循环 ---
	commitBroadcast  *commitBroadcaster // 提交广播通知（替换 commitNotify 的 cap=1 限制）
	replicateTrigger chan struct{}      // 专用复制循环信号（替换 per-batch CAS 触发）
	replicateStop    chan struct{}      // 专用复制循环停止信号
	replicateWg      sync.WaitGroup     // 专用复制循环 WaitGroup
}

type commitBroadcaster struct {
	mu        sync.RWMutex
	commitIdx int64
	notifyCh  chan struct{}
}

func newCommitBroadcaster() *commitBroadcaster {
	return &commitBroadcaster{notifyCh: make(chan struct{})}
}

func (cb *commitBroadcaster) Notify(newCommitIdx int64) {
	cb.mu.Lock()
	if newCommitIdx > cb.commitIdx {
		cb.commitIdx = newCommitIdx
		old := cb.notifyCh
		cb.notifyCh = make(chan struct{})
		close(old)
	}
	cb.mu.Unlock()
}

func (cb *commitBroadcaster) CurrentCommitIdx() int64 {
	cb.mu.RLock()
	idx := cb.commitIdx
	cb.mu.RUnlock()
	return idx
}

func (cb *commitBroadcaster) WaitCh() chan struct{} {
	cb.mu.RLock()
	ch := cb.notifyCh
	cb.mu.RUnlock()
	return ch
}

type proposeRequest struct {
	command  []byte
	resultCh chan proposeResult
	tEnqueue int64
	tFlush   int64
	tRepl    int64
}

type proposeResult struct {
	index int64
	err   error
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

func NewRaftNode(
	id string,
	peerAddrs map[string]string,
	peerClients map[string]pb.RaftServiceClient,
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
		logStartIndex: 1,
		commitIdx:     0,
		lastApplied:   0,
		nextIdx:       make(map[string]int64),
		matchIdx:      make(map[string]int64),
		electionTimer: time.NewTimer(randomElectionTimeout()),
		heartbeatStop: make(chan struct{}),
		commitNotify:  make(chan struct{}, 1),
		voteResultCh:  make(chan voteResult, 256),
		stateChangeCh: make(chan NodeState, 8),
		shutdownCh:    make(chan struct{}),
		peerClients:   peerClients,
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
		logCaughtUp:              false,
		degradedFollowers:        make(map[string]bool),
		gapSince:                 make(map[string]time.Time),
		snapRecvBuf:              make(map[string]*snapshotReceiveSession),
		proposeBatchCh:           make(chan *proposeRequest, 1024),
		replicateCh:              make(chan struct{}, 256),
		proposeBatchClose:        make(chan struct{}),
		proposeBatchSize:         64,
		proposeBatchWin:          2 * time.Millisecond,
		commitBroadcast:          newCommitBroadcaster(),
		replicateTrigger:         make(chan struct{}, 256),
		replicateStop:            make(chan struct{}),
	}

	// V2.3: 初始化集群配置（自身 + 所有 peer）
	initialPeers := make([]string, 0, len(peerAddrs)+1)
	initialPeers = append(initialPeers, id)
	for pid := range peerAddrs {
		initialPeers = append(initialPeers, pid)
	}
	rn.config = NewClusterConfig(initialPeers, logger)

	return rn
}

// SetLogCaughtUp 设置日志追赶状态
// 外部在从 WAL 恢复日志后调用此方法标记节点已追上
func (rn *RaftNode) SetLogCaughtUp(caughtUp bool) {
	rn.mu.Lock()
	rn.logCaughtUp = caughtUp
	rn.mu.Unlock()
}

// RestoreFromWAL 从 WAL 回放日志恢复内存状态
// 在节点启动时调用，将磁盘上已持久化的日志重新加载回内存
func (rn *RaftNode) RestoreFromWAL(logs []RaftLog) {
	if len(logs) == 0 {
		rn.mu.Lock()
		rn.walReplayCompleted = true
		rn.mu.Unlock()
		return
	}

	// Fix #3: WAL条目按Index排序后再校验，避免随机存储顺序导致误判
	sort.Slice(logs, func(i, j int) bool {
		return logs[i].Index < logs[j].Index
	})

	integrity, validUntil := rn.validateLogIntegrity(logs)

	rn.mu.Lock()
	defer rn.mu.Unlock()

	if integrity != "完整" && validUntil > 0 {
		rn.logs = logs[:validUntil]
		rn.logf("[raft/%s] WAL完整性告警: %s, 恢复至 index=%d", rn.id, integrity, validUntil)
	} else {
		rn.logs = logs
	}

	// Fix #1: 截断后重编号index为连续序列(1,2,...N)，确保log位置与index一致
	for i := range rn.logs {
		rn.logs[i].Index = int64(i + 1)
	}
	rn.logStartIndex = 1

	lastLog := rn.logs[len(rn.logs)-1]
	// Fix #1: commitIdx/lastApplied基于log位置而非原始WAL index，防止截断后index空洞
	rn.commitIdx = int64(len(rn.logs))
	rn.lastApplied = int64(len(rn.logs))
	if lastLog.Term > rn.term {
		rn.term = lastLog.Term
	}
	rn.logCaughtUp = true
	rn.walReplayCompleted = true

	rn.stats.Lock()
	rn.stats.Term = rn.term
	rn.stats.CommitIndex = rn.commitIdx
	rn.stats.LastApplied = rn.lastApplied
	rn.stats.LogCount = len(rn.logs)
	rn.stats.Unlock()

	if rn.logger != nil {
		rn.logger.Printf("WAL回放完成: 恢复 %d 条日志, commitIdx=%d, term=%d, 完整性=%s",
			len(rn.logs), rn.commitIdx, rn.term, integrity)
	}
}

// validateLogIntegrity 校验日志索引连续性与完整性
func (rn *RaftNode) validateLogIntegrity(logs []RaftLog) (integrity string, validUntil int64) {
	integrity = "完整"
	validUntil = int64(len(logs))

	for i := 1; i < len(logs); i++ {
		if logs[i].Index == logs[i-1].Index {
			integrity = "损坏"
			validUntil = int64(i)
			return
		}
		if logs[i].Index != logs[i-1].Index+1 {
			integrity = "截断"
			validUntil = int64(i)
			return
		}
	}
	return
}

// StepDownForWALFailure WAL故障强制降级为Follower
func (rn *RaftNode) StepDownForWALFailure(reason string) {
	rn.mu.Lock()
	oldState := rn.state
	rn.state = StateFollower
	rn.votedFor = ""
	rn.leaderID = ""
	rn.walGateClosed = true

	if oldState == StateLeader {
		select {
		case rn.heartbeatStop <- struct{}{}:
		default:
		}
		rn.mu.Unlock()
		rn.StopProposeBatch()
		rn.mu.Lock()
	}

	rn.electionTimer.Reset(randomElectionTimeout())
	rn.updateStats()
	rn.mu.Unlock()

	rn.logf("[raft/%s] WAL门禁触发: 强制降级为Follower, 原因: %s (原状态: %s)",
		rn.id, reason, oldState.String())
}

// IdentifyLaggingFollowers 识别日志落后的Follower节点
func (rn *RaftNode) IdentifyLaggingFollowers() []LaggingFollower {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.state != StateLeader {
		return nil
	}

	threshold := int64(100)
	if rn.batchSyncMgr != nil {
		threshold = rn.batchSyncMgr.config.LagThreshold
	}

	var result []LaggingFollower
	for _, p := range rn.peers {
		matchIdx := rn.matchIdx[p.ID]
		nextIdx := rn.nextIdx[p.ID]
		// 刀一: 使用 nextIdx（乐观估计）而非 matchIdx+1 作为同步起点
		// nextIdx 在 Leader 当选时初始化为 lastLogIdx+1（乐观假设 follower 已跟上）
		// 心跳探测失败时递减，batch sync 失败时也递减，永不跳回 1
		estimatedIdx := nextIdx - 1
		if estimatedIdx < matchIdx {
			estimatedIdx = matchIdx // matchIdx 是已确认的下界
		}
		gap := rn.commitIdx - estimatedIdx
		if gap > threshold {
			batchSize := int64(4096)
			if rn.batchSyncMgr != nil {
				batchSize = rn.batchSyncMgr.config.MaxBatchSize
			}
			if gap < batchSize {
				batchSize = gap
			}
			startIdx := nextIdx
			if startIdx < 1 {
				startIdx = 1
			}
			result = append(result, LaggingFollower{
				PeerID:    p.ID,
				Gap:       gap,
				BatchSize: batchSize,
				StartIdx:  startIdx,
				EndIdx:    rn.commitIdx,
			})
		}
	}
	return result
}

// LaggingFollower 落后Follower信息
type LaggingFollower struct {
	PeerID    string
	Gap       int64
	BatchSize int64
	StartIdx  int64
	EndIdx    int64
}

// GetLogEntries 获取指定索引范围的日志条目（转换为proto格式）
// 如果 startIdx < logStartIndex，返回 ErrCompacted，调用方应走快照路径
func (rn *RaftNode) GetLogEntries(startIdx, endIdx int64) ([]*pb.LogEntry, error) {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if startIdx < rn.logStartIndex {
		return nil, ErrCompacted
	}

	var entries []*pb.LogEntry
	for i := startIdx; i <= endIdx; i++ {
		e := rn.logAtLocked(i)
		if e != nil {
			entries = append(entries, &pb.LogEntry{
				Term:    e.Term,
				Index:   e.Index,
				Command: e.Command,
				Sm3Hash: e.SM3Hash,
			})
		}
	}
	return entries, nil
}

// GetLogStartIndex 获取日志压缩后的起始索引
func (rn *RaftNode) GetLogStartIndex() int64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.logStartIndex
}

// lastLogIndexLocked 返回最后一条日志的 Index（0 表示空）。调用方须持有 rn.mu。
func (rn *RaftNode) lastLogIndexLocked() int64 {
	if len(rn.logs) == 0 {
		return 0
	}
	lsi := rn.logStartIndex
	if lsi == 0 {
		lsi = 1
	}
	return int64(len(rn.logs)) + lsi - 1
}

// logAtLocked 返回指定 Index 的日志条目指针，越界或已压缩返回 nil。调用方须持有 rn.mu。
func (rn *RaftNode) logAtLocked(idx int64) *RaftLog {
	lsi := rn.logStartIndex
	if lsi == 0 {
		lsi = 1
	}
	arrIdx := idx - lsi
	if arrIdx < 0 || int(arrIdx) >= len(rn.logs) {
		return nil
	}
	return &rn.logs[arrIdx]
}

// GetPeerHttpAddr 获取指定 peer 的 HTTP 地址（快照传输用）
func (rn *RaftNode) GetPeerHttpAddr(peerID string) (string, bool) {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	addr, ok := rn.peerHttpAddrs[peerID]
	return addr, ok
}

// ReloadFromSnapshot 从快照数据重载日志
// snapshotData 是 json([]RaftLog) 格式
// lastIncludedIndex/lastIncludedTerm 是快照中最后一条日志的索引和任期
// T035: 快照后日志追赶 — logStartIndex 从首条日志推断，commitIdx/lastApplied 设为 lastIncludedIndex
func (rn *RaftNode) ReloadFromSnapshot(snapshotData []byte, lastIncludedIndex int64, lastIncludedTerm int64) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	var logs []RaftLog
	if err := json.Unmarshal(snapshotData, &logs); err != nil {
		return fmt.Errorf("快照反序列化失败: %w", err)
	}

	if len(logs) == 0 {
		rn.logs = make([]RaftLog, 0)
		rn.commitIdx = lastIncludedIndex
		rn.lastApplied = lastIncludedIndex
		rn.logStartIndex = lastIncludedIndex + 1
	} else {
		rn.logs = logs
		rn.commitIdx = lastIncludedIndex
		rn.lastApplied = lastIncludedIndex
		rn.logStartIndex = logs[0].Index
	}

	rn.logf("[raft/%s] 快照重载: %d 条日志, commitIdx=%d, lastApplied=%d, logStartIndex=%d",
		rn.id, len(logs), lastIncludedIndex, lastIncludedIndex, rn.logStartIndex)

	return nil
}

// =========================================================================
// batch35 T031/T032: 分片 InstallSnapshot 协议实现
// =========================================================================

// SnapshotThrottle 令牌桶限流器（T033），限制快照传输出站带宽
type SnapshotThrottle struct {
	tokens chan struct{}
	rate   int
	burst  int
}

// NewSnapshotThrottle 创建快照传输限流器
func NewSnapshotThrottle(rate int, burst int) *SnapshotThrottle {
	if burst < 1 {
		burst = 1
	}
	t := &SnapshotThrottle{
		tokens: make(chan struct{}, burst),
		rate:   rate,
		burst:  burst,
	}
	for i := 0; i < burst; i++ {
		t.tokens <- struct{}{}
	}
	go t.refill()
	return t
}

func (t *SnapshotThrottle) refill() {
	interval := time.Second / time.Duration(t.rate)
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		select {
		case t.tokens <- struct{}{}:
		default:
		}
	}
}

// Acquire 阻塞获取一个令牌许可
func (t *SnapshotThrottle) Acquire(ctx context.Context) error {
	select {
	case <-t.tokens:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release 释放一个令牌许可
func (t *SnapshotThrottle) Release() {
	select {
	case t.tokens <- struct{}{}:
	default:
	}
}

// snapshotReceiveSession 分片接收会话（T031）
type snapshotReceiveSession struct {
	buf              []byte
	lastIncludedIdx  int64
	lastIncludedTerm int64
	leaderTerm       int64
	leaderID         string
}

// snapshotChunkSize 快照分片大小（1MB）
const snapshotChunkSize = 1024 * 1024

// HandleInstallSnapshot T031: RPC 服务端处理 — 校验 term → 累积分片 → done=true 时应用状态机
func (rn *RaftNode) HandleInstallSnapshot(req InstallSnapshotRequest) InstallSnapshotResponse {
	rn.mu.Lock()
	if req.Term < rn.term {
		resp := InstallSnapshotResponse{Term: rn.term, Success: false}
		rn.mu.Unlock()
		return resp
	}
	if req.Term > rn.term {
		rn.term = req.Term
		rn.state = StateFollower
		rn.votedFor = ""
		rn.leaderID = req.LeaderId
	}
	rn.mu.Unlock()

	rn.snapRecvMu.Lock()
	defer rn.snapRecvMu.Unlock()

	session, ok := rn.snapRecvBuf[req.LeaderId]
	if !ok {
		session = &snapshotReceiveSession{
			lastIncludedIdx:  req.LastIncludedIndex,
			lastIncludedTerm: req.LastIncludedTerm,
			leaderTerm:       req.Term,
			leaderID:         req.LeaderId,
		}
		if rn.snapRecvBuf == nil {
			rn.snapRecvBuf = make(map[string]*snapshotReceiveSession)
		}
		rn.snapRecvBuf[req.LeaderId] = session
	}

	session.buf = append(session.buf, req.Data...)

	if !req.Done {
		return InstallSnapshotResponse{Term: rn.term, Success: true}
	}

	snapshotData := session.buf
	lastIdx := session.lastIncludedIdx
	lastTerm := session.lastIncludedTerm
	delete(rn.snapRecvBuf, req.LeaderId)

	rn.mu.Lock()
	if lastIdx > 0 {
		cutIdx := lastIdx - rn.logStartIndex + 1
		if cutIdx > 0 && int(cutIdx) <= len(rn.logs) {
			rn.logs = rn.logs[cutIdx:]
		} else if int(cutIdx) > len(rn.logs) {
			rn.logs = make([]RaftLog, 0)
		}
		rn.logStartIndex = lastIdx + 1
		if rn.commitIdx < lastIdx {
			rn.commitIdx = lastIdx
		}
		if rn.lastApplied < lastIdx {
			rn.lastApplied = lastIdx
		}
	}
	rn.mu.Unlock()

	if rn.installSnapshot != nil {
		if err := rn.installSnapshot(snapshotData, lastIdx, lastTerm); err != nil {
			rn.logf("[raft/%s] HandleInstallSnapshot: installSnapshot 回调失败: %v", rn.id, err)
			return InstallSnapshotResponse{Term: rn.term, Success: false}
		}
	}

	rn.logf("[raft/%s] HandleInstallSnapshot: 快照安装完成, lastIncludedIndex=%d, lastIncludedTerm=%d, logStartIndex=%d",
		rn.id, lastIdx, lastTerm, rn.GetLogStartIndex())

	return InstallSnapshotResponse{Term: rn.term, Success: true}
}

// sendInstallSnapshot T032: Leader 侧分片发送 + 限流控制 + 完成后切换 AppendEntries 追赶
func (rn *RaftNode) sendInstallSnapshot(peerID string, term int64, leaderCommit int64) bool {
	if rn.getSnapshotData == nil {
		return false
	}
	snapshotData, lastIdx, lastTerm, err := rn.getSnapshotData()
	if err != nil {
		rn.logf("[raft/%s] sendInstallSnapshot: 快照读取失败: %v", rn.id, err)
		return false
	}

	httpAddr, ok := rn.GetPeerHttpAddr(peerID)
	if !ok {
		return false
	}

	throttle := rn.snapshotThrottle
	totalLen := len(snapshotData)
	offset := int64(0)

	for offset < int64(totalLen) {
		end := offset + int64(snapshotChunkSize)
		if end > int64(totalLen) {
			end = int64(totalLen)
		}
		chunk := SnapshotChunk{
			Offset: offset,
			Data:   snapshotData[offset:end],
			Last:   end >= int64(totalLen),
		}

		if throttle != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := throttle.Acquire(ctx); err != nil {
				cancel()
				rn.logf("[raft/%s] sendInstallSnapshot: 限流获取失败: %v", rn.id, err)
				return false
			}
			cancel()
		}

		req := InstallSnapshotRequest{
			Term:              term,
			LeaderId:          rn.id,
			LastIncludedIndex: lastIdx,
			LastIncludedTerm:  lastTerm,
			Offset:            chunk.Offset,
			Data:              chunk.Data,
			Done:              chunk.Last,
		}
		body, _ := json.Marshal(req)
		url := fmt.Sprintf("http://%s/raft/install-snapshot-chunk", httpAddr)
		ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
		httpReq, _ := http.NewRequestWithContext(ctx2, "POST", url, bytes.NewReader(body))
		httpReq.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			cancel2()
			rn.logf("[raft/%s] sendInstallSnapshot: 分片发送失败 offset=%d: %v", rn.id, offset, err)
			return false
		}
		resp.Body.Close()
		cancel2()
		if resp.StatusCode != http.StatusOK {
			rn.logf("[raft/%s] sendInstallSnapshot: 分片被拒绝 offset=%d status=%d", rn.id, offset, resp.StatusCode)
			return false
		}

		if throttle != nil {
			throttle.Release()
		}
		offset = end
	}

	rn.mu.Lock()
	if rn.state == StateLeader && atomic.LoadInt64(&rn.term) == term {
		rn.matchIdx[peerID] = lastIdx
		rn.nextIdx[peerID] = lastIdx + 1
	}
	rn.mu.Unlock()
	rn.advanceCommit(term)

	rn.logf("[raft/%s] sendInstallSnapshot: peer=%s 快照发送完成, lastIdx=%d, nextIdx=%d",
		rn.id, peerID, lastIdx, lastIdx+1)
	return true
}

// GetLogTerm 获取指定索引日志的任期
func (rn *RaftNode) GetLogTerm(idx int64) int64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	e := rn.logAtLocked(idx)
	if e == nil {
		return 0
	}
	return e.Term
}

// getCommitIdx 获取当前commitIdx
func (rn *RaftNode) getCommitIdx() int64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.commitIdx
}

// UpdateFollowerProgress 更新Follower的同步进度
func (rn *RaftNode) UpdateFollowerProgress(peerID string, lastMatch int64) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if lastMatch > rn.matchIdx[peerID] {
		rn.matchIdx[peerID] = lastMatch
	}
	rn.nextIdx[peerID] = lastMatch + 1
}

// DecrementNextIdx 刀一: 标准Raft回退探测 — follower拒绝时递减nextIdx，禁止跳回1
func (rn *RaftNode) DecrementNextIdx(peerID string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.nextIdx[peerID] > 1 {
		rn.nextIdx[peerID]--
	}
}

// GetNextIdx 获取指定 follower 的 nextIdx
func (rn *RaftNode) GetNextIdx(peerID string) int64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.nextIdx[peerID]
}

// R-04修复A: UpdatePeerClient 更新指定 peer 的 gRPC 客户端（重连回调调用）
func (rn *RaftNode) UpdatePeerClient(peerID string, client pb.RaftServiceClient) {
	rn.peerClientsMu.Lock()
	rn.peerClients[peerID] = client
	rn.peerClientsMu.Unlock()
	rn.logf("[raft/%s] R-04修复A: peer %s 客户端已更新（重连回调）", rn.id, peerID)
}

// R-04修复A: GetPeerClient 线程安全获取指定 peer 的 gRPC 客户端
func (rn *RaftNode) GetPeerClient(peerID string) (pb.RaftServiceClient, bool) {
	rn.peerClientsMu.RLock()
	defer rn.peerClientsMu.RUnlock()
	client, ok := rn.peerClients[peerID]
	return client, ok
}

// R-04修复B: MarkFollowerDegraded 标记 follower 降级（批量同步放弃）
func (rn *RaftNode) MarkFollowerDegraded(peerID string) {
	rn.mu.Lock()
	if !rn.degradedFollowers[peerID] {
		rn.degradedFollowers[peerID] = true
		rn.logf("[raft/%s] R-04修复B [WARN] follower %s 标记降级（批量同步放弃）", rn.id, peerID)
	}
	rn.mu.Unlock()
}

// R-04修复B: ClearFollowerDegraded 清除 follower 降级标记（同步成功）
func (rn *RaftNode) ClearFollowerDegraded(peerID string) {
	rn.mu.Lock()
	if rn.degradedFollowers[peerID] {
		delete(rn.degradedFollowers, peerID)
		rn.logf("[raft/%s] R-04修复B: follower %s 降级已清除（同步恢复）", rn.id, peerID)
	}
	rn.mu.Unlock()
}

// R-04修复C: CheckGapAlerts 检查 gap 持续告警（gap>阈值持续10s → ERROR日志）
func (rn *RaftNode) CheckGapAlerts() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.state != StateLeader {
		return
	}
	threshold := int64(100)
	if rn.batchSyncMgr != nil {
		threshold = rn.batchSyncMgr.config.LagThreshold
	}
	for _, p := range rn.peers {
		gap := rn.commitIdx - rn.matchIdx[p.ID]
		if gap > threshold {
			if _, exists := rn.gapSince[p.ID]; !exists {
				rn.gapSince[p.ID] = time.Now()
			}
			if time.Since(rn.gapSince[p.ID]) > 10*time.Second {
				rn.logf("[raft/%s] R-04修复C [ERROR] follower %s gap=%d 持续>10s (commitIdx=%d, matchIdx=%d, degraded=%v)",
					rn.id, p.ID, gap, rn.commitIdx, rn.matchIdx[p.ID], rn.degradedFollowers[p.ID])
			}
		} else {
			delete(rn.gapSince, p.ID)
		}
	}
}

// R-04修复C: FollowerGaps 返回每个 follower 的 commit gap
func (rn *RaftNode) FollowerGaps() map[string]int64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	gaps := make(map[string]int64, len(rn.peers))
	for _, p := range rn.peers {
		gaps[p.ID] = rn.commitIdx - rn.matchIdx[p.ID]
	}
	return gaps
}

// R-04修复C: DegradedFollowers 返回降级 follower 列表
func (rn *RaftNode) DegradedFollowers() []string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	result := make([]string, 0)
	for id, degraded := range rn.degradedFollowers {
		if degraded {
			result = append(result, id)
		}
	}
	return result
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

func (rn *RaftNode) IsWALGateClosed() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.walGateClosed
}

// SetOnCommit 设置日志提交回调
// 当 Raft 日志被提交（commitIdx 前进时）调用此回调
// 用于接入 WAL 加密持久化 + TiDB/MySQL 异步落盘管线
func (rn *RaftNode) SetOnCommit(fn func(RaftLog)) {
	rn.mu.Lock()
	rn.onCommit = fn
	rn.mu.Unlock()
}

// CompactLogs 快照后日志压缩：切片截断 logs[upToIndex:] 物理释放内存
// 前移 logStartIndex = upToIndex + 1，GetLog/GetLogEntries 对 < logStartIndex 的请求返回 ErrCompacted
func (rn *RaftNode) CompactLogs(upToIndex int64) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if upToIndex <= 0 || upToIndex < rn.logStartIndex {
		return
	}

	cutCount := int(upToIndex - rn.logStartIndex + 1)
	if cutCount > len(rn.logs) {
		cutCount = len(rn.logs)
	}

	rn.logs = rn.logs[cutCount:]
	rn.logStartIndex = upToIndex + 1

	rn.logf("[raft/%s] 日志压缩: 切片截断至 index=%d, logStartIndex=%d, 物理释放 %d 条日志",
		rn.id, upToIndex, rn.logStartIndex, cutCount)
}

// collectCommittedLogs 收集从 oldCommit+1 到 commitIdx 的已提交日志
// 调用前必须持有 rn.mu 锁
func (rn *RaftNode) collectCommittedLogs(oldCommit int64) []RaftLog {
	if rn.onCommit == nil || rn.commitIdx <= oldCommit {
		return nil
	}
	var logs []RaftLog
	for i := oldCommit + 1; i <= rn.commitIdx; i++ {
		e := rn.logAtLocked(i)
		if e != nil {
			logs = append(logs, *e)
		}
	}
	return logs
}

// fireOnCommit 触发已提交日志的回调
func (rn *RaftNode) fireOnCommit(logs []RaftLog) {

	if rn.onCommit == nil || len(logs) == 0 {
		return
	}
	for _, log := range logs {
		rn.onCommit(log)
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

	rn.stats.Lock()
	rn.stats.HeartbeatLostCount++
	rn.stats.Unlock()

	if !rn.walReplayCompleted {
		rn.mu.Unlock()
		rn.logf("[raft/%s] 重放未完成拒绝选举", rn.id)
		rn.electionTimer.Reset(randomElectionTimeout() * 2)
		return
	}

	if rn.walGateClosed {
		rn.mu.Unlock()
		rn.logf("[raft/%s] WAL门禁关闭，拒绝选举", rn.id)
		rn.electionTimer.Reset(randomElectionTimeout() * 2)
		return
	}

	// V2.3: 无 peer 的节点不发起选举（等待被动态加入集群）
	if len(rn.peers) == 0 {
		rn.mu.Unlock()
		rn.electionTimer.Reset(randomElectionTimeout() * 10)
		return
	}

	// 新节点日志未追上 Leader 时，不发起选举，给 Leader 更多时间同步日志
	// 豁免条件：
	//   1. 从未见过 Leader（lastHeartbeat zero，全新集群）
	//   2. Leader 心跳过期（>2s，需接任）— batch22: 从 5s 降至 2s 匹配 800-1200ms 选举超时
	//   3. 连续3次选举失败（选举风暴自愈，强制突破）
	if !rn.logCaughtUp && !rn.lastHeartbeat.IsZero() && time.Since(rn.lastHeartbeat) < 2*time.Second &&
		rn.candidateFailCount < 3 {
		rn.mu.Unlock()
		rn.logf("[raft/%s] 日志尚未追上 Leader，跳过选举 (logs=%d, failCount=%d)", rn.id, len(rn.logs), rn.candidateFailCount)
		rn.electionTimer.Reset(randomElectionTimeout() * 2)
		return
	}

	// 选举风暴自愈：连续3次失败后强制突破 logCaughtUp，允许发起选举
	if !rn.logCaughtUp && rn.candidateFailCount >= 3 {
		rn.logCaughtUp = true
		rn.logf("[raft/%s] ⚡ 选举超时突破: 连续 %d 次失败，强制 logCaughtUp=true", rn.id, rn.candidateFailCount)
	}

	// batch23: pre-vote 探测 — 获 quorum 预支持才转 Candidate，防选票分裂
	rn.stats.Lock()
	rn.stats.PreVoteRoundCount++
	rn.stats.Unlock()
	preVoteTerm := atomic.LoadInt64(&rn.term) + 1
	lastLogIdx := rn.lastLogIndexLocked()
	var lastLogTm int64
	if len(rn.logs) > 0 {
		lastLogTm = rn.logs[len(rn.logs)-1].Term
	}
	peersSnapshot := make([]PeerInfo, len(rn.peers))
	copy(peersSnapshot, rn.peers)
	rn.mu.Unlock()

	if !rn.preVoteProbe(preVoteTerm, lastLogIdx, lastLogTm, peersSnapshot) {
		rn.logf("[raft/%s] pre-vote 未获 quorum，放弃本轮选举", rn.id)
		rn.electionTimer.Reset(randomElectionTimeout())
		return
	}

	rn.mu.Lock()
	// 进入 Candidate 状态
	rn.state = StateCandidate
	rn.electionEventCount.Add(1)
	rn.stats.Lock()
	rn.stats.ElectionRoundCount++
	rn.stats.Unlock()
	currentTerm := atomic.AddInt64(&rn.term, 1)
	rn.votedFor = rn.id
	rn.leaderID = ""
	rn.electionStart = time.Now()

	// 选举风暴自愈：记录首次 Candidate 时间窗口
	if rn.candidateFailCount == 0 {
		rn.firstCandidateTime = time.Now()
	}

	peers := make([]PeerInfo, len(rn.peers))
	copy(peers, rn.peers)
	rn.mu.Unlock()

	rn.logf("[raft/%s] 选举超时触发 → Candidate, term=%d", rn.id, currentTerm)

	// 向所有 peer 并发发送 RequestVote
	rn.requestVotes(currentTerm, peers)

	// Fix #5: 重置选举计时器移到投票收集之后，避免在投票期间触发新选举导致term被改
	rn.electionTimer.Reset(randomElectionTimeout())
}

// =========================================================================
// 并发投票请求
// =========================================================================

func (rn *RaftNode) requestVotes(term int64, peers []PeerInfo) {
	votesNeeded := rn.config.quorumSize() // V2.3: 动态 quorum（支持联合共识）
	votesGranted := int32(1)              // 自己投自己一票
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
			client, ok := rn.GetPeerClient(p.ID)
			if !ok {
				rn.logf("[raft/%s] 未找到 peer %s 的 gRPC 客户端", rn.id, p.ID)
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
			defer cancel()

			req := &pb.RequestVoteRequest{
				Term:         term,
				CandidateId:  rn.id,
				LastLogIndex: rn.getLastLogIndex(),
				LastLogTerm:  rn.getLastLogTerm(),
			}

			resp, err := client.RequestVote(ctx, req)
			if err != nil {
				rn.logf("[raft/%s] RequestVote → %s 失败: %v", rn.id, p.ID, err)
				return
			}

			// 处理响应中的 term
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
		slogInfo("leader_elected", "赢得选举", map[string]interface{}{
			"votes_granted": votesGranted,
			"votes_needed":  peersCount + 1,
			"term":          term,
			"elapsed_ms":    time.Since(rn.electionStart).Milliseconds(),
		})

		// 切换到 Leader
		rn.state = StateLeader
		rn.leaderID = rn.id

		// 选举风暴自愈：重置计数器
		rn.candidateFailCount = 0
		rn.firstCandidateTime = time.Time{}

		// 初始化 Leader 状态
		lastLogIdx := rn.lastLogIndexLocked()
		for _, p := range rn.peers {
			rn.nextIdx[p.ID] = lastLogIdx + 1
			rn.matchIdx[p.ID] = 0
		}

		// F4b: 追加 no-op 条目，确保新 Leader 当选后可安全提交前 term 条目
		noOpIndex := rn.lastLogIndexLocked() + 1
		rn.logs = append(rn.logs, RaftLog{
			Index:   noOpIndex,
			Term:    rn.term,
			Command: nil,
		})
		rn.stats.Lock()
		rn.stats.LogCount = len(rn.logs)
		rn.stats.Unlock()
		rn.logf("[raft/%s] no-op 条目已追加: index=%d term=%d", rn.id, noOpIndex, rn.term)

		// 启动心跳循环
		go rn.heartbeatLoop()

		// batch11: 启动 group commit 攒批（锁内调用）
		rn.startProposeBatchLocked()

		// TCX-Ⅳ: 启动批量同步管理器
		if rn.batchSyncMgr != nil {
			rn.batchSyncMgr.Start()
		}

		rn.updateStats()
	} else {
		rn.logf("[raft/%s] 选举失败 (得票 %d/%d, term=%d)",
			rn.id, votesGranted, peersCount+1, term)
		slogWarn("election_lost", "选举失败", map[string]interface{}{
			"votes_granted": votesGranted,
			"votes_needed":  peersCount + 1,
			"term":          term,
		})

		// 选举风暴自愈：连续3次以上选举失败且在60秒窗口内，强制突破 logCaughtUp 死锁
		rn.candidateFailCount++
		if rn.candidateFailCount >= 3 && time.Since(rn.firstCandidateTime) <= 60*time.Second {
			if !rn.logCaughtUp {
				rn.logCaughtUp = true
				rn.logf("[raft/%s] ⚡ 选举风暴自愈: 连续 %d 次选举失败，强制突破 logCaughtUp 死锁", rn.id, rn.candidateFailCount)
			}
		}
	}
}

// =========================================================================
// batch23: pre-vote 探测 — 防选票分裂
// =========================================================================

func (rn *RaftNode) preVoteProbe(term int64, lastLogIdx int64, lastLogTm int64, peers []PeerInfo) bool {
	votesNeeded := rn.config.quorumSize()
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

			addr, ok := rn.peerHttpAddrs[p.ID]
			if !ok {
				return
			}

			reqBody, _ := json.Marshal(map[string]interface{}{
				"term":           term,
				"candidate_id":   rn.id,
				"last_log_index": lastLogIdx,
				"last_log_term":  lastLogTm,
			})

			client := preVoteHTTPClient
			resp, err := client.Post("http://"+addr+"/raft/pre_vote", "application/json", bytes.NewReader(reqBody))
			if err != nil {
				return
			}
			defer resp.Body.Close()

			var result struct {
				Term           int64 `json:"term"`
				PreVoteGranted bool  `json:"pre_vote_granted"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return
			}
			if result.PreVoteGranted {
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

// HandlePreVote — pre-vote 服务端处理，复用 HandleRequestVote 检查但不更新状态
func (rn *RaftNode) HandlePreVote(term int64, candidateId string, lastLogIndex int64, lastLogTerm int64) (int64, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	respTerm := rn.term
	granted := false

	// 【检查1】term 倒退拒绝
	if term < rn.term {
		return respTerm, false
	}

	// 【检查2】候选者不在当前配置中
	if rn.config != nil {
		allNodes := rn.config.allNodes()
		if len(allNodes) > 1 && !stringInSlice(candidateId, allNodes) {
			return respTerm, false
		}
	}

	// 注意：跳过检查3（stepDown）— pre-vote 不更新 term/state
	// 注意：跳过检查5（votedFor 冲突）— pre-vote 不检查 votedFor

	// 【检查4】WAL 重放未完成 / 门禁关闭
	if !rn.walReplayCompleted {
		return respTerm, false
	}
	if rn.walGateClosed {
		return respTerm, false
	}

	// 【检查6】logCaughtUp 检查（含选举风暴自愈突破）
	if !rn.logCaughtUp && rn.candidateFailCount >= 3 && !rn.firstCandidateTime.IsZero() && time.Since(rn.firstCandidateTime) <= 60*time.Second {
		// pre-vote 中不修改 logCaughtUp，仅检查
	}
	if !rn.logCaughtUp && !rn.lastHeartbeat.IsZero() {
		return respTerm, false
	}

	// 【检查7】空日志候选者禁止当选
	if lastLogIndex == 0 && rn.commitIdx > 0 {
		return respTerm, false
	}

	// 【检查8】log up-to-date 检查
	lastIdx := rn.lastLogIndexLocked()
	var localLastTerm int64
	if lastIdx > 0 {
		localLastTerm = rn.logs[len(rn.logs)-1].Term
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
// 降级为 Follower（收到更高 term 的消息时）
// =========================================================================

func (rn *RaftNode) stepDown(higherTerm int64) {
	rn.mu.Lock()

	if higherTerm <= rn.term {
		rn.mu.Unlock()
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
	needStopBatch := false
	if oldState == StateLeader {
		select {
		case rn.heartbeatStop <- struct{}{}:
		default:
		}
		needStopBatch = rn.proposeBatchOn
	}

	// 重置选举计时器
	rn.electionTimer.Reset(randomElectionTimeout())

	// Fix Bug C: 收集需要锁外执行的回调
	var oldBatchSyncMgr *BatchSyncManager
	if oldState == StateLeader && rn.batchSyncMgr != nil {
		oldBatchSyncMgr = rn.batchSyncMgr
		rn.batchSyncMgr = nil
	}

	rn.updateStats()
	rn.mu.Unlock()

	// 锁外停止批量同步管理器（避免死锁：Stop()等待SyncLoop，SyncLoop需要RLock）
	if oldBatchSyncMgr != nil {
		oldBatchSyncMgr.Stop()
		rn.mu.Lock()
		rn.batchSyncMgr = NewBatchSyncManager(rn, oldBatchSyncMgr.config)
		rn.mu.Unlock()
	}

	// batch11: 锁外停止 group commit 攒批（避免死锁：StopProposeBatch 获取 rn.mu）
	if needStopBatch {
		rn.StopProposeBatch()
	}
}

// notifyCommit 非阻塞投递提交推进信号（V2.5.1 B1修复：Propose 去自旋用）
// commitIdx 前进时调用；cap=1 + select+default 保证永不阻塞。
func (rn *RaftNode) notifyCommit() {
	select {
	case rn.commitNotify <- struct{}{}:
	default:
	}
}

// =========================================================================
// Leader 心跳循环
// =========================================================================

func (rn *RaftNode) heartbeatLoop() {
	ticker := time.NewTicker(rn.currentHeartbeatInterval)
	defer ticker.Stop()

	rn.logf("[raft/%s] 心跳循环启动 (间隔: %v)", rn.id, rn.currentHeartbeatInterval)

	for {
		select {
		case <-rn.shutdownCh:
			rn.logf("[raft/%s] 心跳循环关闭 (shutdown)", rn.id)
			return

		case <-rn.heartbeatStop:
			rn.logf("[raft/%s] 心跳循环停止 (降级)", rn.id)
			return

		case <-rn.replicateCh:
			rn.sendHeartbeats()

		case <-ticker.C:
			rn.sendHeartbeats()
			newInterval := rn.adaptiveHeartbeatAdjust()
			if newInterval != rn.currentHeartbeatInterval {
				rn.currentHeartbeatInterval = newInterval
				rn.heartbeatIntervalAtomic.Store(int64(newInterval))
				ticker.Reset(newInterval)
				rn.logf("[raft/%s] 自适应心跳调整: %v", rn.id, newInterval)
			}
		}
	}
}

func (rn *RaftNode) sendHeartbeats() {

	rn.mu.RLock()
	if rn.state != StateLeader {
		rn.mu.RUnlock()
		return
	}
	term := rn.term
	leaderCommit := rn.commitIdx
	peers := make([]PeerInfo, len(rn.peers))
	copy(peers, rn.peers)
	nextIdxSnapshot := make(map[string]int64, len(rn.nextIdx))
	for k, v := range rn.nextIdx {
		nextIdxSnapshot[k] = v
	}
	// V2.5.1 B2修复: 不再全量拷贝 rn.logs（消除 O(N) 每心跳拷贝风暴——E08 OOM 主犯）。
	// 锁内为每个 peer 构造增量 entries 切片 + prevLog 快照，构造完立即解锁再发送（持锁不做网络 IO）。
	logEnd := rn.lastLogIndexLocked()
	type peerPlan struct {
		prevIdx       int64
		prevTerm      int64
		entries       []*pb.LogEntry
		needsSnapshot bool // follower nextIdx < logStartIndex，需走快照路径
	}
	plans := make(map[string]*peerPlan, len(peers))
	for _, p := range peers {
		pp := &peerPlan{}
		start := nextIdxSnapshot[p.ID]
		if start < rn.logStartIndex {
			pp.needsSnapshot = true
			plans[p.ID] = pp
			continue
		}
		if start > rn.logStartIndex {
			pp.prevIdx = start - 1
			if e := rn.logAtLocked(pp.prevIdx); e != nil {
				pp.prevTerm = e.Term
			}
		}
		if start <= logEnd {
			n := int(logEnd - start + 1)
			pp.entries = make([]*pb.LogEntry, 0, n)
			for i := start; i <= logEnd; i++ {
				if l := rn.logAtLocked(i); l != nil {
					pp.entries = append(pp.entries, &pb.LogEntry{
						Term:    l.Term,
						Index:   l.Index,
						Command: l.Command,
						Sm3Hash: l.SM3Hash,
					})
				}
			}
		}
		plans[p.ID] = pp
	}
	rn.mu.RUnlock()

	var timeoutCount int32
	var successCount int32

	for _, peer := range peers {
		go func(p PeerInfo) {
			defer func() {
				if r := recover(); r != nil {
					rn.logf("[raft/%s] AppendEntries RPC panic recovered: %v", rn.id, r)
				}
			}()
			client, ok := rn.GetPeerClient(p.ID)
			if !ok {
				return
			}

			plan := plans[p.ID]

			if plan.needsSnapshot {
				if rn.batchSyncMgr != nil {
					rn.batchSyncMgr.sendSnapshot(p.ID)
				} else if rn.getSnapshotData != nil {
					rn.sendInstallSnapshot(p.ID, term, leaderCommit)
				}
				return
			}

			prevLogIdx := plan.prevIdx
			prevLogTerm := plan.prevTerm

			// V2.5.1 B2: entries 已在锁内按 nextIdx 增量构造完毕，此处直接引用（消除 O(N) 拷贝）
			entries := plan.entries

			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
			defer cancel()

			req := &pb.AppendEntriesRequest{
				Term:         term,
				LeaderId:     rn.id,
				PrevLogIndex: prevLogIdx,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}

			resp, err := client.AppendEntries(ctx, req)
			if err != nil {
				atomic.AddInt32(&timeoutCount, 1)
				return
			}

			atomic.AddInt32(&successCount, 1)

			if resp.Term > term {
				rn.stepDown(resp.Term)
				return
			}

			// V2.3: 根据响应更新 nextIdx / matchIdx
			if resp.Success {
				rn.mu.Lock()
				if rn.state == StateLeader && atomic.LoadInt64(&rn.term) == term {
					newMatch := logEnd
					if newMatch > rn.matchIdx[p.ID] {
						rn.matchIdx[p.ID] = newMatch
					}
					rn.nextIdx[p.ID] = newMatch + 1
				}
				rn.mu.Unlock()
				rn.advanceCommit(term)
			} else {
				rn.mu.Lock()
				if rn.nextIdx[p.ID] > 1 {
					// T017: 快速回退优化 — 指数回退（proto 无 ConflictIndex，用指数回退替代逐条递减 O(log N)）
					gap := rn.nextIdx[p.ID] - 1
					if gap > 1 {
						rn.nextIdx[p.ID] -= gap / 2
					} else {
						rn.nextIdx[p.ID]--
					}
				}
				rn.mu.Unlock()
			}
		}(peer)
	}

	// advanceCommit 兜底（follower goroutine 中已调用，此处防止竞态遗漏）
	rn.advanceCommit(term)

	if timeoutCount > 0 {
		atomic.AddInt32(&rn.consecutiveTimeouts, int32(timeoutCount))
		atomic.StoreInt32(&rn.consecutiveSuccess, 0)
	} else {
		atomic.AddInt32(&rn.consecutiveSuccess, 1)
		atomic.StoreInt32(&rn.consecutiveTimeouts, 0)
	}
}

// advanceCommit 尝试推进 commitIdx（每个 follower 应答后调用，不等所有 follower）
func (rn *RaftNode) advanceCommit(term int64) {
	var committedLogs []RaftLog
	rn.mu.Lock()
	if rn.state == StateLeader && atomic.LoadInt64(&rn.term) == term {
		oldPeers := rn.config.oldPeers()
		newPeers := rn.config.newPeers()
		for N := rn.lastLogIndexLocked(); N > rn.commitIdx; N-- {
			oldOK := true
			newOK := true
			if len(oldPeers) > 0 {
				oldCount := 1
				for _, p := range rn.peers {
					if stringInSlice(p.ID, oldPeers) && rn.matchIdx[p.ID] >= N {
						oldCount++
					}
				}
				oldOK = oldCount >= len(oldPeers)/2+1
			}
			if len(newPeers) > 0 {
				newCount := 1
				for _, p := range rn.peers {
					if stringInSlice(p.ID, newPeers) && rn.matchIdx[p.ID] >= N {
						newCount++
					}
				}
				newOK = newCount >= len(newPeers)/2+1
			}
			if oldOK && newOK {
				// T018: Figure 8 校验 — 仅当前 term 日志可直接提交，旧 term 日志在新 term 日志提交后间接提交
				if e := rn.logAtLocked(N); e == nil || e.Term != rn.term {
					continue
				}
				oldCommit := rn.commitIdx
				rn.commitIdx = N
				rn.applyConfigChangesLocked(oldCommit, N)
				committedLogs = rn.collectCommittedLogs(oldCommit)
				rn.lastApplied = rn.commitIdx
				rn.notifyCommit()
				rn.commitBroadcast.Notify(rn.commitIdx)
				break
			}
		}
		rn.updateStats()
	}
	rn.mu.Unlock()
	if len(committedLogs) > 0 {
		rn.fireOnCommit(committedLogs)
	}
}

// =========================================================================
// batch11: group commit 攒批层
// =========================================================================

// StartProposeBatch 启动 group commit 攒批循环（Leader 当选后调用）
func (rn *RaftNode) StartProposeBatch() {
	rn.mu.Lock()
	rn.startProposeBatchLocked()
	rn.mu.Unlock()
}

// startProposeBatchLocked 锁内版本（调用前必须持有 rn.mu）
func (rn *RaftNode) startProposeBatchLocked() {
	if rn.proposeBatchOn {
		return
	}
	rn.proposeBatchOn = true

	rn.proposeBatchWg.Add(1)
	go rn.proposeBatchLoop()
	rn.replicateWg.Add(1)
	go rn.replicateLoop()
	rn.logf("[raft/%s] group commit 攒批已启动 (batchSize=%d, batchWindow=%v)", rn.id, rn.proposeBatchSize, rn.proposeBatchWin)
}

// StopProposeBatch 停止 group commit 攒批循环
func (rn *RaftNode) StopProposeBatch() {
	rn.mu.Lock()
	if !rn.proposeBatchOn {
		rn.mu.Unlock()
		return
	}
	rn.proposeBatchOn = false
	rn.mu.Unlock()

	close(rn.proposeBatchClose)
	rn.proposeBatchWg.Wait()
	rn.proposeBatchClose = make(chan struct{})
	close(rn.replicateStop)
	rn.replicateWg.Wait()
	rn.replicateStop = make(chan struct{})
	rn.logf("[raft/%s] group commit 攒批已停止", rn.id)
}

// replicateLoop batch19: 专用复制循环（仅信号触发，heartbeatLoop 50ms ticker 兜底）
func (rn *RaftNode) replicateLoop() {
	defer rn.replicateWg.Done()
	for {
		select {
		case <-rn.shutdownCh:
			return
		case <-rn.replicateStop:
			return
		case <-rn.replicateTrigger:
			for {
				select {
				case <-rn.replicateTrigger:
				default:
					goto send
				}
			}
		send:
			rn.sendHeartbeats()
		}
	}
}

// proposeBatchLoop group commit 攒批主循环
func (rn *RaftNode) proposeBatchLoop() {
	defer rn.proposeBatchWg.Done()

	batch := make([]*proposeRequest, 0, rn.proposeBatchSize)
	timer := time.NewTimer(rn.proposeBatchWin)
	defer timer.Stop()

	// batch18 T2: 自适应 flush 超时
	adaptiveWin := func() time.Duration {
		if rn.inFlightUtilFn != nil {
			util := rn.inFlightUtilFn()
			if util < 0.5 {
				return 1 * time.Millisecond // 低负载快速 flush
			}
		}
		return rn.proposeBatchWin // 高负载保持 2ms
	}

	flush := func() {
		if len(batch) > 0 {
			rn.proposeBatchFlush(batch)
			batch = make([]*proposeRequest, 0, rn.proposeBatchSize)
		}
		timer.Reset(adaptiveWin())
	}

	for {
		select {
		case <-rn.proposeBatchClose:
			for {
				select {
				case req := <-rn.proposeBatchCh:
					batch = append(batch, req)
					if len(batch) >= rn.proposeBatchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		case req := <-rn.proposeBatchCh:
			batch = append(batch, req)
			if len(batch) >= rn.proposeBatchSize {
				flush()
			}
		case <-timer.C:
			flush()
		}
	}
}

// proposeBatchFlush 批量追加日志 + 立即触发复制
func (rn *RaftNode) proposeBatchFlush(batch []*proposeRequest) {
	n := len(batch)
	tFlush := time.Now().UnixMicro()

	// batch18 T1: 锁外预构造日志切片，减少锁持有时间
	rn.mu.RLock()
	term := rn.term
	isLeader := rn.state == StateLeader && !rn.walGateClosed
	baseIdx := rn.lastLogIndexLocked()
	rn.mu.RUnlock()

	if !isLeader {
		rn.mu.RLock()
		leader := rn.leaderID
		rn.mu.RUnlock()
		for _, req := range batch {
			req.resultCh <- proposeResult{err: fmt.Errorf("not leader: current leader is %s", leader)}
		}
		return
	}

	// 预构造日志条目（锁外）
	batchLogs := make([]RaftLog, n)
	for i, req := range batch {
		batchLogs[i] = RaftLog{
			Index:   baseIdx + int64(i+1),
			Term:    term,
			Command: req.command,
		}
	}

	// 锁内仅一次 append（O(1) 摊还）
	rn.mu.Lock()
	if rn.state != StateLeader || rn.walGateClosed {
		rn.mu.Unlock()
		leader := rn.leaderID
		for _, req := range batch {
			req.resultCh <- proposeResult{err: fmt.Errorf("not leader: current leader is %s", leader)}
		}
		return
	}
	// 校正 baseIdx（锁内实际长度可能已变）
	actualBase := rn.lastLogIndexLocked()
	if actualBase != baseIdx {
		for i := range batchLogs {
			batchLogs[i].Index = actualBase + int64(i+1)
		}
		baseIdx = actualBase
	}
	rn.logs = append(rn.logs, batchLogs...)
	logCount := len(rn.logs)
	rn.mu.Unlock()

	// stats 更新移出写锁
	rn.stats.Lock()
	rn.stats.LogCount = logCount
	rn.stats.Unlock()

	// 立即触发复制（不等 50ms 心跳 ticker）
	tRepl := time.Now().UnixMicro()
	// batch19: 专用复制循环信号（替换 batch18 T3 的 CAS 触发）
	select {
	case rn.replicateTrigger <- struct{}{}:
	default:
	}

	// 记录批统计
	rn.batchStatsMu.Lock()
	rn.batchCountTotal++
	rn.batchSizeTotal += int64(n)
	if n <= 64 {
		rn.batchSizeHist[n-1]++
	} else {
		rn.batchSizeHist[64]++
	}
	rn.batchStatsMu.Unlock()

	// 各请求按 index 精确等待 commit
	for i, req := range batch {
		index := baseIdx + int64(i+1)
		req.tFlush = tFlush
		req.tRepl = tRepl
		go rn.waitForCommit(index, req.resultCh, req)
	}
}

// waitForCommit 等待指定 index 被 commit 后返回结果
func (rn *RaftNode) waitForCommit(index int64, resultCh chan proposeResult, req *proposeRequest) {
	commitTimer := time.NewTimer(3 * time.Second)
	defer commitTimer.Stop()
	fallbackTicker := time.NewTicker(2 * time.Millisecond)
	defer fallbackTicker.Stop()

	for {
		rn.mu.RLock()
		committed := rn.commitIdx >= index
		stillLeader := rn.state == StateLeader
		rn.mu.RUnlock()

		if committed {
			if globalLatencyDecomp != nil && globalLatencyDecomp.enabled.Load() && req != nil {
				tCommit := time.Now().UnixMicro()
				batchWait := req.tFlush - req.tEnqueue
				batchFlush := req.tRepl - req.tFlush
				quorumWait := tCommit - req.tRepl
				if batchWait < 0 {
					batchWait = 0
				}
				if batchFlush < 0 {
					batchFlush = 0
				}
				if quorumWait < 0 {
					quorumWait = 0
				}
				globalLatencyDecomp.Record(batchWait, batchFlush, quorumWait, 0)
			}
			resultCh <- proposeResult{index: index}
			return
		}
		if !stillLeader {
			resultCh <- proposeResult{err: fmt.Errorf("lost leadership while waiting for commit at index=%d", index)}
			return
		}

		commitCh := rn.commitBroadcast.WaitCh()
		select {
		case <-commitCh:
		case <-fallbackTicker.C:
		case <-commitTimer.C:
			resultCh <- proposeResult{err: fmt.Errorf("commit timeout: index=%d not committed after 3s", index)}
			return
		case <-rn.shutdownCh:
			resultCh <- proposeResult{err: fmt.Errorf("node shutdown while waiting for commit at index=%d", index)}
			return
		}
	}
}

// BatchStats 返回 group commit 批统计
func (rn *RaftNode) BatchStats() (batchCount, batchSizeTotal int64, hist [65]int64) {
	rn.batchStatsMu.Lock()
	defer rn.batchStatsMu.Unlock()
	return rn.batchCountTotal, rn.batchSizeTotal, rn.batchSizeHist
}

// =========================================================================
// Propose — 客户端写入入口
// Leader直接appendLog → sendHeartbeats(复制) → 等待多数派commit → 返回
// =========================================================================

func (rn *RaftNode) Propose(command []byte) (int64, error) {
	// batch11: group commit 路径（攒批 → 批量追加 → 立即复制 → 按 index 精确等待）
	rn.mu.RLock()
	batchOn := rn.proposeBatchOn
	rn.mu.RUnlock()

	if batchOn {
		req := &proposeRequest{
			command:  command,
			resultCh: make(chan proposeResult, 1),
			tEnqueue: time.Now().UnixMicro(),
		}
		select {
		case rn.proposeBatchCh <- req:
		default:
			return 0, fmt.Errorf("propose batch channel full")
		}
		result := <-req.resultCh
		return result.index, result.err
	}

	// 原始路径（攒批未启用时的 fallback）
	rn.mu.Lock()
	if rn.state != StateLeader {
		leader := rn.leaderID
		rn.mu.Unlock()
		return 0, fmt.Errorf("not leader: current leader is %s", leader)
	}
	if rn.walGateClosed {
		rn.mu.Unlock()
		return 0, fmt.Errorf("WAL gate closed")
	}

	index := rn.lastLogIndexLocked() + 1
	entry := RaftLog{
		Index:   index,
		Term:    rn.term,
		Command: command,
	}
	rn.logs = append(rn.logs, entry)

	rn.stats.Lock()
	rn.stats.LogCount = len(rn.logs)
	rn.stats.Unlock()

	rn.mu.Unlock()

	rn.logf("[raft/%s] Propose: index=%d term=%d cmd_len=%d", rn.id, index, entry.Term, len(command))

	// V2.5.1 B1修复: Propose 去自旋——不再循环调 sendHeartbeats（消除写请求对心跳全量拷贝的放大，
	// E08 OOM 主放大器）。commit 由心跳 ticker(50ms) 的 sendHeartbeats 自然推进并 notifyCommit；
	// 此处 select: notify 快速路径 + 50ms ticker 兜底（纯 O(1) commitIdx 检查，不触发任何拷贝），
	// 1s timer 保留原 ~1s 等待上限语义。
	commitTimer := time.NewTimer(3 * time.Second)
	defer commitTimer.Stop()
	fallbackTicker := time.NewTicker(2 * time.Millisecond)
	defer fallbackTicker.Stop()
	for {
		rn.mu.RLock()
		committed := rn.commitIdx >= index
		stillLeader := rn.state == StateLeader
		rn.mu.RUnlock()

		if committed {
			return index, nil
		}
		if !stillLeader {
			return 0, fmt.Errorf("lost leadership while waiting for commit at index=%d", index)
		}

		commitCh := rn.commitBroadcast.WaitCh()
		select {
		case <-commitCh:
		case <-fallbackTicker.C:
		case <-commitTimer.C:
			return 0, fmt.Errorf("commit timeout: index=%d not committed after 3s", index)
		}
	}
}

// GetLog 返回指定索引的日志条目（供客户端读取校验）
func (rn *RaftNode) GetLog(index int64) (RaftLog, bool) {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	if index < rn.logStartIndex {
		return RaftLog{}, false
	}
	e := rn.logAtLocked(index)
	if e == nil {
		return RaftLog{}, false
	}
	return *e, true
}

// V2.3: 应用已提交的配置变更（调用前必须持有 rn.mu 写锁）
func (rn *RaftNode) applyConfigChangesLocked(from, to int64) {
	for i := from + 1; i <= to; i++ {
		e := rn.logAtLocked(i)
		if e == nil {
			continue
		}
		if !isConfigEntry(e.Command) {
			continue
		}
		change, ok := decodeConfigChange(e.Command)
		if !ok {
			continue
		}
		rn.config.commitConfigChange(change)
		if change.Type == ConfigAdd {
			if _, exists := rn.peerAddrs[change.NodeID]; !exists && change.NodeID != rn.id {
				rn.addPeerDynamic(change.NodeID, change.Address)
			}
		}
		if change.Type == ConfigRemove {
			if _, exists := rn.peerAddrs[change.NodeID]; exists {
				rn.removePeerDynamic(change.NodeID)
			}
		}
		rn.logf("[raft/%s] [membership] 配置变更已提交并应用: index=%d, type=%d, node=%s, C_new=%v",
			rn.id, i, change.Type, change.NodeID, change.NewPeers)
	}
}

// =========================================================================
// gRPC 服务端处理 — RequestVote
// =========================================================================

func (rn *RaftNode) HandleRequestVote(
	ctx context.Context, req *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {
	rn.mu.Lock()

	resp := &pb.RequestVoteResponse{Term: rn.term, VoteGranted: false}

	if req.Term < rn.term {
		resp.Term = rn.term
		rn.mu.Unlock()
		return resp, nil
	}

	// V2.3: 拒绝不在当前配置中的候选者的投票请求（防止被移除的节点扰乱集群）
	if rn.config != nil {
		allNodes := rn.config.allNodes()
		if len(allNodes) > 1 && !stringInSlice(req.CandidateId, allNodes) {
			rn.mu.Unlock()
			return resp, nil
		}
	}

	if req.Term > rn.term {
		if req.Term-rn.term > 10000 {
			rn.logf("[raft/%s] 拦截异常高 Term 飙升: 候选者 Term=%d, 本地 Term=%d, 差额=%d, 更新term并拒绝投票",
				rn.id, req.Term, rn.term, req.Term-rn.term)
			rn.term = req.Term
			rn.state = StateFollower
			rn.votedFor = ""
			rn.leaderID = ""
			rn.electionTimer.Reset(randomElectionTimeout())
			resp.Term = req.Term
			rn.mu.Unlock()
			return resp, nil
		}
		rn.term = req.Term
		rn.state = StateFollower
		rn.votedFor = ""
		rn.leaderID = ""
		rn.electionTimer.Reset(randomElectionTimeout())
		resp.Term = req.Term
	}

	if !rn.walReplayCompleted {
		rn.logf("[raft/%s] 拒绝投票给 %s: WAL重放未完成", rn.id, req.CandidateId)
		rn.mu.Unlock()
		return resp, nil
	}

	if rn.walGateClosed {
		rn.logf("[raft/%s] 拒绝投票给 %s: WAL门禁关闭", rn.id, req.CandidateId)
		rn.mu.Unlock()
		return resp, nil
	}

	if rn.votedFor != "" && rn.votedFor != req.CandidateId {
		rn.mu.Unlock()
		return resp, nil
	}

	// 严格拦截规则 1：自身日志未追上 Leader 的新节点不参与投票
	// 豁免：从未见过 Leader（lastHeartbeat zero，全新集群）时允许投票给首个候选者，避免选举死锁
	// 选举风暴自愈（自身突破）：连续3次以上选举失败且在60秒窗口内，强制突破 logCaughtUp 死锁
	if !rn.logCaughtUp && rn.candidateFailCount >= 3 && !rn.firstCandidateTime.IsZero() && time.Since(rn.firstCandidateTime) <= 60*time.Second {
		rn.logCaughtUp = true
		rn.logf("[raft/%s] ⚡ 投票死锁突破(自身): 连续 %d 次选举失败(窗口内)，强制 logCaughtUp=true",
			rn.id, rn.candidateFailCount)
	}
	// 选举风暴自愈（协同突破）：候选者 term 高出 3+，说明其已经历多轮选举失败
	// 当前节点强制突破 logCaughtUp，允许投票给该候选者 —— 解决9节点跨机房死锁
	if !rn.logCaughtUp && req.Term-rn.term >= 3 && !rn.lastHeartbeat.IsZero() && time.Since(rn.lastHeartbeat) > 3*time.Second {
		rn.logCaughtUp = true
		rn.logf("[raft/%s] ⚡ 投票死锁突破(协同): 候选者 %s term=%d 高出 %d，强制 logCaughtUp=true",
			rn.id, req.CandidateId, req.Term, req.Term-rn.term)
	}
	if !rn.logCaughtUp && !rn.lastHeartbeat.IsZero() {
		rn.logf("[raft/%s] 拒绝投票给 %s: 自身日志尚未追上 Leader (logCaughtUp=false)",
			rn.id, req.CandidateId)
		rn.mu.Unlock()
		return resp, nil
	}

	// 严格拦截规则 2：空日志候选者在集群已有提交日志时绝对禁止当选
	if req.LastLogIndex == 0 && rn.commitIdx > 0 {
		rn.logf("[raft/%s] 拒绝投票给 %s: 候选者日志为空但集群已提交到 index=%d",
			rn.id, req.CandidateId, rn.commitIdx)
		rn.mu.Unlock()
		return resp, nil
	}

	lastIdx := rn.lastLogIndexLocked()
	var lastTerm int64
	if lastIdx > 0 {
		lastTerm = rn.logs[len(rn.logs)-1].Term
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
// gRPC 服务端处理 — AppendEntries (心跳 + 日志复制)
// =========================================================================

func (rn *RaftNode) HandleAppendEntries(
	ctx context.Context, req *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {
	rn.mu.Lock()

	resp := &pb.AppendEntriesResponse{Term: rn.term, Success: false}

	if rn.walGateClosed {
		atomic.AddInt64(&rn.rejectedWriteCount, 1)
		rn.mu.Unlock()
		return resp, nil
	}

	if req.Term < rn.term {
		resp.Term = rn.term
		rn.mu.Unlock()
		return resp, nil
	}

	if req.Term > rn.term {
		rn.term = req.Term
		rn.state = StateFollower
		rn.votedFor = ""
	} else if req.Term == rn.term && rn.state == StateLeader {
		// Fix #4: 同Term的Leader拒绝AppendEntries — Raft保证同Term最多一个Leader，
		// 收到同Term AppendEntries说明对端自称Leader，属异常，拒绝以保护日志完整性
		rn.mu.Unlock()
		return resp, nil
	} else if req.Term == rn.term && rn.state == StateCandidate {
		// V2.3: 同Term收到Leader有效心跳：Candidate回正为Follower
		rn.state = StateFollower
		rn.votedFor = ""
	}

	// V2.3: 拒绝不在当前配置中的 Leader（已被移除的节点不能继续当 Leader）
	// 但新节点（配置只有自己）可以接受任何 Leader 的 AppendEntries
	if rn.config != nil {
		allNodes := rn.config.allNodes()
		if len(allNodes) > 1 && !stringInSlice(req.LeaderId, allNodes) {
			rn.mu.Unlock()
			return resp, nil
		}
	}

	rn.electionTimer.Reset(randomElectionTimeout())
	rn.leaderID = req.LeaderId
	rn.lastHeartbeat = time.Now() // 收到 Leader 消息即刷新心跳（含心跳/日志同步）

	if len(req.Entries) == 0 {
		var committedLogs []RaftLog
		// Fix #2: 仅Follower才根据LeaderCommit推进commitIdx，防止外部请求直接推进
		if req.LeaderCommit > rn.commitIdx && rn.state == StateFollower {
			oldCommit := rn.commitIdx
			lastLogIdx := rn.lastLogIndexLocked()
			if req.LeaderCommit < lastLogIdx {
				rn.commitIdx = req.LeaderCommit
			} else {
				rn.commitIdx = lastLogIdx
			}
			rn.lastApplied = rn.commitIdx
			committedLogs = rn.collectCommittedLogs(oldCommit)
			rn.applyConfigChangesLocked(oldCommit, rn.commitIdx) // V2.3
		}
		// 日志追上 Leader 后，允许参与选举
		if !rn.logCaughtUp && req.LeaderCommit > 0 && rn.lastLogIndexLocked() >= req.LeaderCommit {
			rn.logCaughtUp = true
			rn.logf("[raft/%s] 日志已追上 Leader (logs=%d, leaderCommit=%d)，允许参与选举",
				rn.id, len(rn.logs), req.LeaderCommit)
		}
		resp.Success = true
		rn.updateStats()
		rn.mu.Unlock()
		rn.fireOnCommit(committedLogs)
		return resp, nil
	}

	lastLogIdx := rn.lastLogIndexLocked()

	if req.PrevLogIndex > lastLogIdx {
		rn.mu.Unlock()
		return resp, nil
	}
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex < rn.logStartIndex {
			// 压缩感知：follower 已通过快照拥有该前缀，视为匹配成功
		} else if e := rn.logAtLocked(req.PrevLogIndex); e == nil || e.Term != req.PrevLogTerm {
			rn.mu.Unlock()
			return resp, nil
		}
	}

	for _, entry := range req.Entries {
		if entry.Index <= lastLogIdx {
			if e := rn.logAtLocked(entry.Index); e != nil {
				if e.Term != entry.Term {
					rn.logs = rn.logs[:entry.Index-rn.logStartIndex]
				}
			}
		}
		if entry.Index > rn.lastLogIndexLocked() {
			// T020: 截断后重算 SM3Hash，保证链式校验完整性
			logEntry := RaftLog{
				Index:   entry.Index,
				Term:    entry.Term,
				Command: entry.Command,
				SM3Hash: entry.Sm3Hash,
			}
			if sm3IntegrityEnabled {
				var prevHash []byte
				if len(rn.logs) > 0 {
					prevHash = rn.logs[len(rn.logs)-1].SM3Hash
				}
				if recomputed := ComputeEntrySM3(prevHash, entry.Term, entry.Index, entry.Command); recomputed != nil {
					logEntry.SM3Hash = recomputed
				}
			}
			rn.logs = append(rn.logs, logEntry)
		}
	}

	var committedLogs []RaftLog
	// Fix #2: 仅Follower才根据LeaderCommit推进commitIdx，防止外部请求直接推进
	if req.LeaderCommit > rn.commitIdx && rn.state == StateFollower {
		oldCommit := rn.commitIdx
		newLast := rn.lastLogIndexLocked()
		if req.LeaderCommit < newLast {
			rn.commitIdx = req.LeaderCommit
		} else {
			rn.commitIdx = newLast
		}
		rn.lastApplied = rn.commitIdx
		committedLogs = rn.collectCommittedLogs(oldCommit)
		rn.applyConfigChangesLocked(oldCommit, rn.commitIdx) // V2.3
	}

	// 日志追上 Leader 后，允许参与选举
	if !rn.logCaughtUp && req.LeaderCommit > 0 && rn.lastLogIndexLocked() >= req.LeaderCommit {
		rn.logCaughtUp = true
		rn.logf("[raft/%s] 日志已追上 Leader (logs=%d, leaderCommit=%d)，允许参与选举",
			rn.id, len(rn.logs), req.LeaderCommit)
	}

	resp.Success = true
	rn.updateStats()
	rn.mu.Unlock()
	rn.fireOnCommit(committedLogs)

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
	slogInfo("state_transition", fmt.Sprintf("%s→%s", oldState, newState), map[string]interface{}{
		"from": oldState.String(),
		"to":   newState.String(),
		"term": rn.Term(),
	})
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
	return rn.lastLogIndexLocked()
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
