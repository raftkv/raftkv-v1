package main

import (
	"sync"
	"testing"
	"time"
)

func TestBatch11_ProposeBatch_ReducedLockAcquisition(t *testing.T) {
	rn := &RaftNode{
		id:        "node-1",
		state:     StateLeader,
		term:      10,
		logs:      make([]RaftLog, 0),
		commitIdx: 0,
		nextIdx:   make(map[string]int64),
		matchIdx:  make(map[string]int64),
		peers:     []PeerInfo{{ID: "node-2"}, {ID: "node-3"}, {ID: "node-4"}, {ID: "node-5"}},
		stats: &RaftStats{
			ID:        "node-1",
			State:     "Leader",
			Term:      10,
			LeaderID:  "node-1",
			PeerCount: 4,
		},

		commitNotify:             make(chan struct{}, 1),
		commitBroadcast:          newCommitBroadcaster(),
		replicateTrigger:         make(chan struct{}, 256),
		replicateStop:            make(chan struct{}),
		shutdownCh:               make(chan struct{}),
		proposeBatchCh:           make(chan *proposeRequest, 1024),
		replicateCh:              make(chan struct{}, 1),
		proposeBatchClose:        make(chan struct{}),
		proposeBatchSize:         64,
		proposeBatchWin:          2 * time.Millisecond,
		currentHeartbeatInterval: 50 * time.Millisecond,
	}

	const M = 100
	var wg sync.WaitGroup
	wg.Add(M)

	for i := 0; i < M; i++ {
		go func() {
			defer wg.Done()
			req := &proposeRequest{
				command:  []byte("test"),
				resultCh: make(chan proposeResult, 1),
			}
			rn.proposeBatchCh <- req
			<-req.resultCh
		}()
	}

	batch := make([]*proposeRequest, 0, 64)
	timer := time.NewTimer(2 * time.Millisecond)
	defer timer.Stop()

	collected := 0
	for collected < M {
		select {
		case req := <-rn.proposeBatchCh:
			batch = append(batch, req)
			if len(batch) >= 64 {
				rn.proposeBatchFlush(batch)
				collected += len(batch)
				batch = make([]*proposeRequest, 0, 64)
				timer.Reset(2 * time.Millisecond)
			}
		case <-timer.C:
			if len(batch) > 0 {
				rn.proposeBatchFlush(batch)
				collected += len(batch)
				batch = make([]*proposeRequest, 0, 64)
			}
			timer.Reset(2 * time.Millisecond)
		}
	}

	wg.Wait()

	if len(rn.logs) != M {
		t.Errorf("期望 %d 条日志, 实际 %d", M, len(rn.logs))
	}

	batchCount, batchSizeTotal, _ := rn.BatchStats()
	if batchCount == 0 {
		t.Error("批次数不应为 0")
	}
	if batchSizeTotal != int64(M) {
		t.Errorf("总攒批条数期望 %d, 实际 %d", M, batchSizeTotal)
	}
	t.Logf("M=%d 请求, batchCount=%d, avg batch size=%.1f", M, batchCount, float64(batchSizeTotal)/float64(batchCount))
}

func TestBatch11_SinglePropose_LatencyNotDegrade(t *testing.T) {
	rn := &RaftNode{
		id:        "node-1",
		state:     StateLeader,
		term:      10,
		logs:      make([]RaftLog, 0),
		commitIdx: 0,
		nextIdx:   make(map[string]int64),
		matchIdx:  make(map[string]int64),
		peers:     []PeerInfo{{ID: "node-2"}},
		stats:     &RaftStats{ID: "node-1", State: "Leader", Term: 10, PeerCount: 1},

		commitNotify:             make(chan struct{}, 1),
		commitBroadcast:          newCommitBroadcaster(),
		replicateTrigger:         make(chan struct{}, 256),
		replicateStop:            make(chan struct{}),
		shutdownCh:               make(chan struct{}),
		proposeBatchCh:           make(chan *proposeRequest, 1024),
		replicateCh:              make(chan struct{}, 1),
		proposeBatchClose:        make(chan struct{}),
		proposeBatchSize:         64,
		proposeBatchWin:          2 * time.Millisecond,
		currentHeartbeatInterval: 50 * time.Millisecond,
	}

	req := &proposeRequest{
		command:  []byte("single"),
		resultCh: make(chan proposeResult, 1),
	}

	start := time.Now()
	rn.proposeBatchCh <- req

	batch := []*proposeRequest{req}
	rn.proposeBatchFlush(batch)

	go func() {
		time.Sleep(10 * time.Millisecond)
		rn.mu.Lock()
		rn.commitIdx = 1
		rn.mu.Unlock()
		select {
		case rn.commitNotify <- struct{}{}:
		default:
		}
	}()

	result := <-req.resultCh
	elapsed := time.Since(start)

	if result.err != nil {
		t.Fatalf("单条 Propose 失败: %v", result.err)
	}
	if result.index != 1 {
		t.Errorf("期望 index=1, 实际 index=%d", result.index)
	}

	maxAllowed := 5*time.Millisecond + 10*time.Millisecond
	if elapsed > maxAllowed {
		t.Errorf("单条延迟 %v 超过上限 %v", elapsed, maxAllowed)
	}
	t.Logf("单条 Propose 延迟: %v", elapsed)
}

func TestBatch11_BatchFlush_TriggersImmediateReplication(t *testing.T) {
	rn := &RaftNode{
		id:        "node-1",
		state:     StateLeader,
		term:      10,
		logs:      make([]RaftLog, 0),
		commitIdx: 0,
		nextIdx:   make(map[string]int64),
		matchIdx:  make(map[string]int64),
		peers:     []PeerInfo{{ID: "node-2"}, {ID: "node-3"}, {ID: "node-4"}, {ID: "node-5"}},
		stats:     &RaftStats{ID: "node-1", State: "Leader", Term: 10, PeerCount: 4},

		commitNotify:             make(chan struct{}, 1),
		commitBroadcast:          newCommitBroadcaster(),
		replicateTrigger:         make(chan struct{}, 256),
		replicateStop:            make(chan struct{}),
		shutdownCh:               make(chan struct{}),
		proposeBatchCh:           make(chan *proposeRequest, 1024),
		replicateCh:              make(chan struct{}, 1),
		proposeBatchClose:        make(chan struct{}),
		proposeBatchSize:         64,
		proposeBatchWin:          2 * time.Millisecond,
		currentHeartbeatInterval: 50 * time.Millisecond,
	}

	batch := make([]*proposeRequest, 0, 10)
	for i := 0; i < 10; i++ {
		batch = append(batch, &proposeRequest{
			command:  []byte("cmd"),
			resultCh: make(chan proposeResult, 1),
		})
	}

	rn.proposeBatchFlush(batch)

	select {
	case <-rn.replicateTrigger:
	default:
		t.Error("proposeBatchFlush 后应触发 replicateTrigger")
	}

	if len(rn.logs) != 10 {
		t.Errorf("期望 10 条日志, 实际 %d", len(rn.logs))
	}
}

func TestBatch11_BatchFlush_NotLeader_ReturnsError(t *testing.T) {
	rn := &RaftNode{
		id:       "node-1",
		state:    StateFollower,
		term:     10,
		logs:     make([]RaftLog, 0),
		leaderID: "node-2",
		nextIdx:  make(map[string]int64),
		matchIdx: make(map[string]int64),
		peers:    []PeerInfo{{ID: "node-2"}},
		stats:    &RaftStats{ID: "node-1", State: "Follower", Term: 10, PeerCount: 1},

		commitNotify:             make(chan struct{}, 1),
		commitBroadcast:          newCommitBroadcaster(),
		replicateTrigger:         make(chan struct{}, 256),
		replicateStop:            make(chan struct{}),
		shutdownCh:               make(chan struct{}),
		proposeBatchCh:           make(chan *proposeRequest, 1024),
		replicateCh:              make(chan struct{}, 1),
		proposeBatchClose:        make(chan struct{}),
		proposeBatchSize:         64,
		proposeBatchWin:          2 * time.Millisecond,
		currentHeartbeatInterval: 50 * time.Millisecond,
	}

	batch := []*proposeRequest{{
		command:  []byte("test"),
		resultCh: make(chan proposeResult, 1),
	}}

	rn.proposeBatchFlush(batch)

	result := <-batch[0].resultCh
	if result.err == nil {
		t.Error("非 Leader 应返回错误")
	}
}

func TestBatch11_BatchStats_TracksHistogram(t *testing.T) {
	rn := &RaftNode{
		id:       "node-1",
		state:    StateLeader,
		term:     10,
		logs:     make([]RaftLog, 0),
		nextIdx:  make(map[string]int64),
		matchIdx: make(map[string]int64),
		peers:    []PeerInfo{{ID: "node-2"}, {ID: "node-3"}, {ID: "node-4"}, {ID: "node-5"}},
		stats:    &RaftStats{ID: "node-1", State: "Leader", Term: 10, PeerCount: 4},

		commitNotify:             make(chan struct{}, 1),
		commitBroadcast:          newCommitBroadcaster(),
		replicateTrigger:         make(chan struct{}, 256),
		replicateStop:            make(chan struct{}),
		shutdownCh:               make(chan struct{}),
		proposeBatchCh:           make(chan *proposeRequest, 1024),
		replicateCh:              make(chan struct{}, 1),
		proposeBatchClose:        make(chan struct{}),
		proposeBatchSize:         64,
		proposeBatchWin:          2 * time.Millisecond,
		currentHeartbeatInterval: 50 * time.Millisecond,
	}

	for i := 0; i < 3; i++ {
		batch := make([]*proposeRequest, 0, 5)
		for j := 0; j < 5; j++ {
			batch = append(batch, &proposeRequest{
				command:  []byte("cmd"),
				resultCh: make(chan proposeResult, 1),
			})
		}
		rn.proposeBatchFlush(batch)
	}

	batchCount, batchSizeTotal, hist := rn.BatchStats()
	if batchCount != 3 {
		t.Errorf("期望 3 批, 实际 %d", batchCount)
	}
	if batchSizeTotal != 15 {
		t.Errorf("期望 15 条, 实际 %d", batchSizeTotal)
	}
	if hist[4] != 3 {
		t.Errorf("期望 hist[4]=3 (3 批各 5 条), 实际 hist[4]=%d", hist[4])
	}
}

func TestBatch11_WaitForCommit_CommitAdvances(t *testing.T) {
	rn := &RaftNode{
		id:        "node-1",
		state:     StateLeader,
		term:      10,
		logs:      make([]RaftLog, 0),
		commitIdx: 0,
		nextIdx:   make(map[string]int64),
		matchIdx:  make(map[string]int64),
		peers:     []PeerInfo{{ID: "node-2"}},
		stats:     &RaftStats{ID: "node-1", State: "Leader", Term: 10, PeerCount: 1},

		commitNotify:             make(chan struct{}, 1),
		commitBroadcast:          newCommitBroadcaster(),
		replicateTrigger:         make(chan struct{}, 256),
		replicateStop:            make(chan struct{}),
		shutdownCh:               make(chan struct{}),
		proposeBatchCh:           make(chan *proposeRequest, 1024),
		replicateCh:              make(chan struct{}, 1),
		proposeBatchClose:        make(chan struct{}),
		proposeBatchSize:         64,
		proposeBatchWin:          2 * time.Millisecond,
		currentHeartbeatInterval: 50 * time.Millisecond,
	}

	resultCh := make(chan proposeResult, 1)
	go rn.waitForCommit(1, resultCh, nil)

	time.Sleep(20 * time.Millisecond)
	rn.mu.Lock()
	rn.commitIdx = 1
	rn.mu.Unlock()
	select {
	case rn.commitNotify <- struct{}{}:
	default:
	}

	result := <-resultCh
	if result.err != nil {
		t.Errorf("期望 commit 成功, 错误: %v", result.err)
	}
	if result.index != 1 {
		t.Errorf("期望 index=1, 实际 %d", result.index)
	}
}

func TestBatch11_WaitForCommit_LostLeadership(t *testing.T) {
	rn := &RaftNode{
		id:        "node-1",
		state:     StateLeader,
		term:      10,
		logs:      make([]RaftLog, 0),
		commitIdx: 0,
		nextIdx:   make(map[string]int64),
		matchIdx:  make(map[string]int64),
		peers:     []PeerInfo{{ID: "node-2"}},
		stats:     &RaftStats{ID: "node-1", State: "Leader", Term: 10, PeerCount: 1},

		commitNotify:             make(chan struct{}, 1),
		commitBroadcast:          newCommitBroadcaster(),
		replicateTrigger:         make(chan struct{}, 256),
		replicateStop:            make(chan struct{}),
		shutdownCh:               make(chan struct{}),
		proposeBatchCh:           make(chan *proposeRequest, 1024),
		replicateCh:              make(chan struct{}, 1),
		proposeBatchClose:        make(chan struct{}),
		proposeBatchSize:         64,
		proposeBatchWin:          2 * time.Millisecond,
		currentHeartbeatInterval: 50 * time.Millisecond,
	}

	resultCh := make(chan proposeResult, 1)
	go rn.waitForCommit(1, resultCh, nil)

	time.Sleep(20 * time.Millisecond)
	rn.mu.Lock()
	rn.state = StateFollower
	rn.mu.Unlock()
	select {
	case rn.commitNotify <- struct{}{}:
	default:
	}

	result := <-resultCh
	if result.err == nil {
		t.Error("丢失 Leader 后应返回错误")
	}
}
