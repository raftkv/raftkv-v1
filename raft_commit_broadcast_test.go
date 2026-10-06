package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCommitBroadcast_NotifyWakesAll(t *testing.T) {
	cb := newCommitBroadcaster()

	const N = 100
	var wg sync.WaitGroup
	wg.Add(N)
	woken := int32(0)

	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			ch := cb.WaitCh()
			<-ch
			atomic.AddInt32(&woken, 1)
		}()
	}

	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&woken) != 0 {
		t.Fatalf("goroutine 在 Notify 前被唤醒")
	}

	cb.Notify(1)
	wg.Wait()

	if atomic.LoadInt32(&woken) != N {
		t.Errorf("期望 %d goroutine 被唤醒, 实际 %d", N, atomic.LoadInt32(&woken))
	}
}

func TestCommitBroadcast_CurrentCommitIdx(t *testing.T) {
	cb := newCommitBroadcaster()

	if cb.CurrentCommitIdx() != 0 {
		t.Errorf("初始 commitIdx 应为 0")
	}

	cb.Notify(5)
	if cb.CurrentCommitIdx() != 5 {
		t.Errorf("Notify(5) 后 commitIdx 应为 5, 实际 %d", cb.CurrentCommitIdx())
	}

	cb.Notify(3)
	if cb.CurrentCommitIdx() != 5 {
		t.Errorf("Notify(3) 后 commitIdx 应保持 5, 实际 %d", cb.CurrentCommitIdx())
	}

	cb.Notify(10)
	if cb.CurrentCommitIdx() != 10 {
		t.Errorf("Notify(10) 后 commitIdx 应为 10, 实际 %d", cb.CurrentCommitIdx())
	}
}

func TestCommitBroadcast_MultipleNotifyGenerations(t *testing.T) {
	cb := newCommitBroadcaster()

	ch1 := cb.WaitCh()
	cb.Notify(1)
	<-ch1

	ch2 := cb.WaitCh()
	if ch1 == ch2 {
		t.Error("Notify 后 WaitCh 应返回新 channel")
	}

	cb.Notify(2)
	<-ch2

	ch3 := cb.WaitCh()
	select {
	case <-ch3:
	default:
	}
}

func TestGroupCommit_BatchQuorumConfirm(t *testing.T) {
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
	rn.config = NewClusterConfig([]string{"node-1", "node-2", "node-3", "node-4", "node-5"}, nil)

	for i := 0; i < 10; i++ {
		rn.logs = append(rn.logs, RaftLog{Index: int64(i + 1), Term: 10, Command: []byte("cmd")})
	}

	rn.matchIdx["node-2"] = 10
	rn.matchIdx["node-3"] = 10

	const N = 10
	var wg sync.WaitGroup
	wg.Add(N)
	committed := int32(0)

	for i := 1; i <= N; i++ {
		go func(idx int64) {
			defer wg.Done()
			ch := rn.commitBroadcast.WaitCh()
			select {
			case <-ch:
				rn.mu.RLock()
				ok := rn.commitIdx >= idx
				rn.mu.RUnlock()
				if ok {
					atomic.AddInt32(&committed, 1)
				}
			case <-time.After(1 * time.Second):
			}
		}(int64(i))
	}

	time.Sleep(50 * time.Millisecond)
	rn.advanceCommit(10)
	wg.Wait()

	if atomic.LoadInt32(&committed) != N {
		t.Errorf("期望 %d 请求被通知已提交, 实际 %d", N, atomic.LoadInt32(&committed))
	}
}

func TestGroupCommit_LostLeadership(t *testing.T) {
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

	rn.logs = append(rn.logs, RaftLog{Index: 1, Term: 10, Command: []byte("cmd")})

	resultCh := make(chan proposeResult, 1)
	req := &proposeRequest{
		command:  []byte("cmd"),
		resultCh: resultCh,
		tEnqueue: time.Now().UnixMicro(),
	}

	go rn.waitForCommit(1, resultCh, req)

	time.Sleep(50 * time.Millisecond)

	rn.mu.Lock()
	rn.state = StateFollower
	rn.leaderID = "node-2"
	rn.mu.Unlock()

	rn.commitBroadcast.Notify(0)

	select {
	case res := <-resultCh:
		if res.err == nil {
			t.Error("失去 leadership 后应返回错误")
		}
	case <-time.After(2 * time.Second):
		t.Error("waitForCommit 未在 2s 内返回")
	}
}

func TestReplicateLoop_StartStop(t *testing.T) {
	rn := &RaftNode{
		id:                       "node-1",
		state:                    StateFollower,
		term:                     10,
		logs:                     make([]RaftLog, 0),
		commitIdx:                0,
		nextIdx:                  make(map[string]int64),
		matchIdx:                 make(map[string]int64),
		peers:                    []PeerInfo{{ID: "node-2"}, {ID: "node-3"}},
		stats:                    &RaftStats{ID: "node-1", State: "Follower", Term: 10, PeerCount: 2},
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

	rn.replicateWg.Add(1)
	go rn.replicateLoop()

	rn.replicateTrigger <- struct{}{}

	time.Sleep(50 * time.Millisecond)

	close(rn.replicateStop)
	rn.replicateWg.Wait()
}
