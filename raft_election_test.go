package main

import (
	"testing"
)

// TestElection_StartIdxNotOneAfterElection
// 选举验证: Leader当选后，IdentifyLaggingFollowers的StartIdx不应为1
// 旧代码: matchIdx=0 → StartIdx=1（全量重传）
// 新代码: nextIdx=lastLogIdx+1 → StartIdx=lastLogIdx+1（乐观估计，无需同步）
func TestElection_StartIdxNotOneAfterElection(t *testing.T) {
	rn := &RaftNode{
		id:        "node-1",
		state:     StateLeader,
		term:      10,
		logs:      make([]RaftLog, 10000),
		commitIdx: 10000,
		nextIdx:   make(map[string]int64),
		matchIdx:  make(map[string]int64),
		peers:     []PeerInfo{{ID: "node-2"}, {ID: "node-3"}, {ID: "node-4"}, {ID: "node-5"}},
	}
	for i := range rn.logs {
		rn.logs[i] = RaftLog{Index: int64(i + 1), Term: 10, Command: []byte("cmd")}
	}

	// 模拟 Leader 当选初始化（raft.go:760-764）
	lastLogIdx := int64(len(rn.logs))
	for _, p := range rn.peers {
		rn.nextIdx[p.ID] = lastLogIdx + 1
		rn.matchIdx[p.ID] = 0
	}

	lagging := rn.IdentifyLaggingFollowers()

	// 期望: gap = commitIdx - (nextIdx-1) = 10000 - 10000 = 0，不触发batch sync
	if len(lagging) != 0 {
		for _, f := range lagging {
			t.Errorf("follower %s 不应触发batch sync: StartIdx=%d Gap=%d（nextIdx乐观估计已跟上）",
				f.PeerID, f.StartIdx, f.Gap)
		}
	}
	t.Log("✓ Leader当选后nextIdx=lastLogIdx+1，gap=0，不触发全量重传")
}

// TestElection_FollowerBehind100Entries
// 选举验证: follower落后100条时，StartIdx应为合理值（lastLogIdx-99），而非1
func TestElection_FollowerBehind100Entries(t *testing.T) {
	rn := &RaftNode{
		id:        "node-1",
		state:     StateLeader,
		term:      10,
		logs:      make([]RaftLog, 10000),
		commitIdx: 10000,
		nextIdx:   make(map[string]int64),
		matchIdx:  make(map[string]int64),
		peers:     []PeerInfo{{ID: "node-2"}},
	}
	for i := range rn.logs {
		rn.logs[i] = RaftLog{Index: int64(i + 1), Term: 10, Command: []byte("cmd")}
	}

	// 模拟心跳探测后发现follower落后101条
	// nextIdx已被心跳递减到 9900（follower有9899条）
	rn.nextIdx["node-2"] = 9900
	rn.matchIdx["node-2"] = 9899

	lagging := rn.IdentifyLaggingFollowers()

	if len(lagging) == 0 {
		t.Fatal("follower落后101条应触发batch sync")
	}

	f := lagging[0]
	if f.StartIdx != 9900 {
		t.Errorf("StartIdx=%d, 期望 9900（nextIdx）", f.StartIdx)
	}
	if f.Gap != 101 {
		t.Errorf("Gap=%d, 期望 101", f.Gap)
	}
	if f.StartIdx == 1 {
		t.Error("StartIdx=1 是旧bug（全量重传），选举修复后不应出现")
	}
	t.Logf("✓ follower落后101条: StartIdx=%d, Gap=%d, 仅传增量而非全量", f.StartIdx, f.Gap)
}

// TestElection_ElectionStormNoStartIdxOne
// 选举验证: 模拟3次连续选举，每次选举后StartIdx都不为1
func TestElection_ElectionStormNoStartIdxOne(t *testing.T) {
	rn := &RaftNode{
		id:        "node-1",
		state:     StateLeader,
		term:      10,
		logs:      make([]RaftLog, 50000),
		commitIdx: 50000,
		nextIdx:   make(map[string]int64),
		matchIdx:  make(map[string]int64),
		peers:     []PeerInfo{{ID: "node-2"}, {ID: "node-3"}},
	}
	for i := range rn.logs {
		rn.logs[i] = RaftLog{Index: int64(i + 1), Term: 10, Command: []byte("cmd")}
	}

	// 模拟3次连续选举
	for election := 1; election <= 3; election++ {
		lastLogIdx := int64(len(rn.logs))
		for _, p := range rn.peers {
			rn.nextIdx[p.ID] = lastLogIdx + 1
			rn.matchIdx[p.ID] = 0
		}

		lagging := rn.IdentifyLaggingFollowers()

		for _, f := range lagging {
			if f.StartIdx == 1 {
				t.Errorf("选举%d: follower %s StartIdx=1（旧bug），应为 %d",
					election, f.PeerID, lastLogIdx+1)
			}
		}
		t.Logf("✓ 选举%d: 无StartIdx=1，nextIdx=%d", election, lastLogIdx+1)
	}
}

// TestElection_DecrementNextIdx
// 选举验证: DecrementNextIdx 正确递减且不低于1
func TestElection_DecrementNextIdx(t *testing.T) {
	rn := &RaftNode{
		nextIdx: make(map[string]int64),
	}
	rn.nextIdx["node-2"] = 100

	rn.DecrementNextIdx("node-2")
	if rn.GetNextIdx("node-2") != 99 {
		t.Errorf("DecrementNextIdx: got %d, want 99", rn.GetNextIdx("node-2"))
	}

	// 递减到1后不再递减
	rn.nextIdx["node-2"] = 1
	rn.DecrementNextIdx("node-2")
	if rn.GetNextIdx("node-2") != 1 {
		t.Errorf("DecrementNextIdx at 1: got %d, want 1 (不应低于1)", rn.GetNextIdx("node-2"))
	}
	t.Log("✓ DecrementNextIdx 正确递减且不低于1")
}
