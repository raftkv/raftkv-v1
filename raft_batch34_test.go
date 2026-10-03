package main

import (
	pb "raftkv/proto"
	"testing"
	"time"
)

func TestElectionStateMachine_T022(t *testing.T) {
	rn := NewRaftNode("test-node", map[string]string{}, nil, nil)

	if rn.state != StateFollower {
		t.Fatalf("初始状态应为 Follower, 实际 %d", rn.state)
	}

	rn.mu.Lock()
	rn.state = StateCandidate
	rn.term = 5
	rn.mu.Unlock()
	if rn.state != StateCandidate {
		t.Fatalf("转为 Candidate 失败")
	}

	rn.mu.Lock()
	rn.state = StateLeader
	rn.mu.Unlock()
	if rn.state != StateLeader {
		t.Fatalf("转为 Leader 失败")
	}

	higherTerm := int64(10)
	rn.stepDown(higherTerm)
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	if rn.state != StateFollower {
		t.Errorf("stepDown 后应为 Follower, 实际 %d", rn.state)
	}
	if rn.term != higherTerm {
		t.Errorf("stepDown 后 term 应为 %d, 实际 %d", higherTerm, rn.term)
	}
}

func TestElectionTimeoutDistribution_T022(t *testing.T) {
	const samples = 100
	min := int64(electionTimeoutMin)
	max := int64(electionTimeoutMax)

	for i := 0; i < samples; i++ {
		timeout := randomElectionTimeout().Milliseconds()
		if timeout < min || timeout > max {
			t.Errorf("选举超时 %dms 超出范围 [%d, %d]", timeout, min, max)
		}
	}

	belowMin := 0
	aboveMid := 0
	mid := (min + max) / 2
	for i := 0; i < samples; i++ {
		timeout := randomElectionTimeout().Milliseconds()
		if timeout < mid {
			belowMin++
		} else {
			aboveMid++
		}
	}
	if belowMin == 0 || aboveMid == 0 {
		t.Errorf("选举超时分布不均匀: 低于中位数 %d, 高于中位数 %d", belowMin, aboveMid)
	}
}

func TestPreVoteNoSideEffects_T023(t *testing.T) {
	rn := NewRaftNode("test-node", map[string]string{}, nil, nil)

	rn.mu.Lock()
	originalTerm := rn.term
	originalState := rn.state
	originalVotedFor := rn.votedFor
	rn.mu.Unlock()

	respTerm, granted := rn.HandlePreVote(1, "candidate-1", 0, 0)

	rn.mu.RLock()
	defer rn.mu.RUnlock()

	_ = respTerm
	_ = granted

	if rn.term != originalTerm {
		t.Errorf("HandlePreVote 不应修改 term: 原始 %d, 当前 %d", originalTerm, rn.term)
	}
	if rn.state != originalState {
		t.Errorf("HandlePreVote 不应修改 state: 原始 %d, 当前 %d", originalState, rn.state)
	}
	if rn.votedFor != originalVotedFor {
		t.Errorf("HandlePreVote 不应修改 votedFor: 原始 %s, 当前 %s", originalVotedFor, rn.votedFor)
	}
}

func TestPreVoteTermReject_T023(t *testing.T) {
	rn := NewRaftNode("test-node", map[string]string{}, nil, nil)

	rn.mu.Lock()
	rn.term = 5
	rn.mu.Unlock()

	_, granted := rn.HandlePreVote(3, "candidate-1", 0, 0)
	if granted {
		t.Errorf("term 倒退应拒绝 pre-vote")
	}
}

func TestVoteConstraintVotedFor_T024(t *testing.T) {
	rn := NewRaftNode("test-node", map[string]string{}, nil, nil)

	rn.mu.Lock()
	rn.term = 5
	rn.votedFor = "candidate-A"
	rn.logCaughtUp = true
	rn.mu.Unlock()

	req := &pb.RequestVoteRequest{
		Term:         5,
		CandidateId:  "candidate-B",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp, err := rn.HandleRequestVote(nil, req)
	if err != nil {
		t.Fatalf("HandleRequestVote 错误: %v", err)
	}
	if resp.VoteGranted {
		t.Errorf("同一 term 已投给 A，应拒绝 B 的投票请求")
	}
}

func TestVoteConstraintLogUpToDate_T024(t *testing.T) {
	rn := NewRaftNode("test-node", map[string]string{}, nil, nil)

	rn.mu.Lock()
	rn.term = 5
	rn.logs = []RaftLog{
		{Index: 1, Term: 3, Command: []byte("cmd1")},
		{Index: 2, Term: 5, Command: []byte("cmd2")},
	}
	rn.logCaughtUp = true
	rn.mu.Unlock()

	req := &pb.RequestVoteRequest{
		Term:         6,
		CandidateId:  "candidate-B",
		LastLogIndex: 1,
		LastLogTerm:  3,
	}
	resp, err := rn.HandleRequestVote(nil, req)
	if err != nil {
		t.Fatalf("HandleRequestVote 错误: %v", err)
	}
	if resp.VoteGranted {
		t.Errorf("候选者日志落后 (idx=1,term=3 vs local idx=2,term=5)，应拒绝投票")
	}
}

func TestVoteConstraintLogCaughtUp_T024(t *testing.T) {
	rn := NewRaftNode("test-node", map[string]string{}, nil, nil)

	rn.mu.Lock()
	rn.term = 5
	rn.logCaughtUp = false
	rn.lastHeartbeat = time.Now()
	rn.mu.Unlock()

	req := &pb.RequestVoteRequest{
		Term:         6,
		CandidateId:  "candidate-B",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp, err := rn.HandleRequestVote(nil, req)
	if err != nil {
		t.Fatalf("HandleRequestVote 错误: %v", err)
	}
	if resp.VoteGranted {
		t.Errorf("logCaughtUp=false 应拒绝投票")
	}
}

func TestAppendEntriesConflictIndex_T025(t *testing.T) {
	rn := NewRaftNode("test-node", map[string]string{}, nil, nil)

	rn.mu.Lock()
	rn.term = 5
	rn.logs = []RaftLog{
		{Index: 1, Term: 3, Command: []byte("cmd1")},
		{Index: 2, Term: 3, Command: []byte("cmd2")},
		{Index: 3, Term: 5, Command: []byte("cmd3")},
	}
	rn.state = StateFollower
	rn.mu.Unlock()

	req := &pb.AppendEntriesRequest{
		Term:         5,
		LeaderId:     "leader-1",
		PrevLogIndex: 5,
		PrevLogTerm:  5,
		Entries: []*pb.LogEntry{
			{Term: 5, Index: 6, Command: []byte("cmd6")},
		},
		LeaderCommit: 0,
	}
	resp, err := rn.HandleAppendEntries(nil, req)
	if err != nil {
		t.Fatalf("HandleAppendEntries 错误: %v", err)
	}
	if resp.Success {
		t.Errorf("prevLogIndex=5 超出本地日志长度=3，应返回 Success=false")
	}
}

func TestAppendEntriesTruncation_T025(t *testing.T) {
	rn := NewRaftNode("test-node", map[string]string{}, nil, nil)

	rn.mu.Lock()
	rn.term = 5
	rn.logs = []RaftLog{
		{Index: 1, Term: 3, Command: []byte("cmd1")},
		{Index: 2, Term: 3, Command: []byte("cmd2")},
		{Index: 3, Term: 5, Command: []byte("cmd3")},
	}
	rn.state = StateFollower
	rn.mu.Unlock()

	req := &pb.AppendEntriesRequest{
		Term:         6,
		LeaderId:     "leader-1",
		PrevLogIndex: 2,
		PrevLogTerm:  3,
		Entries: []*pb.LogEntry{
			{Term: 6, Index: 3, Command: []byte("cmd3-new")},
		},
		LeaderCommit: 0,
	}
	resp, err := rn.HandleAppendEntries(nil, req)
	if err != nil {
		t.Fatalf("HandleAppendEntries 错误: %v", err)
	}
	if !resp.Success {
		t.Fatalf("应返回 Success=true（截断后追加新条目）")
	}

	rn.mu.RLock()
	defer rn.mu.RUnlock()
	if len(rn.logs) != 3 {
		t.Errorf("截断后应有 3 条日志, 实际 %d", len(rn.logs))
	}
	if rn.logs[2].Term != 6 {
		t.Errorf("第三条日志 term 应为 6, 实际 %d", rn.logs[2].Term)
	}
}

func TestAdvanceCommitFigure8_T026(t *testing.T) {
	rn := NewRaftNode("test-node", map[string]string{
		"peer-1": "127.0.0.1:9999",
		"peer-2": "127.0.0.1:9998",
	}, nil, nil)

	rn.mu.Lock()
	rn.term = 5
	rn.state = StateLeader
	rn.logs = []RaftLog{
		{Index: 1, Term: 3, Command: []byte("cmd1")},
		{Index: 2, Term: 3, Command: []byte("cmd2")},
		{Index: 3, Term: 5, Command: []byte("cmd3")},
	}
	rn.matchIdx["peer-1"] = 2
	rn.matchIdx["peer-2"] = 2
	rn.mu.Unlock()

	rn.advanceCommit(5)

	rn.mu.RLock()
	commitAfterFirst := rn.commitIdx
	rn.mu.RUnlock()
	if commitAfterFirst > 0 {
		t.Errorf("Figure 8: 旧 term 日志 (term=3) 不应直接提交, commitIdx=%d", commitAfterFirst)
	}

	rn.mu.Lock()
	rn.matchIdx["peer-1"] = 3
	rn.matchIdx["peer-2"] = 3
	rn.mu.Unlock()

	rn.advanceCommit(5)

	rn.mu.RLock()
	commitAfterSecond := rn.commitIdx
	rn.mu.RUnlock()
	if commitAfterSecond < 3 {
		t.Errorf("当前 term 日志 (term=5) 获 quorum 后应提交, commitIdx=%d", commitAfterSecond)
	}
}

func TestAdvanceCommitQuorum_T026(t *testing.T) {
	rn := NewRaftNode("test-node", map[string]string{
		"peer-1": "127.0.0.1:9999",
		"peer-2": "127.0.0.1:9998",
	}, nil, nil)

	rn.mu.Lock()
	rn.term = 5
	rn.state = StateLeader
	rn.logs = []RaftLog{
		{Index: 1, Term: 5, Command: []byte("cmd1")},
		{Index: 2, Term: 5, Command: []byte("cmd2")},
	}
	rn.matchIdx["peer-1"] = 2
	rn.matchIdx["peer-2"] = 0
	rn.mu.Unlock()

	rn.advanceCommit(5)

	rn.mu.RLock()
	defer rn.mu.RUnlock()
	if rn.commitIdx < 2 {
		t.Errorf("Leader + 1 follower = 2/3 quorum, 应提交到 index=2, commitIdx=%d", rn.commitIdx)
	}
}
