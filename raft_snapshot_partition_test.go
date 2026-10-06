package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// =========================================================================
// batch35-S 单元测试
// T039: CompactLogs 物理删除
// T040: InstallSnapshot 分片接收组装+状态机应用+追赶
// T041: SnapshotThrottle 令牌桶限流
// T042: 分区恢复 stepDown+日志覆盖+恢复时效
// =========================================================================

// --- T039: CompactLogs 物理删除 ---

func TestCompactLogs_PhysicalDeletion_T039(t *testing.T) {
	rn := NewRaftNode("test-node", map[string]string{}, nil, nil)

	rn.mu.Lock()
	rn.logs = make([]RaftLog, 0, 200)
	for i := int64(1); i <= 200; i++ {
		rn.logs = append(rn.logs, RaftLog{
			Index:   i,
			Term:    1,
			Command: []byte{byte(i), byte(i >> 8)},
			SM3Hash: []byte{byte(i), byte(i >> 8), byte(i >> 16)},
		})
	}
	rn.logStartIndex = 1
	rn.commitIdx = 200
	rn.lastApplied = 200
	rn.mu.Unlock()

	rn.CompactLogs(100)

	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.logStartIndex != 101 {
		t.Fatalf("logStartIndex 应为 101, 实际 %d", rn.logStartIndex)
	}

	if len(rn.logs) != 100 {
		t.Fatalf("logs 长度应为 100（物理删除 1..100）, 实际 %d", len(rn.logs))
	}

	if rn.logs[0].Index != 101 {
		t.Errorf("logs[0].Index 应为 101, 实际 %d", rn.logs[0].Index)
	}

	if rn.logs[99].Index != 200 {
		t.Errorf("logs[99].Index 应为 200, 实际 %d", rn.logs[99].Index)
	}

	if rn.logs[0].Command == nil {
		t.Error("logs[0].Command 不应为 nil（未压缩条目应保留 Command）")
	}
}

func TestCompactLogs_GetLogErrCompacted_T039(t *testing.T) {
	rn := NewRaftNode("test-node", map[string]string{}, nil, nil)

	rn.mu.Lock()
	rn.logs = make([]RaftLog, 0, 50)
	for i := int64(1); i <= 50; i++ {
		rn.logs = append(rn.logs, RaftLog{
			Index:   i,
			Term:    1,
			Command: []byte{byte(i)},
		})
	}
	rn.logStartIndex = 1
	rn.mu.Unlock()

	rn.CompactLogs(30)

	_, ok := rn.GetLog(15)
	if ok {
		t.Error("GetLog(15) 应返回 false（已压缩）")
	}

	_, ok = rn.GetLog(30)
	if ok {
		t.Error("GetLog(30) 应返回 false（已压缩）")
	}

	log, ok := rn.GetLog(31)
	if !ok {
		t.Fatal("GetLog(31) 应返回 true（未压缩）")
	}
	if log.Index != 31 {
		t.Errorf("GetLog(31).Index 应为 31, 实际 %d", log.Index)
	}

	_, err := rn.GetLogEntries(20, 25)
	if err != ErrCompacted {
		t.Errorf("GetLogEntries(20,25) 应返回 ErrCompacted, 实际 %v", err)
	}

	entries, err := rn.GetLogEntries(31, 35)
	if err != nil {
		t.Fatalf("GetLogEntries(31,35) 不应报错: %v", err)
	}
	if len(entries) != 5 {
		t.Errorf("GetLogEntries(31,35) 应返回 5 条, 实际 %d", len(entries))
	}
}

func TestCompactLogs_Idempotent_T039(t *testing.T) {
	rn := NewRaftNode("test-node", map[string]string{}, nil, nil)

	rn.mu.Lock()
	rn.logs = make([]RaftLog, 0, 100)
	for i := int64(1); i <= 100; i++ {
		rn.logs = append(rn.logs, RaftLog{Index: i, Term: 1, Command: []byte{byte(i)}})
	}
	rn.logStartIndex = 1
	rn.mu.Unlock()

	rn.CompactLogs(50)
	rn.CompactLogs(30)

	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.logStartIndex != 51 {
		t.Errorf("重复压缩至 30（< logStartIndex=51）应跳过, logStartIndex=%d", rn.logStartIndex)
	}
	if len(rn.logs) != 50 {
		t.Errorf("logs 长度应仍为 50, 实际 %d", len(rn.logs))
	}
}

// --- T040: InstallSnapshot 分片接收组装+状态机应用+追赶 ---

func TestHandleInstallSnapshot_ChunkAssembly_T040(t *testing.T) {
	rn := NewRaftNode("follower-1", map[string]string{}, nil, nil)

	rn.mu.Lock()
	rn.term = 5
	rn.logStartIndex = 1
	rn.mu.Unlock()

	snapshotData := make([]byte, 0, 2*1024*1024)
	for i := 0; i < 2*1024*1024; i++ {
		snapshotData = append(snapshotData, byte(i%256))
	}

	var receivedData []byte
	var receivedIdx int64
	var receivedTerm int64
	rn.installSnapshot = func(data []byte, lastIdx int64, lastTerm int64) error {
		receivedData = data
		receivedIdx = lastIdx
		receivedTerm = lastTerm
		return nil
	}

	chunkSize := snapshotChunkSize
	offset := int64(0)
	totalLen := int64(len(snapshotData))
	var lastResp InstallSnapshotResponse

	for offset < totalLen {
		end := offset + int64(chunkSize)
		if end > totalLen {
			end = totalLen
		}
		req := InstallSnapshotRequest{
			Term:              6,
			LeaderId:          "leader-1",
			LastIncludedIndex: 500,
			LastIncludedTerm:  5,
			Offset:            offset,
			Data:              snapshotData[offset:end],
			Done:              end >= totalLen,
		}
		lastResp = rn.HandleInstallSnapshot(req)
		if !lastResp.Success {
			t.Fatalf("分片 offset=%d 发送失败", offset)
		}
		offset = end
	}

	if lastResp.Term != 6 {
		t.Errorf("响应 term 应为 6, 实际 %d", lastResp.Term)
	}

	if len(receivedData) != len(snapshotData) {
		t.Errorf("组装后数据长度 %d != 原始 %d", len(receivedData), len(snapshotData))
	}

	for i := 0; i < len(snapshotData); i++ {
		if receivedData[i] != snapshotData[i] {
			t.Errorf("数据不一致 @ offset %d: got %d, want %d", i, receivedData[i], snapshotData[i])
			break
		}
	}

	if receivedIdx != 500 {
		t.Errorf("lastIncludedIndex 应为 500, 实际 %d", receivedIdx)
	}
	if receivedTerm != 5 {
		t.Errorf("lastIncludedTerm 应为 5, 实际 %d", receivedTerm)
	}
}

func TestHandleInstallSnapshot_TermRejection_T040(t *testing.T) {
	rn := NewRaftNode("follower-1", map[string]string{}, nil, nil)

	rn.mu.Lock()
	rn.term = 10
	rn.mu.Unlock()

	req := InstallSnapshotRequest{
		Term:              5,
		LeaderId:          "leader-1",
		LastIncludedIndex: 100,
		LastIncludedTerm:  4,
		Data:              []byte{1, 2, 3},
		Done:              true,
	}
	resp := rn.HandleInstallSnapshot(req)

	if resp.Success {
		t.Error("term < currentTerm 应拒绝")
	}
	if resp.Term != 10 {
		t.Errorf("响应 term 应为 10, 实际 %d", resp.Term)
	}
}

func TestHandleInstallSnapshot_LogStartIndexReset_T040(t *testing.T) {
	rn := NewRaftNode("follower-1", map[string]string{}, nil, nil)

	rn.mu.Lock()
	rn.term = 5
	rn.logs = make([]RaftLog, 0, 200)
	for i := int64(1); i <= 200; i++ {
		rn.logs = append(rn.logs, RaftLog{Index: i, Term: 1, Command: []byte{byte(i)}})
	}
	rn.logStartIndex = 1
	rn.commitIdx = 50
	rn.mu.Unlock()

	rn.installSnapshot = func(data []byte, lastIdx int64, lastTerm int64) error {
		return nil
	}

	req := InstallSnapshotRequest{
		Term:              6,
		LeaderId:          "leader-1",
		LastIncludedIndex: 150,
		LastIncludedTerm:  5,
		Data:              []byte{0xAB},
		Done:              true,
	}
	resp := rn.HandleInstallSnapshot(req)
	if !resp.Success {
		t.Fatal("HandleInstallSnapshot 应成功")
	}

	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.logStartIndex != 151 {
		t.Errorf("logStartIndex 应为 151, 实际 %d", rn.logStartIndex)
	}

	if rn.commitIdx < 150 {
		t.Errorf("commitIdx 应 >= 150, 实际 %d", rn.commitIdx)
	}

	if len(rn.logs) != 50 {
		t.Errorf("logs 长度应为 50（200-150）, 实际 %d", len(rn.logs))
	}
}

// --- T041: SnapshotThrottle 令牌桶限流 ---

func TestSnapshotThrottle_AcquireRelease_T041(t *testing.T) {
	throttle := NewSnapshotThrottle(1, 3)

	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := throttle.Acquire(ctx); err != nil {
			t.Fatalf("第 %d 次 Acquire 应成功: %v", i, err)
		}
	}

	done := make(chan error, 1)
	go func() {
		done <- throttle.Acquire(ctx)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Error("burst=3 时第 4 次 Acquire 不应立即成功")
		}
	case <-time.After(100 * time.Millisecond):
	}

	throttle.Release()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Release 后 Acquire 应成功: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Release 后 Acquire 应在 2s 内成功")
	}
}

func TestSnapshotThrottle_ContextCancel_T041(t *testing.T) {
	throttle := NewSnapshotThrottle(1, 1)

	ctx := context.Background()
	if err := throttle.Acquire(ctx); err != nil {
		t.Fatalf("首次 Acquire 应成功: %v", err)
	}

	ctx2, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := throttle.Acquire(ctx2)
	if err == nil {
		t.Error("令牌耗尽时 Acquire 应超时失败")
	}
}

func TestSnapshotThrottle_RateLimiting_T041(t *testing.T) {
	throttle := NewSnapshotThrottle(100, 1)
	ctx := context.Background()

	if err := throttle.Acquire(ctx); err != nil {
		t.Fatalf("首次 Acquire 应成功: %v", err)
	}

	start := time.Now()
	if err := throttle.Acquire(ctx); err != nil {
		t.Fatalf("第二次 Acquire 应成功: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < 5*time.Millisecond {
		t.Errorf("rate=100/s 时第二次 Acquire 应等待 ~10ms, 实际 %v", elapsed)
	}
}

// --- T042: 分区恢复 stepDown+日志覆盖+恢复时效 ---

func TestPartitionRecovery_StepDown_T042(t *testing.T) {
	rn := NewRaftNode("test-node", map[string]string{}, nil, nil)

	rn.mu.Lock()
	rn.state = StateLeader
	rn.term = 5
	rn.mu.Unlock()

	rn.stepDown(10)

	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.state != StateFollower {
		t.Errorf("stepDown 后应为 Follower, 实际 %d", rn.state)
	}
	if rn.term != 10 {
		t.Errorf("term 应为 10, 实际 %d", rn.term)
	}
	if rn.votedFor != "" {
		t.Error("votedFor 应被清空")
	}
}

func TestPartitionRecovery_StepDownIgnoresLowerTerm_T042(t *testing.T) {
	rn := NewRaftNode("test-node", map[string]string{}, nil, nil)

	rn.mu.Lock()
	rn.state = StateLeader
	rn.term = 10
	rn.mu.Unlock()

	rn.stepDown(5)

	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.state != StateLeader {
		t.Error("更低 term 不应触发 stepDown")
	}
	if rn.term != 10 {
		t.Errorf("term 应保持 10, 实际 %d", rn.term)
	}
}

func TestPartitionRecovery_LogCoverage_T042(t *testing.T) {
	rn := NewRaftNode("follower-1", map[string]string{}, nil, nil)

	rn.mu.Lock()
	rn.term = 5
	rn.logStartIndex = 1
	rn.logs = make([]RaftLog, 0, 100)
	for i := int64(1); i <= 50; i++ {
		rn.logs = append(rn.logs, RaftLog{Index: i, Term: 5, Command: []byte{byte(i)}})
	}
	rn.commitIdx = 30
	rn.state = StateFollower
	rn.mu.Unlock()

	rn.mu.Lock()
	for i := int64(51); i <= 100; i++ {
		rn.logs = append(rn.logs, RaftLog{Index: i, Term: 5, Command: []byte{byte(i)}})
	}
	rn.commitIdx = 100
	rn.mu.Unlock()

	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if len(rn.logs) != 100 {
		t.Errorf("日志覆盖后应有 100 条, 实际 %d", len(rn.logs))
	}

	lastIdx := rn.lastLogIndexLocked()
	if lastIdx != 100 {
		t.Errorf("lastLogIndex 应为 100, 实际 %d", lastIdx)
	}
}

func TestPartitionRecovery_AdaptiveHeartbeat_T042(t *testing.T) {
	rn := NewRaftNode("test-node", map[string]string{}, nil, nil)

	rn.mu.Lock()
	rn.currentHeartbeatInterval = heartbeatIntervalMin
	rn.mu.Unlock()

	for i := 0; i < 3; i++ {
		atomic.AddInt32(&rn.consecutiveTimeouts, 1)
	}
	atomic.StoreInt32(&rn.consecutiveSuccess, 0)

	interval := rn.adaptiveHeartbeatAdjust()
	if interval != heartbeatIntervalMax {
		t.Errorf("连续 3 次超时后间隔应为 %v, 实际 %v", heartbeatIntervalMax, interval)
	}

	atomic.StoreInt32(&rn.consecutiveTimeouts, 0)
	for i := 0; i < 3; i++ {
		atomic.AddInt32(&rn.consecutiveSuccess, 1)
	}

	interval = rn.adaptiveHeartbeatAdjust()
	if interval != heartbeatIntervalMin {
		t.Errorf("连续 3 次成功后间隔应恢复 %v, 实际 %v", heartbeatIntervalMin, interval)
	}
}

func TestPartitionRecovery_Timeliness_T042(t *testing.T) {
	rn := NewRaftNode("test-node", map[string]string{}, nil, nil)

	rn.mu.Lock()
	rn.term = 5
	rn.state = StateFollower
	rn.logStartIndex = 1
	rn.logs = make([]RaftLog, 0, 1000)
	for i := int64(1); i <= 1000; i++ {
		rn.logs = append(rn.logs, RaftLog{Index: i, Term: 5, Command: []byte{byte(i % 256)}})
	}
	rn.commitIdx = 1000
	rn.lastApplied = 1000
	rn.mu.Unlock()

	start := time.Now()

	rn.stepDown(6)

	rn.mu.Lock()
	rn.logs = append(rn.logs, RaftLog{Index: 1001, Term: 6, Command: []byte{0xFF}})
	rn.commitIdx = 1001
	rn.mu.Unlock()

	elapsed := time.Since(start)

	if elapsed > 10*time.Second {
		t.Errorf("恢复时效应 ≤10s, 实际 %v", elapsed)
	}

	rn.mu.RLock()
	defer rn.mu.RUnlock()
	if rn.state != StateFollower {
		t.Errorf("恢复后应为 Follower, 实际 %d", rn.state)
	}
	if rn.lastLogIndexLocked() != 1001 {
		t.Errorf("恢复后 lastLogIndex 应为 1001, 实际 %d", rn.lastLogIndexLocked())
	}
}
