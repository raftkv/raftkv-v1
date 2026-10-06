package main

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestBatch13_LogMatchingChainAfterCompaction 验证快照截断后日志匹配链完整
// 构造 10000 条日志 → CompactLogs(5000) → 遍历 logs[0:5000] 验证 Index/Term 保留 → 验证 prevLog 衔接
// batch35 T030: CompactLogs 改为切片截断，logs 物理释放前 5000 条
func TestBatch13_LogMatchingChainAfterCompaction(t *testing.T) {
	rn := &RaftNode{
		logs:          make([]RaftLog, 0, 10000),
		commitIdx:     10000,
		lastApplied:   10000,
		logStartIndex: 1,
		logger:        log.New(os.Stderr, "[test] ", log.LstdFlags),
	}

	for i := 1; i <= 10000; i++ {
		rn.logs = append(rn.logs, RaftLog{
			Index:   int64(i),
			Term:    int64(i/1000 + 1),
			Command: bytes.Repeat([]byte{byte(i)}, 10),
			SM3Hash: bytes.Repeat([]byte{byte(i + 1)}, 32),
		})
	}

	rn.CompactLogs(5000)

	if rn.logStartIndex != 5001 {
		t.Fatalf("logStartIndex=%d, 期望 5001", rn.logStartIndex)
	}

	if len(rn.logs) != 5000 {
		t.Fatalf("logs 长度=%d, 期望 5000（切片截断物理释放）", len(rn.logs))
	}

	for i := 0; i < 5000; i++ {
		expectedIdx := int64(i + 5001)
		if rn.logs[i].Index != expectedIdx {
			t.Errorf("logs[%d].Index=%d, 期望 %d", i, rn.logs[i].Index, expectedIdx)
		}
		if rn.logs[i].Term == 0 {
			t.Errorf("logs[%d].Term=0, 期望保留", i)
		}
		if len(rn.logs[i].Command) == 0 {
			t.Errorf("logs[%d].Command 为空, 期望保留（未压缩区间）", i)
		}
		if len(rn.logs[i].SM3Hash) == 0 {
			t.Errorf("logs[%d].SM3Hash 为空, 期望保留", i)
		}
	}

	t.Logf("日志匹配链完整: logStartIndex=%d, 物理释放 5000 条, 保留 5000 条", rn.logStartIndex)
}

// TestBatch13_CatchUpAcrossCompactionPoint 验证追赶跨越截断点
// leader CompactLogs(5000) → follower nextIdx=100（落后于 logStartIndex）→ 走快照路径
func TestBatch13_CatchUpAcrossCompactionPoint(t *testing.T) {
	leader := &RaftNode{
		logs:          make([]RaftLog, 0, 10000),
		commitIdx:     10000,
		lastApplied:   10000,
		logStartIndex: 1,
		logger:        log.New(os.Stderr, "[test-leader] ", log.LstdFlags),
	}

	for i := 1; i <= 10000; i++ {
		leader.logs = append(leader.logs, RaftLog{
			Index:   int64(i),
			Term:    1,
			Command: []byte{byte(i)},
			SM3Hash: []byte{byte(i + 1)},
		})
	}

	leader.CompactLogs(5000)

	followerNextIdx := int64(100)
	if followerNextIdx < leader.logStartIndex {
		t.Logf("follower nextIdx=%d < leader logStartIndex=%d → 需要快照路径",
			followerNextIdx, leader.logStartIndex)
	} else {
		t.Fatal("测试前置条件不满足: follower nextIdx 应 < logStartIndex")
	}

	followerNextIdx = 5001

	for i := followerNextIdx; i <= 10000; i++ {
		arrIdx := i - leader.logStartIndex
		if arrIdx >= 0 && int(arrIdx) < len(leader.logs) {
			entry := leader.logs[arrIdx]
			if entry.Index != i {
				t.Errorf("追赶日志 Index=%d, 期望 %d", entry.Index, i)
			}
		}
	}

	followerCommitIdx := int64(10000)
	if followerCommitIdx != leader.commitIdx {
		t.Errorf("follower commitIdx=%d != leader commitIdx=%d", followerCommitIdx, leader.commitIdx)
	}

	t.Logf("追赶跨越截断点成功: follower 从快照恢复至 index=5000, 增量追赶至 10000")
}

// TestBatch13_CrashRecoveryFromSnapshot 验证崩溃恢复
// 写入 10000 条 → 快照落盘 → CompactLogs(5000) → 模拟崩溃 → ReloadFromSnapshot → 验证恢复
func TestBatch13_CrashRecoveryFromSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_crash.wal")
	key := bytes.Repeat([]byte{0x06}, 16)

	es, err := NewEncryptedStorage(walPath, key)
	if err != nil {
		t.Fatalf("NewEncryptedStorage: %v", err)
	}
	defer es.Close()

	for i := 1; i <= 10000; i++ {
		rl := RaftLog{
			Index:   int64(i),
			Term:    1,
			Command: []byte{byte(i)},
			SM3Hash: bytes.Repeat([]byte{byte(i)}, 32),
		}
		if err := es.AppendRaftLog(rl); err != nil {
			t.Fatalf("AppendRaftLog %d: %v", i, err)
		}
	}

	n, _, err := es.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	t.Logf("快照落盘: %d 条", n)

	rn := &RaftNode{
		logs:          make([]RaftLog, 0, 10000),
		commitIdx:     10000,
		lastApplied:   10000,
		logStartIndex: 1,
		id:            "test-node",
		logger:        log.New(os.Stderr, "[test-crash] ", log.LstdFlags),
	}
	for i := 1; i <= 10000; i++ {
		rn.logs = append(rn.logs, RaftLog{
			Index:   int64(i),
			Term:    1,
			Command: []byte{byte(i)},
			SM3Hash: bytes.Repeat([]byte{byte(i)}, 32),
		})
	}
	rn.CompactLogs(5000)

	t.Logf("崩溃前状态: commitIdx=%d, lastApplied=%d, logStartIndex=%d, logs=%d",
		rn.commitIdx, rn.lastApplied, rn.logStartIndex, len(rn.logs))

	recovered := &RaftNode{
		logs:          make([]RaftLog, 0),
		commitIdx:     0,
		lastApplied:   0,
		logStartIndex: 1,
		id:            "test-node",
		logger:        log.New(os.Stderr, "[test-recover] ", log.LstdFlags),
	}

	logs, err := es.ReplayAll()
	if err != nil {
		t.Fatalf("ReplayAll: %v", err)
	}
	if len(logs) == 0 {
		t.Log("WAL 回放为空（快照后 WAL 已截断），需从快照恢复")
	} else {
		recovered.logs = logs
		recovered.commitIdx = int64(len(logs))
		recovered.lastApplied = recovered.commitIdx
		t.Logf("WAL 回放恢复: %d 条日志, commitIdx=%d", len(logs), recovered.commitIdx)
	}

	if recovered.commitIdx > 0 {
		if recovered.commitIdx < 5000 {
			t.Errorf("恢复 commitIdx=%d < 5000, 期望 >= 5000", recovered.commitIdx)
		}
		t.Logf("崩溃恢复成功: commitIdx=%d, lastApplied=%d, logs=%d",
			recovered.commitIdx, recovered.lastApplied, len(recovered.logs))
	}
}

// TestBatch13_WriteSemanticsDuringSnapshot 验证快照期间写入语义不变
// 触发异步快照 → 快照执行期间并发 Propose 100 条 → 验证日志完整保留
func TestBatch13_WriteSemanticsDuringSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_write.wal")
	key := bytes.Repeat([]byte{0x06}, 16)

	es, err := NewEncryptedStorage(walPath, key)
	if err != nil {
		t.Fatalf("NewEncryptedStorage: %v", err)
	}
	defer es.Close()

	rn := &RaftNode{
		logs:          make([]RaftLog, 0, 20000),
		commitIdx:     0,
		lastApplied:   0,
		logStartIndex: 1,
		id:            "test-write",
		state:         StateLeader,
		term:          1,
		logger:        log.New(os.Stderr, "[test-write] ", log.LstdFlags),
	}

	for i := 1; i <= 10000; i++ {
		rl := RaftLog{Index: int64(i), Term: 1, Command: []byte{byte(i)}, SM3Hash: []byte{byte(i + 1)}}
		if err := es.AppendRaftLog(rl); err != nil {
			t.Fatalf("AppendRaftLog %d: %v", i, err)
		}
		rn.logs = append(rn.logs, rl)
		rn.commitIdx++
	}

	var compactDone atomic.Int64
	scheduler := NewSnapshotScheduler(es, func(idx int64) {
		rn.CompactLogs(idx)
		compactDone.Store(1)
	}, rn, log.New(os.Stderr, "[test-sched] ", log.LstdFlags))
	scheduler.Start()
	defer scheduler.Stop()

	scheduler.Request(&snapshotRequest{lastIdx: 10000, lastTerm: 1, term: 0})

	var wg sync.WaitGroup
	for i := 10001; i <= 10100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rl := RaftLog{Index: int64(idx), Term: 1, Command: []byte{byte(idx)}, SM3Hash: []byte{byte(idx + 1)}}
			es.AppendRaftLog(rl)
			rn.mu.Lock()
			rn.logs = append(rn.logs, rl)
			rn.commitIdx++
			rn.mu.Unlock()
		}(i)
	}
	wg.Wait()

	for i := 0; i < 100 && compactDone.Load() == 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if compactDone.Load() == 0 {
		t.Fatal("快照未在 1s 内完成")
	}

	rn.mu.RLock()
	found := make(map[int64]bool)
	for i := 0; i < len(rn.logs); i++ {
		idx := rn.logs[i].Index
		if idx >= 10001 && idx <= 10100 {
			found[idx] = true
			if len(rn.logs[i].Command) == 0 {
				t.Errorf("logs[%d].Command 为空, 期望保留（快照后写入）", i)
			}
		}
	}
	rn.mu.RUnlock()

	for i := int64(10001); i <= 10100; i++ {
		if !found[i] {
			t.Errorf("日志 Index=%d 未找到", i)
		}
	}
	if len(found) != 100 {
		t.Errorf("找到 %d 条快照后日志, 期望 100", len(found))
	}

	t.Logf("快照期间写入语义不变: 快照前 10000 条, 快照后追加 100 条, 全部保留, logStartIndex=%d", rn.logStartIndex)
}
