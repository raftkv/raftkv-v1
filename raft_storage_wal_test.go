package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestWALRotationAndReplay WAL 轮转 + 回放验证：
// 写入足够数据触发轮转 → 验证 closed WALs 存在 → 验证 Replay 从 closed + current 读取
func TestWALRotationAndReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping rotation test in short mode")
	}

	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_rotate.wal")

	w, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	defer w.Close()

	// Phase 1: 写入 256 条 × 4MB ≈ 1.36GB JSON（超过 90% × 1GB 阈值）
	data4MB := bytes.Repeat([]byte{0x42}, 4*1024*1024)
	for i := 0; i < 256; i++ {
		if err := w.Append(walEntry{
			Index: int64(i + 1),
			Term:  1,
			Data:  data4MB,
		}); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush 1: %v", err)
	}

	// Phase 2: 再写 10 条小数据（触发轮转）
	for i := 0; i < 10; i++ {
		if err := w.Append(walEntry{
			Index: int64(256 + i + 1),
			Term:  2,
			Data:  []byte{byte(i)},
		}); err != nil {
			t.Fatalf("Append phase2[%d]: %v", i, err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush 2: %v", err)
	}

	// 验证轮转发生
	closedWALs := w.ClosedWALs()
	if len(closedWALs) == 0 {
		t.Fatal("期望至少 1 个 closed WAL")
	}
	t.Logf("closedWALs: %d 个", len(closedWALs))

	// 验证 Replay 返回全部 266 条
	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(entries) != 266 {
		t.Errorf("Replay 返回 %d 条, 期望 266", len(entries))
	}

	// 验证前 256 条 Term=1，后 10 条 Term=2
	for i, e := range entries {
		if i < 256 && e.Term != 1 {
			t.Errorf("entry[%d].Term=%d, 期望 1", i, e.Term)
			break
		}
		if i >= 256 && e.Term != 2 {
			t.Errorf("entry[%d].Term=%d, 期望 2", i, e.Term)
			break
		}
	}
}

// TestWALReplayWithClosedWALs 手动构造 closed WAL 验证回放：
// 写入 WAL A → 关闭 → rename 为 .closed → 创建 WAL B → 写入 → 验证 Replay 读取两者
func TestWALReplayWithClosedWALs(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_closed.wal")

	// WAL A: 写入 5 条
	wA, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("NewWAL A: %v", err)
	}
	for i := 0; i < 5; i++ {
		wA.Append(walEntry{Index: int64(i + 1), Term: 1, Data: []byte{byte(i)}})
	}
	wA.Flush()
	wA.Close()

	// 模拟轮转：rename WAL A → .closed.NNN
	closedPath := walPath + ".closed.1000"
	if err := os.Rename(walPath, closedPath); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// WAL B: 重新打开（会发现 closed WAL）
	wB, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("NewWAL B: %v", err)
	}
	defer wB.Close()

	closedWALs := wB.ClosedWALs()
	if len(closedWALs) != 1 || closedWALs[0] != closedPath {
		t.Fatalf("期望 1 个 closed WAL [%s], 实际 %v", closedPath, closedWALs)
	}

	// 写入 3 条到 WAL B
	for i := 0; i < 3; i++ {
		wB.Append(walEntry{Index: int64(5 + i + 1), Term: 2, Data: []byte{byte(5 + i)}})
	}
	wB.Flush()

	// 验证 Replay 返回 8 条（5 from closed + 3 from current）
	entries, err := wB.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(entries) != 8 {
		t.Errorf("Replay 返回 %d 条, 期望 8", len(entries))
	}
	for i, e := range entries {
		if i < 5 && e.Term != 1 {
			t.Errorf("entry[%d].Term=%d, 期望 1", i, e.Term)
		}
		if i >= 5 && e.Term != 2 {
			t.Errorf("entry[%d].Term=%d, 期望 2", i, e.Term)
		}
	}
}

// TestLogCompaction 日志压缩验证：
// 创建 RaftNode → 添加日志 → CompactLogs → 验证 Command/SM3Hash 被释放
func TestLogCompaction(t *testing.T) {
	node := NewRaftNode("test-node", nil, nil, nil)

	// 构造 10 条日志
	logs := make([]RaftLog, 10)
	for i := 0; i < 10; i++ {
		logs[i] = RaftLog{
			Index:   int64(i + 1),
			Term:    1,
			Command: bytes.Repeat([]byte{byte(i)}, 100),
			SM3Hash: bytes.Repeat([]byte{0xAB}, 32),
		}
	}
	node.RestoreFromWAL(logs)

	// 压缩前：所有 Command 非 nil
	node.mu.RLock()
	for i, l := range node.logs {
		if l.Command == nil {
			t.Errorf("压缩前 logs[%d].Command 为 nil", i)
		}
	}
	node.mu.RUnlock()

	// 压缩至 index=7
	node.CompactLogs(7)

	// 压缩后：index 1-7 的 Command/SM3Hash 为 nil，8-10 保留
	node.mu.RLock()
	defer node.mu.RUnlock()
	for i, l := range node.logs {
		if l.Index <= 7 {
			if l.Command != nil {
				t.Errorf("压缩后 logs[%d].Command 应为 nil, 实际 len=%d", i, len(l.Command))
			}
			if l.SM3Hash != nil {
				t.Errorf("压缩后 logs[%d].SM3Hash 应为 nil", i)
			}
		} else {
			if l.Command == nil {
				t.Errorf("压缩后 logs[%d].Command 不应为 nil", i)
			}
			if l.SM3Hash == nil {
				t.Errorf("压缩后 logs[%d].SM3Hash 不应为 nil", i)
			}
		}
	}
}

// TestSnapshotCleansClosedWALs 快照后清理 closed WALs 验证：
// 构造 closed WAL → 快照 → 验证 closed WALs 被删除
func TestSnapshotCleansClosedWALs(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_clean.wal")
	key := bytes.Repeat([]byte{0x05}, 16)

	es, err := NewEncryptedStorage(walPath, key)
	if err != nil {
		t.Fatalf("NewEncryptedStorage: %v", err)
	}
	defer es.Close()

	// 写入日志并快照
	for i := 0; i < 5; i++ {
		es.AppendRaftLog(RaftLog{
			Index:   int64(i + 1),
			Term:    1,
			Command: []byte{byte(i)},
		})
	}
	if _, _, err := es.Snapshot(); err != nil {
		t.Fatalf("Snapshot 1: %v", err)
	}

	// 手动构造 closed WAL
	closedPath := walPath + ".closed.2000"
	closedData := []byte("dummy closed wal")
	if err := os.WriteFile(closedPath, closedData, 0644); err != nil {
		t.Fatalf("创建 closed WAL: %v", err)
	}
	es.wal.closedWALs = append(es.wal.closedWALs, closedPath)

	// 再写入并快照（应清理 closed WALs）
	for i := 0; i < 3; i++ {
		es.AppendRaftLog(RaftLog{
			Index:   int64(5 + i + 1),
			Term:    2,
			Command: []byte{byte(5 + i)},
		})
	}
	if _, _, err := es.Snapshot(); err != nil {
		t.Fatalf("Snapshot 2: %v", err)
	}

	// 验证 closed WAL 文件已被删除
	if _, err := os.Stat(closedPath); !os.IsNotExist(err) {
		t.Errorf("closed WAL 文件应已删除, err=%v", err)
	}

	// 验证 closedWALs 列表为空
	if closedWALs := es.wal.ClosedWALs(); len(closedWALs) != 0 {
		t.Errorf("closedWALs 应为空, 实际 %v", closedWALs)
	}

	// 验证数据完整
	all, err := es.ReplayAll()
	if err != nil {
		t.Fatalf("ReplayAll: %v", err)
	}
	if len(all) != 8 {
		t.Errorf("ReplayAll 返回 %d 条, 期望 8", len(all))
	}
}
