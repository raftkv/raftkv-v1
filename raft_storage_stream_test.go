package main

import (
	"bytes"
	"runtime"
	"testing"
)

// TestSnapshotStreamingBasic 小数据功能验证：
// 追加日志 → 快照 → 再追加 → 再快照 → ReplayAll 校验数据完整性
func TestSnapshotStreamingBasic(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := tmpDir + "\\test_basic.wal"
	key := bytes.Repeat([]byte{0x01}, 16)

	es, err := NewEncryptedStorage(walPath, key)
	if err != nil {
		t.Fatalf("NewEncryptedStorage: %v", err)
	}
	defer es.Close()

	// Phase 1: 追加 10 条日志
	for i := 0; i < 10; i++ {
		log := RaftLog{
			Index:   int64(i + 1),
			Term:    1,
			Command: []byte{byte(i), byte(i + 1)},
			SM3Hash: bytes.Repeat([]byte{0xAB}, 32),
		}
		if err := es.AppendRaftLog(log); err != nil {
			t.Fatalf("AppendRaftLog[%d]: %v", i, err)
		}
	}

	count, _, err := es.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot 1: %v", err)
	}
	if count != 10 {
		t.Errorf("Snapshot 1 count=%d, want 10", count)
	}

	all, err := es.ReplayAll()
	if err != nil {
		t.Fatalf("ReplayAll 1: %v", err)
	}
	if len(all) != 10 {
		t.Fatalf("ReplayAll 1 len=%d, want 10", len(all))
	}
	for i, l := range all {
		if l.Index != int64(i+1) {
			t.Errorf("all[%d].Index=%d, want %d", i, l.Index, i+1)
		}
	}

	// Phase 2: 追加 5 条日志（term=2）
	for i := 0; i < 5; i++ {
		log := RaftLog{
			Index:   int64(10 + i + 1),
			Term:    2,
			Command: []byte{byte(10 + i)},
			SM3Hash: bytes.Repeat([]byte{0xCD}, 32),
		}
		if err := es.AppendRaftLog(log); err != nil {
			t.Fatalf("AppendRaftLog phase2[%d]: %v", i, err)
		}
	}

	count2, _, err := es.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot 2: %v", err)
	}
	if count2 != 5 {
		t.Errorf("Snapshot 2 count=%d, want 5", count2)
	}

	all2, err := es.ReplayAll()
	if err != nil {
		t.Fatalf("ReplayAll 2: %v", err)
	}
	if len(all2) != 15 {
		t.Fatalf("ReplayAll 2 len=%d, want 15", len(all2))
	}
	for i, l := range all2 {
		if l.Index != int64(i+1) {
			t.Errorf("all2[%d].Index=%d, want %d", i, l.Index, i+1)
		}
		if i < 10 && l.Term != 1 {
			t.Errorf("all2[%d].Term=%d, want 1", i, l.Term)
		}
		if i >= 10 && l.Term != 2 {
			t.Errorf("all2[%d].Term=%d, want 2", i, l.Term)
		}
	}
}

// TestSnapshotStreamingMemoryBounded 大数据内存边界验证：
// 构造 ~100MB 状态 → 快照 → 追加少量 → 再快照 → 断言峰值分配 < 100MB
// 旧代码 io.ReadAll 路径在此场景下 TotalAlloc delta > 500MB，
// 流式实现应 < 100MB（WAL replay 10条 + 64KB 缓冲 + 逐条 marshal）。
func TestSnapshotStreamingMemoryBounded(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := tmpDir + "\\test_mem.wal"
	key := bytes.Repeat([]byte{0x02}, 16)

	es, err := NewEncryptedStorage(walPath, key)
	if err != nil {
		t.Fatalf("NewEncryptedStorage: %v", err)
	}
	defer es.Close()

	// Phase 1: 追加 1000 条 × 100KB command ≈ 100MB 状态
	const phase1Count = 1000
	cmd := bytes.Repeat([]byte{0x42}, 100*1024)
	for i := 0; i < phase1Count; i++ {
		log := RaftLog{
			Index:   int64(i + 1),
			Term:    1,
			Command: cmd,
			SM3Hash: bytes.Repeat([]byte{0xAB}, 32),
		}
		if err := es.AppendRaftLog(log); err != nil {
			t.Fatalf("AppendRaftLog[%d]: %v", i, err)
		}
	}

	if _, _, err := es.Snapshot(); err != nil {
		t.Fatalf("Snapshot 1: %v", err)
	}

	all1, err := es.ReplayAll()
	if err != nil {
		t.Fatalf("ReplayAll 1: %v", err)
	}
	if len(all1) != phase1Count {
		t.Fatalf("ReplayAll 1 len=%d, want %d", len(all1), phase1Count)
	}

	// Phase 2: 追加 10 条新日志，测量 Snapshot 期间内存分配
	const phase2Count = 10
	for i := 0; i < phase2Count; i++ {
		log := RaftLog{
			Index:   int64(phase1Count + i + 1),
			Term:    2,
			Command: cmd,
			SM3Hash: bytes.Repeat([]byte{0xCD}, 32),
		}
		if err := es.AppendRaftLog(log); err != nil {
			t.Fatalf("AppendRaftLog phase2[%d]: %v", i, err)
		}
	}

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	if _, _, err := es.Snapshot(); err != nil {
		t.Fatalf("Snapshot 2: %v", err)
	}

	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	allocDelta := m2.TotalAlloc - m1.TotalAlloc
	allocDeltaMB := allocDelta / 1024 / 1024
	t.Logf("Snapshot 2 TotalAlloc delta: %d MB", allocDeltaMB)

	// 流式实现：WAL replay(10条×100KB) + 64KB缓冲 + 逐条marshal ≈ 2MB
	// 旧 io.ReadAll 实现：replay + io.ReadAll(100MB) + json.Marshal(100MB) + merged(200MB) + gzip(200MB) ≈ 600MB
	// 阈值 100MB 清晰区分两种实现
	if allocDelta > 100*1024*1024 {
		t.Errorf("Snapshot 2 内存分配 %d MB 超过 100MB 阈值（流式实现应远低于此值）", allocDeltaMB)
	}

	// 校验数据完整性
	all2, err := es.ReplayAll()
	if err != nil {
		t.Fatalf("ReplayAll 2: %v", err)
	}
	wantTotal := phase1Count + phase2Count
	if len(all2) != wantTotal {
		t.Fatalf("ReplayAll 2 len=%d, want %d", len(all2), wantTotal)
	}
	for i, l := range all2 {
		if l.Index != int64(i+1) {
			t.Errorf("all2[%d].Index=%d, want %d", i, l.Index, i+1)
			break
		}
	}
}

// TestSnapshotStreamingNoNewLogs 快照后无新日志再快照：数据不变
func TestSnapshotStreamingNoNewLogs(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := tmpDir + "\\test_nonew.wal"
	key := bytes.Repeat([]byte{0x03}, 16)

	es, err := NewEncryptedStorage(walPath, key)
	if err != nil {
		t.Fatalf("NewEncryptedStorage: %v", err)
	}
	defer es.Close()

	for i := 0; i < 5; i++ {
		log := RaftLog{
			Index:   int64(i + 1),
			Term:    1,
			Command: []byte{byte(i)},
		}
		if err := es.AppendRaftLog(log); err != nil {
			t.Fatalf("AppendRaftLog[%d]: %v", i, err)
		}
	}

	if _, _, err := es.Snapshot(); err != nil {
		t.Fatalf("Snapshot 1: %v", err)
	}

	// 无新日志，再次快照
	count, _, err := es.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot 2 (no new): %v", err)
	}
	if count != 0 {
		t.Errorf("Snapshot 2 count=%d, want 0", count)
	}

	all, err := es.ReplayAll()
	if err != nil {
		t.Fatalf("ReplayAll: %v", err)
	}
	if len(all) != 5 {
		t.Errorf("ReplayAll len=%d, want 5", len(all))
	}
}

// TestSnapshotStreamingEmptyStorage 空存储快照：无现有快照 + 无新日志
func TestSnapshotStreamingEmptyStorage(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := tmpDir + "\\test_empty.wal"
	key := bytes.Repeat([]byte{0x04}, 16)

	es, err := NewEncryptedStorage(walPath, key)
	if err != nil {
		t.Fatalf("NewEncryptedStorage: %v", err)
	}
	defer es.Close()

	count, _, err := es.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot on empty: %v", err)
	}
	if count != 0 {
		t.Errorf("Snapshot count=%d, want 0", count)
	}

	all, err := es.ReplayAll()
	if err != nil {
		t.Fatalf("ReplayAll: %v", err)
	}
	if len(all) != 0 {
		t.Errorf("ReplayAll len=%d, want 0", len(all))
	}
}
