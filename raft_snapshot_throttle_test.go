package main

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestSnapshotThrottle 快照触发节流验证：
// 1. 首次快照立即触发（无前次快照时间限制）
// 2. 节流期内再次达到阈值不触发（时间间隔不足）
// 3. 节流期过后触发
func TestSnapshotThrottle(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_throttle.wal")
	key := bytes.Repeat([]byte{0x06}, 16)

	es, err := NewEncryptedStorage(walPath, key)
	if err != nil {
		t.Fatalf("NewEncryptedStorage: %v", err)
	}
	defer es.Close()

	p := &RaftPipeline{
		storage:             es,
		snapshotThreshold:   3,
		snapshotMinInterval: 100 * time.Millisecond,
		logger:              log.New(os.Stderr, "[test-throttle] ", log.LstdFlags),
	}

	var snapshotCount atomic.Int64
	p.SetOnSnapshotCompact(func(idx int64) {
		snapshotCount.Add(1)
	})

	// Phase 1: 追加 3 条（达到阈值），首次快照应立即触发
	for i := 0; i < 3; i++ {
		p.OnCommit(RaftLog{Index: int64(i + 1), Term: 1, Command: []byte{byte(i)}})
	}
	if c := snapshotCount.Load(); c != 1 {
		t.Errorf("Phase 1: snapshotCount=%d, 期望 1", c)
	}
	t.Log("Phase 1: 首次快照已触发 ✓")

	// Phase 2: 追加 3 条（再次达到阈值），节流期内不应触发
	for i := 0; i < 3; i++ {
		p.OnCommit(RaftLog{Index: int64(3 + i + 1), Term: 1, Command: []byte{byte(3 + i)}})
	}
	if c := snapshotCount.Load(); c != 1 {
		t.Errorf("Phase 2: snapshotCount=%d, 期望 1（节流）", c)
	}
	t.Log("Phase 2: 节流生效，快照未触发 ✓")

	// Phase 3: 等待节流期过后，追加 1 条，应触发
	time.Sleep(150 * time.Millisecond)
	p.OnCommit(RaftLog{Index: 7, Term: 1, Command: []byte{0x07}})
	if c := snapshotCount.Load(); c != 2 {
		t.Errorf("Phase 3: snapshotCount=%d, 期望 2", c)
	}
	t.Log("Phase 3: 节流期过后快照已触发 ✓")
}

// TestSnapshotThrottleConfig 环境变量配置验证
func TestSnapshotThrottleConfig(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_cfg.wal")

	os.Setenv("SM4_KEY", "0102030405060708090a0b0c0d0e0f10")
	os.Setenv("WAL_SNAPSHOT_THRESHOLD", "5000")
	os.Setenv("WAL_SNAPSHOT_MIN_INTERVAL_MS", "30000")
	defer os.Unsetenv("SM4_KEY")
	defer os.Unsetenv("WAL_SNAPSHOT_THRESHOLD")
	defer os.Unsetenv("WAL_SNAPSHOT_MIN_INTERVAL_MS")

	cfg := DefaultPipelineConfig()
	cfg.WALPath = walPath
	cfg.EnableSink = false

	p, err := NewRaftPipeline(cfg)
	if err != nil {
		t.Fatalf("NewRaftPipeline: %v", err)
	}
	defer p.Close()

	if p.snapshotThreshold != 5000 {
		t.Errorf("snapshotThreshold=%d, 期望 5000", p.snapshotThreshold)
	}
	if p.snapshotMinInterval != 30*time.Second {
		t.Errorf("snapshotMinInterval=%v, 期望 30s", p.snapshotMinInterval)
	}
}

// TestSnapshotFirstSnapshotImmediate 首次快照无时间限制验证
func TestSnapshotFirstSnapshotImmediate(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_first.wal")
	key := bytes.Repeat([]byte{0x07}, 16)

	es, err := NewEncryptedStorage(walPath, key)
	if err != nil {
		t.Fatalf("NewEncryptedStorage: %v", err)
	}
	defer es.Close()

	// 设置较长节流间隔（60s），首次快照仍应立即触发
	p := &RaftPipeline{
		storage:             es,
		snapshotThreshold:   2,
		snapshotMinInterval: 60 * time.Second,
		logger:              log.New(os.Stderr, "[test-first] ", log.LstdFlags),
	}

	var snapshotCount atomic.Int64
	p.SetOnSnapshotCompact(func(idx int64) {
		snapshotCount.Add(1)
	})

	for i := 0; i < 2; i++ {
		p.OnCommit(RaftLog{Index: int64(i + 1), Term: 1, Command: []byte{byte(i)}})
	}

	if c := snapshotCount.Load(); c != 1 {
		t.Errorf("首次快照 snapshotCount=%d, 期望 1（即使节流间隔=60s）", c)
	}
	t.Log("首次快照在 60s 节流间隔下仍立即触发 ✓")
}
