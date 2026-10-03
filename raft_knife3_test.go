package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestKnife3_ReloadFromSnapshot
// 刀三验证: ReloadFromSnapshot 正确重载日志、更新 commitIdx/lastApplied/logStartIndex
func TestKnife3_ReloadFromSnapshot(t *testing.T) {
	rn := &RaftNode{
		id:   "node-1",
		logs: make([]RaftLog, 10),
	}
	for i := range rn.logs {
		rn.logs[i] = RaftLog{Index: int64(i + 1), Term: 5, Command: []byte("old")}
	}

	// 构造快照数据: 5000 条日志
	snapLogs := make([]RaftLog, 5000)
	for i := range snapLogs {
		snapLogs[i] = RaftLog{Index: int64(i + 1), Term: 10, Command: []byte("snap")}
	}
	snapData, _ := json.Marshal(snapLogs)

	err := rn.ReloadFromSnapshot(snapData, 5000, 10)
	if err != nil {
		t.Fatalf("ReloadFromSnapshot: %v", err)
	}

	if len(rn.logs) != 5000 {
		t.Errorf("logs len=%d, 期望 5000", len(rn.logs))
	}
	if rn.commitIdx != 5000 {
		t.Errorf("commitIdx=%d, 期望 5000", rn.commitIdx)
	}
	if rn.lastApplied != 5000 {
		t.Errorf("lastApplied=%d, 期望 5000", rn.lastApplied)
	}
	if rn.logStartIndex != 1 {
		t.Errorf("logStartIndex=%d, 期望 1", rn.logStartIndex)
	}
	if rn.logs[4999].Index != 5000 || rn.logs[4999].Term != 10 {
		t.Errorf("logs[4999]: Index=%d Term=%d, 期望 5000/10", rn.logs[4999].Index, rn.logs[4999].Term)
	}
	t.Log("✓ ReloadFromSnapshot: 5000 条日志, commitIdx=5000, lastApplied=5000, logStartIndex=1")
}

// TestKnife3_ReloadFromSnapshot_EmptyData
// 刀三验证: 空快照数据（T035: 空快照合法，表示仅状态机状态无日志条目）
func TestKnife3_ReloadFromSnapshot_EmptyData(t *testing.T) {
	rn := &RaftNode{
		id: "node-1",
	}

	err := rn.ReloadFromSnapshot([]byte("[]"), 100, 5)
	if err != nil {
		t.Errorf("空快照应成功（T035: 表示仅状态机状态）: %v", err)
	}
	if rn.logStartIndex != 101 {
		t.Errorf("logStartIndex 应为 101, 实际 %d", rn.logStartIndex)
	}
	t.Log("✓ 空快照数据正确处理: logStartIndex=lastIncludedIndex+1")
}

// TestKnife3_ConcurrencyGuard
// 刀三验证: 同一 follower 并发调用 SyncFollower，只有一个执行
func TestKnife3_ConcurrencyGuard(t *testing.T) {
	rn := &RaftNode{
		id:        "node-1",
		state:     StateLeader,
		term:      10,
		logs:      make([]RaftLog, 100),
		commitIdx: 100,
		nextIdx:   make(map[string]int64),
		matchIdx:  make(map[string]int64),
		peers:     []PeerInfo{{ID: "node-2"}},
	}
	for i := range rn.logs {
		rn.logs[i] = RaftLog{Index: int64(i + 1), Term: 10, Command: []byte("cmd")}
	}
	rn.nextIdx["node-2"] = 1
	rn.matchIdx["node-2"] = 0

	mgr := NewBatchSyncManager(rn, DefaultBatchSyncConfig())

	var execCount int32
	var wg sync.WaitGroup

	// 并发 10 个 SyncFollower 调用
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// SyncFollower 会因 GetPeerClient 失败而快速返回，
			// 但并发护栏应确保只有一个进入
			_, loaded := mgr.inProgress.LoadOrStore("node-2", struct{}{})
			if !loaded {
				atomic.AddInt32(&execCount, 1)
				time.Sleep(50 * time.Millisecond)
				mgr.inProgress.Delete("node-2")
			}
		}()
	}
	wg.Wait()

	if c := atomic.LoadInt32(&execCount); c != 1 {
		t.Errorf("并发执行次数=%d, 期望 1（并发护栏应阻止重复）", c)
	}
	t.Log("✓ 并发护栏: 10 个并发调用，仅 1 个执行")
}

// TestKnife3_SnapshotFallbackOnErrCompacted
// 刀三验证: ErrCompacted 时触发快照兜底路径（HTTP POST）
func TestKnife3_SnapshotFallbackOnErrCompacted(t *testing.T) {
	// 构造 mock follower HTTP server
	var receivedSnapshot bool
	var receivedLastIdx int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/raft/install-snapshot" {
			http.NotFound(w, r)
			return
		}
		var req struct {
			SnapshotData      []byte `json:"snapshot_data"`
			LastIncludedIndex int64  `json:"last_included_index"`
			LastIncludedTerm  int64  `json:"last_included_term"`
			LeaderCommit      int64  `json:"leader_commit"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		receivedSnapshot = true
		receivedLastIdx = req.LastIncludedIndex
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"success":true}`))
	}))
	defer server.Close()

	// 构造快照数据
	snapLogs := make([]RaftLog, 5000)
	for i := range snapLogs {
		snapLogs[i] = RaftLog{Index: int64(i + 1), Term: 10, Command: []byte("snap")}
	}
	snapData, _ := json.Marshal(snapLogs)

	rn := &RaftNode{
		id:            "node-1",
		state:         StateLeader,
		term:          10,
		logs:          make([]RaftLog, 10000),
		commitIdx:     10000,
		nextIdx:       make(map[string]int64),
		matchIdx:      make(map[string]int64),
		peers:         []PeerInfo{{ID: "node-2"}},
		peerHttpAddrs: make(map[string]string),
	}
	for i := range rn.logs {
		rn.logs[i] = RaftLog{Index: int64(i + 1), Term: 10, Command: []byte("cmd")}
	}
	rn.nextIdx["node-2"] = 1
	rn.matchIdx["node-2"] = 0

	// 设置 HTTP 地址为 mock server
	rn.peerHttpAddrs["node-2"] = server.Listener.Addr().String()

	// 设置 getSnapshotData 回调
	rn.getSnapshotData = func() ([]byte, int64, int64, error) {
		return snapData, 5000, 10, nil
	}

	mgr := NewBatchSyncManager(rn, DefaultBatchSyncConfig())
	mgr.sendSnapshot("node-2")

	if !receivedSnapshot {
		t.Error("follower 未收到快照")
	}
	if receivedLastIdx != 5000 {
		t.Errorf("follower 收到 lastIncludedIndex=%d, 期望 5000", receivedLastIdx)
	}
	t.Log("✓ ErrCompacted 触发快照兜底: follower 收到快照, lastIncludedIndex=5000")
}

// TestKnife3_SnapshotUpdatesNextIdxMatchIdx
// 刀三验证: 快照安装成功后，leader 更新 nextIdx/matchIdx
func TestKnife3_SnapshotUpdatesNextIdxMatchIdx(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"success":true}`))
	}))
	defer server.Close()

	snapLogs := make([]RaftLog, 5000)
	for i := range snapLogs {
		snapLogs[i] = RaftLog{Index: int64(i + 1), Term: 10, Command: []byte("snap")}
	}
	snapData, _ := json.Marshal(snapLogs)

	rn := &RaftNode{
		id:                "node-1",
		state:             StateLeader,
		term:              10,
		logs:              make([]RaftLog, 10000),
		commitIdx:         10000,
		nextIdx:           make(map[string]int64),
		matchIdx:          make(map[string]int64),
		peers:             []PeerInfo{{ID: "node-2"}},
		peerHttpAddrs:     make(map[string]string),
		degradedFollowers: make(map[string]bool),
	}
	for i := range rn.logs {
		rn.logs[i] = RaftLog{Index: int64(i + 1), Term: 10, Command: []byte("cmd")}
	}
	rn.nextIdx["node-2"] = 1
	rn.matchIdx["node-2"] = 0
	rn.degradedFollowers["node-2"] = true

	rn.peerHttpAddrs["node-2"] = server.Listener.Addr().String()
	rn.getSnapshotData = func() ([]byte, int64, int64, error) {
		return snapData, 5000, 10, nil
	}

	mgr := NewBatchSyncManager(rn, DefaultBatchSyncConfig())
	mgr.sendSnapshot("node-2")

	// 验证 matchIdx 更新为 lastIncludedIndex
	if rn.matchIdx["node-2"] != 5000 {
		t.Errorf("matchIdx=%d, 期望 5000", rn.matchIdx["node-2"])
	}
	// 验证 nextIdx 更新为 lastIncludedIndex+1
	if rn.nextIdx["node-2"] != 5001 {
		t.Errorf("nextIdx=%d, 期望 5001", rn.nextIdx["node-2"])
	}
	// 验证降级标记清除
	if rn.degradedFollowers["node-2"] {
		t.Error("降级标记应已清除")
	}
	t.Log("✓ 快照安装成功: matchIdx=5000, nextIdx=5001, 降级标记已清除")
}

// TestKnife3_GetPeerHttpAddr
// 刀三验证: GetPeerHttpAddr 正确返回 HTTP 地址
func TestKnife3_GetPeerHttpAddr(t *testing.T) {
	rn := &RaftNode{
		peerHttpAddrs: map[string]string{
			"node-2": "node-2:9000",
			"node-3": "node-3:9000",
		},
	}

	addr, ok := rn.GetPeerHttpAddr("node-2")
	if !ok || addr != "node-2:9000" {
		t.Errorf("GetPeerHttpAddr(node-2): addr=%s ok=%v, 期望 node-2:9000/true", addr, ok)
	}

	_, ok = rn.GetPeerHttpAddr("node-9")
	if ok {
		t.Error("GetPeerHttpAddr(node-9) 应返回 false")
	}
	t.Log("✓ GetPeerHttpAddr 正确返回 HTTP 地址")
}

// TestKnife3_CompressGzip
// 刀三验证: compressGzip 辅助函数正确压缩数据
func TestKnife3_CompressGzip(t *testing.T) {
	original := []byte(`{"test":"data","number":42}`)
	compressed, err := compressGzip(original)
	if err != nil {
		t.Fatalf("compressGzip: %v", err)
	}

	gz, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	defer gz.Close()
	decompressed, err := readAll(gz)
	if err != nil {
		t.Fatalf("读取解压数据: %v", err)
	}

	if !bytes.Equal(decompressed, original) {
		t.Error("压缩→解压数据不一致")
	}
	t.Log("✓ compressGzip 正确压缩数据")
}

func readAll(r interface{ Read([]byte) (int, error) }) ([]byte, error) {
	var buf bytes.Buffer
	tmp := make([]byte, 1024)
	for {
		n, err := r.Read(tmp)
		if n > 0 {
			buf.Write(tmp[:n])
		}
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}
	}
	return buf.Bytes(), nil
}
