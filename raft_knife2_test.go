package main

import (
	"testing"
)

// TestKnife2_CompactLogs_SetsLogStartIndex
// 刀二验证: CompactLogs 后 logStartIndex 正确设置为 upToIndex+1
func TestKnife2_CompactLogs_SetsLogStartIndex(t *testing.T) {
	rn := &RaftNode{
		id:   "node-1",
		logs: make([]RaftLog, 10000),
	}
	for i := range rn.logs {
		rn.logs[i] = RaftLog{Index: int64(i + 1), Term: 10, Command: []byte("cmd"), SM3Hash: []byte("hash")}
	}

	rn.CompactLogs(5000)

	if rn.GetLogStartIndex() != 5001 {
		t.Errorf("logStartIndex=%d, 期望 5001", rn.GetLogStartIndex())
	}
	t.Log("✓ CompactLogs(5000) 后 logStartIndex=5001")
}

// TestKnife2_GetLogEntries_ErrCompacted
// 刀二验证: CompactLogs 后，GetLogEntries 对 < logStartIndex 的请求返回 ErrCompacted
func TestKnife2_GetLogEntries_ErrCompacted(t *testing.T) {
	rn := &RaftNode{
		id:   "node-1",
		logs: make([]RaftLog, 10000),
	}
	for i := range rn.logs {
		rn.logs[i] = RaftLog{Index: int64(i + 1), Term: 10, Command: []byte("cmd"), SM3Hash: []byte("hash")}
	}

	rn.CompactLogs(5000)

	// 请求 startIdx=1（已被压缩）→ ErrCompacted
	entries, err := rn.GetLogEntries(1, 100)
	if err != ErrCompacted {
		t.Errorf("GetLogEntries(1,100): err=%v, 期望 ErrCompacted", err)
	}
	if entries != nil {
		t.Errorf("GetLogEntries(1,100): entries=%v, 期望 nil", entries)
	}
	t.Log("✓ startIdx=1 < logStartIndex=5001 → ErrCompacted，阻止读取已压缩条目")

	// 请求 startIdx=5000（已被压缩）→ ErrCompacted
	_, err = rn.GetLogEntries(5000, 5100)
	if err != ErrCompacted {
		t.Errorf("GetLogEntries(5000,5100): err=%v, 期望 ErrCompacted", err)
	}
	t.Log("✓ startIdx=5000 < logStartIndex=5001 → ErrCompacted")
}

// TestKnife2_GetLogEntries_AfterCompaction_StillWorks
// 刀二验证: CompactLogs 后，GetLogEntries 对 >= logStartIndex 的请求正常工作
func TestKnife2_GetLogEntries_AfterCompaction_StillWorks(t *testing.T) {
	rn := &RaftNode{
		id:            "node-1",
		logs:          make([]RaftLog, 10000),
		logStartIndex: 1,
	}
	for i := range rn.logs {
		rn.logs[i] = RaftLog{Index: int64(i + 1), Term: 10, Command: []byte("cmd"), SM3Hash: []byte("hash")}
	}

	rn.CompactLogs(5000)

	// 请求 startIdx=5001（logStartIndex）→ 正常返回，但 Command/SM3Hash 为 nil
	entries, err := rn.GetLogEntries(5001, 5010)
	if err != nil {
		t.Fatalf("GetLogEntries(5001,5010): err=%v, 期望 nil", err)
	}
	if len(entries) != 10 {
		t.Fatalf("entries len=%d, 期望 10", len(entries))
	}
	// Index/Term 元数据应保留
	if entries[0].Index != 5001 || entries[0].Term != 10 {
		t.Errorf("entries[0]: Index=%d Term=%d, 期望 5001/10", entries[0].Index, entries[0].Term)
	}
	// Command/SM3Hash 应保留（Index=5001 > upToIndex=5000，未被压缩）
	if entries[0].Command == nil || entries[0].Sm3Hash == nil {
		t.Error("entries[0] Command/SM3Hash 应保留（Index=5001 未被压缩）")
	}
	t.Log("✓ startIdx=5001 >= logStartIndex=5001 → 正常返回，元数据和大字段均保留")
}

// TestKnife2_NoCompaction_LogStartIndexZero
// 刀二验证: 未调用 CompactLogs 时，logStartIndex=0，GetLogEntries 正常工作
func TestKnife2_NoCompaction_LogStartIndexZero(t *testing.T) {
	rn := &RaftNode{
		id:   "node-1",
		logs: make([]RaftLog, 100),
	}
	for i := range rn.logs {
		rn.logs[i] = RaftLog{Index: int64(i + 1), Term: 10, Command: []byte("cmd")}
	}

	if rn.GetLogStartIndex() != 0 {
		t.Errorf("logStartIndex=%d, 期望 0（未压缩）", rn.GetLogStartIndex())
	}

	entries, err := rn.GetLogEntries(1, 10)
	if err != nil {
		t.Fatalf("GetLogEntries(1,10): err=%v, 期望 nil", err)
	}
	if len(entries) != 10 {
		t.Fatalf("entries len=%d, 期望 10", len(entries))
	}
	t.Log("✓ 未压缩时 logStartIndex=0，GetLogEntries 正常工作")
}

// TestKnife2_SuccessiveCompaction
// 刀二验证: 多次 CompactLogs 调用，logStartIndex 单调递增
func TestKnife2_SuccessiveCompaction(t *testing.T) {
	rn := &RaftNode{
		id:   "node-1",
		logs: make([]RaftLog, 10000),
	}
	for i := range rn.logs {
		rn.logs[i] = RaftLog{Index: int64(i + 1), Term: 10, Command: []byte("cmd")}
	}

	rn.CompactLogs(3000)
	if rn.GetLogStartIndex() != 3001 {
		t.Errorf("第一次压缩: logStartIndex=%d, 期望 3001", rn.GetLogStartIndex())
	}

	rn.CompactLogs(6000)
	if rn.GetLogStartIndex() != 6001 {
		t.Errorf("第二次压缩: logStartIndex=%d, 期望 6001", rn.GetLogStartIndex())
	}

	// 旧的压缩区域仍然返回 ErrCompacted
	_, err := rn.GetLogEntries(3001, 3100)
	if err != ErrCompacted {
		t.Errorf("GetLogEntries(3001,3100): err=%v, 期望 ErrCompacted（已被第二次压缩覆盖）", err)
	}

	// 新的未压缩区域正常工作
	entries, err := rn.GetLogEntries(6001, 6010)
	if err != nil {
		t.Fatalf("GetLogEntries(6001,6010): err=%v, 期望 nil", err)
	}
	if len(entries) != 10 {
		t.Fatalf("entries len=%d, 期望 10", len(entries))
	}
	t.Log("✓ 多次压缩 logStartIndex 单调递增: 3001 → 6001")
}

// TestKnife2_CompactZeroEntries_NoChange
// 刀二验证: CompactLogs(0) 不压缩任何条目（logs 从 Index=1 开始），logStartIndex 不变
func TestKnife2_CompactZeroEntries_NoChange(t *testing.T) {
	rn := &RaftNode{
		id:   "node-1",
		logs: make([]RaftLog, 100),
	}
	for i := range rn.logs {
		rn.logs[i] = RaftLog{Index: int64(i + 1), Term: 10, Command: []byte("cmd")}
	}

	rn.CompactLogs(0)

	// logs 从 Index=1 开始，Index<=0 无条目，compacted=0，logStartIndex 不变
	if rn.GetLogStartIndex() != 0 {
		t.Errorf("logStartIndex=%d, 期望 0（无条目被压缩）", rn.GetLogStartIndex())
	}

	// Index=1 的条目 Command 应保留
	if rn.logs[0].Command == nil {
		t.Error("logs[0].Command 应保留（Index=1 > 0，未被压缩）")
	}
	t.Log("✓ CompactLogs(0) 无条目被压缩（logs 从 Index=1 开始），logStartIndex=0")
}
