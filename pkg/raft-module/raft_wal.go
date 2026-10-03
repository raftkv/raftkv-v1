// =========================================================================
// RaftKV Module01 — WAL 预写式日志（1GB 预分配 + 批量 fsync）
//
// 改造二（白皮书 Phase 2 ✅ [可立刻试点]）：
//   1. 文件预分配 1GB（f.Truncate(1<<30)），消除运行时扩展开销
//   2. 批量 group commit：攒批 256 条或 5ms 窗口，单次 file.Sync()
//   3. 崩溃恢复：扫描有效记录，遇到零区域自动停止
//
// WAL 记录格式（每条）：
//   [4 字节大端长度][JSON payload]
//   预分配区域全零 → length=0 表示无更多记录
//
// 本文件从原 raftkv/raft_wal.go 原样剥离，纯标准库零依赖。
// =========================================================================

package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// =========================================================================
// 编译时常量
// =========================================================================

const (
	walPreallocSize  = int64(1 << 30)       // 1GB 预分配
	walMaxBatch      = 256                  // 批量 fsync 最大条数
	walFlushInterval = 5 * time.Millisecond // 批量 fsync 时间窗口
	walLenPrefix     = 4                    // 每条记录长度前缀字节数
)

// =========================================================================
// WAL 数据结构
// =========================================================================

// WAL 预写式日志（1GB 预分配 + 批量 fsync）
type WAL struct {
	mu     sync.Mutex // 保护文件读写位置
	file   *os.File
	path   string
	offset int64 // 当前写入偏移量
	closed bool

	batchMu sync.Mutex // 保护批量缓冲
	batch   []walEntry
	timer   *time.Timer
	flushCh chan struct{}
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// walEntry WAL 存储条目
type walEntry struct {
	Index int64  `json:"index"`
	Term  int64  `json:"term"`
	Data  []byte `json:"data"` // 序列化后的命令字节（可能已加密）
}

// =========================================================================
// WAL 生命周期
// =========================================================================

// NewWAL 创建或打开 WAL 文件
// 若文件 < 1GB 则预分配至 1GB，然后扫描已有记录定位写入偏移
func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("打开 WAL 失败: %w", err)
	}

	// 预分配 1GB（仅当当前文件小于 1GB）
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("WAL stat 失败: %w", err)
	}
	if fi.Size() < walPreallocSize {
		if err := f.Truncate(walPreallocSize); err != nil {
			f.Close()
			return nil, fmt.Errorf("WAL 预分配 1GB 失败: %w", err)
		}
	}

	// 扫描已有记录，定位写入偏移
	offset, err := scanWALOffset(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("WAL 扫描失败: %w", err)
	}

	w := &WAL{
		file:    f,
		path:    path,
		offset:  offset,
		batch:   make([]walEntry, 0, walMaxBatch),
		flushCh: make(chan struct{}, 1),
		closeCh: make(chan struct{}),
	}

	// 定位到写入偏移
	if _, err := f.Seek(offset, 0); err != nil {
		f.Close()
		return nil, fmt.Errorf("WAL 定位失败: %w", err)
	}

	w.timer = time.NewTimer(walFlushInterval)
	w.wg.Add(1)
	go w.flushLoop()

	return w, nil
}

// scanWALOffset 扫描 WAL 文件确定有效数据末尾偏移
func scanWALOffset(f *os.File) (int64, error) {
	if _, err := f.Seek(0, 0); err != nil {
		return 0, err
	}
	var offset int64
	prefix := make([]byte, walLenPrefix)
	for {
		n, err := io.ReadFull(f, prefix)
		if err == io.EOF || (err == io.ErrUnexpectedEOF && n == 0) {
			break
		}
		if err != nil {
			return 0, err
		}
		length := binary.BigEndian.Uint32(prefix)
		if length == 0 {
			break // 预分配零区域
		}
		if _, err := f.Seek(int64(length), 1); err != nil {
			return 0, err
		}
		offset += int64(walLenPrefix) + int64(length)
	}
	return offset, nil
}

// =========================================================================
// WAL 写入（批量缓冲 + group commit）
// =========================================================================

// Append 追加一条 walEntry 到批量缓冲
func (w *WAL) Append(entry walEntry) error {
	w.batchMu.Lock()
	if w.closed {
		w.batchMu.Unlock()
		return fmt.Errorf("WAL 已关闭")
	}
	w.batch = append(w.batch, entry)
	shouldFlush := len(w.batch) >= walMaxBatch
	w.batchMu.Unlock()

	if shouldFlush {
		select {
		case w.flushCh <- struct{}{}:
		default:
		}
	}
	return nil
}

// AppendRaftLog 便捷方法：将 RaftLog 序列化后追加
func (w *WAL) AppendRaftLog(log RaftLog) error {
	data, err := json.Marshal(log)
	if err != nil {
		return fmt.Errorf("WAL 序列化失败: %w", err)
	}
	return w.Append(walEntry{
		Index: log.Index,
		Term:  log.Term,
		Data:  data,
	})
}

// flushLoop 批量刷新循环
func (w *WAL) flushLoop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.closeCh:
			w.doFlush()
			return
		case <-w.flushCh:
			w.doFlush()
			w.timer.Reset(walFlushInterval)
		case <-w.timer.C:
			w.doFlush()
			w.timer.Reset(walFlushInterval)
		}
	}
}

// doFlush 执行批量写入 + 单次 fsync
func (w *WAL) doFlush() error {
	w.batchMu.Lock()
	if len(w.batch) == 0 {
		w.batchMu.Unlock()
		return nil
	}
	batch := w.batch
	w.batch = make([]walEntry, 0, walMaxBatch)
	w.batchMu.Unlock()

	w.mu.Lock()
	defer w.mu.Unlock()

	var prefix [walLenPrefix]byte
	for _, entry := range batch {
		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("WAL flush 序列化失败: %w", err)
		}
		binary.BigEndian.PutUint32(prefix[:], uint32(len(data)))
		if _, err := w.file.Write(prefix[:]); err != nil {
			return fmt.Errorf("WAL 写入长度前缀失败: %w", err)
		}
		if _, err := w.file.Write(data); err != nil {
			return fmt.Errorf("WAL 写入数据失败: %w", err)
		}
		w.offset += int64(walLenPrefix) + int64(len(data))
	}

	return w.file.Sync()
}

// =========================================================================
// WAL 读取（崩溃恢复）
// =========================================================================

// Replay 回放 WAL 中所有有效记录
func (w *WAL) Replay() ([]walEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}

	var entries []walEntry
	prefix := make([]byte, walLenPrefix)
	for {
		_, err := io.ReadFull(w.file, prefix)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}
		length := binary.BigEndian.Uint32(prefix)
		if length == 0 {
			break
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(w.file, data); err != nil {
			break
		}
		var entry walEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			continue
		}
		entries = append(entries, entry)
	}

	if _, err := w.file.Seek(w.offset, 0); err != nil {
		return nil, err
	}
	return entries, nil
}

// ReplayRaftLogs 便捷方法：回放并反序列化为 RaftLog
func (w *WAL) ReplayRaftLogs() ([]RaftLog, error) {
	entries, err := w.Replay()
	if err != nil {
		return nil, err
	}
	logs := make([]RaftLog, 0, len(entries))
	for _, e := range entries {
		var log RaftLog
		if err := json.Unmarshal(e.Data, &log); err != nil {
			continue
		}
		logs = append(logs, log)
	}
	return logs, nil
}

// =========================================================================
// WAL 关闭
// =========================================================================

// Close 关闭 WAL，刷新剩余批次后关闭文件
func (w *WAL) Close() error {
	w.batchMu.Lock()
	if w.closed {
		w.batchMu.Unlock()
		return nil
	}
	w.closed = true
	w.batchMu.Unlock()

	close(w.closeCh)
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// Path 返回 WAL 文件路径
func (w *WAL) Path() string {
	return w.path
}

// Offset 返回当前写入偏移量
func (w *WAL) Offset() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}
