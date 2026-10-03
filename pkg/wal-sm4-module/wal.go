// =========================================================================
// RaftKV Module02 — WAL 预写式日志（1GB 预分配 + 批量 fsync group commit）
//
// 设计要点（复用 Module01 Raft 引擎的 WAL 架构，独立化去除 Raft 耦合）：
//   1. 文件预分配 1GB（f.Truncate(1<<30)），消除运行时扩展开销
//   2. 批量 group commit：攒批 256 条或 5ms 窗口，单次 file.Sync()
//   3. 崩溃恢复：扫描有效记录，遇到零区域自动停止
//
// WAL 记录格式（每条）：
//   [4 字节大端长度][payload]
//   预分配区域全零 → length=0 表示无更多记录
//
// 本文件为纯 Go 标准库自研，零外部依赖。
// =========================================================================

package walsm4

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

// WALEntry WAL 存储条目（通用字节记录，不耦合 Raft 类型）
type WALEntry struct {
	Index int64  `json:"index"` // 全局递增索引（从 1 开始，0 表示无索引）
	Term  int64  `json:"term"`  // 任期号（可选，通用日志可为 0）
	Data  []byte `json:"data"`  // 载荷字节（明文或密文，由上层决定）
}

// WAL 预写式日志（1GB 预分配 + 批量 fsync group commit）
type WAL struct {
	mu     sync.Mutex // 保护文件读写位置
	file   *os.File
	path   string
	offset int64 // 当前写入偏移量
	closed bool

	batchMu sync.Mutex // 保护批量缓冲
	batch   []WALEntry
	timer   *time.Timer
	flushCh chan struct{}
	closeCh chan struct{}
	wg      sync.WaitGroup
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
		batch:   make([]WALEntry, 0, walMaxBatch),
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

// Append 追加一条 WALEntry 到批量缓冲
func (w *WAL) Append(entry WALEntry) error {
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

// AppendData 便捷方法：追加原始字节载荷（Index/Term 置 0）
func (w *WAL) AppendData(data []byte) error {
	return w.Append(WALEntry{Data: data})
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
	w.batch = make([]WALEntry, 0, walMaxBatch)
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
func (w *WAL) Replay() ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}

	var entries []WALEntry
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
		var entry WALEntry
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
