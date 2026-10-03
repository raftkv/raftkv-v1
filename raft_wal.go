// =========================================================================
// RaftKV 确定性引擎 — WAL 预写式日志（1GB 预分配 + 批量 fsync）
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
// 设计原则：
//   - 不修改 raft.go 纯内存逻辑，WAL 为独立持久化层
//   - 批量 fsync 将 N 次 syscall 合并为 1 次
//   - 线程安全：写入互斥，批量缓冲独立锁
// =========================================================================

package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// =========================================================================
// 编译时常量
// =========================================================================

const (
	walPreallocSize = int64(1 << 30) // 1GB 预分配
	walLenPrefix    = 4              // 每条记录长度前缀字节数
)

var (
	walMaxBatch      = 256                  // 批量 fsync 最大条数
	walFlushInterval = 5 * time.Millisecond // 批量 fsync 时间窗口
)

// batch20: 全局 fsync 取证计数器（跨 WAL 重建持久化，原子操作不进写路径热区）
var (
	globalFsyncCount        atomic.Int64 // fsync 调用总次数
	globalFsyncTotalEntries atomic.Int64 // fsync 覆盖的总 entry 数
	globalFsyncTotalUs      atomic.Int64 // fsync 总耗时（微秒）
	globalFsyncMaxUs        atomic.Int64 // fsync 单次最大耗时（微秒）
	globalFlushCount        atomic.Int64 // doFlush 调用总次数（含空 flush）
	globalBatchSize1        atomic.Int64 // batch size = 1
	globalBatchSize2_4      atomic.Int64 // batch size 2-4
	globalBatchSize5_16     atomic.Int64 // batch size 5-16
	globalBatchSize17_64    atomic.Int64 // batch size 17-64
	globalBatchSize65_128   atomic.Int64 // batch size 65-128
	globalBatchSize129_256  atomic.Int64 // batch size 129-256
	globalBatchSize257Plus  atomic.Int64 // batch size > 256
)

func init() {
	if v := os.Getenv("WAL_FLUSH_INTERVAL_MS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			log.Fatalf("[wal] WAL_FLUSH_INTERVAL_MS 非法 (%q): 必须为正整数", v)
		}
		walFlushInterval = time.Duration(n) * time.Millisecond
	}
	if v := os.Getenv("WAL_MAX_BATCH"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			log.Fatalf("[wal] WAL_MAX_BATCH 非法 (%q): 必须为正整数", v)
		}
		walMaxBatch = n
	}
}

// =========================================================================
// WAL 数据结构
// =========================================================================

// WAL 预写式日志（1GB 预分配 + 批量 fsync + 轮转）
type WAL struct {
	mu     sync.Mutex // 保护文件读写位置
	file   *os.File
	path   string
	offset int64 // 当前写入偏移量
	closed bool

	closedWALs []string // 已轮转关闭的 WAL 文件路径（按时间顺序）

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

	// 发现已轮转的 closed WAL 文件（崩溃恢复）
	closedPattern := path + ".closed.*"
	if matches, _ := filepath.Glob(closedPattern); len(matches) > 0 {
		sort.Strings(matches)
		w.closedWALs = matches
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
// 遇到零长度前缀（预分配区域）或 EOF 时停止
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
		// 跳过 payload
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
// 当缓冲达到 walMaxBatch 时立即触发刷新
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

// flushLoop 批量刷新循环（事件驱动 + 定时驱动）
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

// doFlush 执行批量写入 + 单次 fsync（group commit 核心）
func (w *WAL) doFlush() error {
	w.batchMu.Lock()
	if len(w.batch) == 0 {
		w.batchMu.Unlock()
		globalFlushCount.Add(1)
		return nil
	}
	batch := w.batch
	w.batch = make([]walEntry, 0, walMaxBatch)
	batchLen := len(batch)
	w.batchMu.Unlock()

	w.mu.Lock()
	defer w.mu.Unlock()

	// 轮转检查：若当前 WAL 已达 90% 容量，先轮转再写入
	if w.offset > walPreallocSize*9/10 {
		if err := w.rotateLocked(); err != nil {
			return fmt.Errorf("WAL 轮转失败: %w", err)
		}
	}

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

	// 单次 fsync 批量持久化（N 次 write → 1 次 fsync）
	// batch20: fsync 取证——记录次数、batch size、耗时（后台 goroutine，非写路径热区）
	fsyncStart := time.Now()
	err := w.file.Sync()
	fsyncUs := time.Since(fsyncStart).Microseconds()

	globalFsyncCount.Add(1)
	globalFsyncTotalEntries.Add(int64(batchLen))
	globalFsyncTotalUs.Add(fsyncUs)
	globalFlushCount.Add(1)
	for {
		old := globalFsyncMaxUs.Load()
		if fsyncUs <= old || globalFsyncMaxUs.CompareAndSwap(old, fsyncUs) {
			break
		}
	}

	switch {
	case batchLen == 1:
		globalBatchSize1.Add(1)
	case batchLen <= 4:
		globalBatchSize2_4.Add(1)
	case batchLen <= 16:
		globalBatchSize5_16.Add(1)
	case batchLen <= 64:
		globalBatchSize17_64.Add(1)
	case batchLen <= 128:
		globalBatchSize65_128.Add(1)
	case batchLen <= 256:
		globalBatchSize129_256.Add(1)
	default:
		globalBatchSize257Plus.Add(1)
	}

	return err
}

// rotateLocked 轮转 WAL：关闭当前文件 → rename 为 .closed.NNN → 创建新文件
// 调用前必须持有 w.mu 锁
func (w *WAL) rotateLocked() error {
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("轮转前 fsync 失败: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("轮转前关闭失败: %w", err)
	}

	closedPath := fmt.Sprintf("%s.closed.%d", w.path, time.Now().UnixNano())
	if err := os.Rename(w.path, closedPath); err != nil {
		return fmt.Errorf("轮转 rename 失败: %w", err)
	}

	w.closedWALs = append(w.closedWALs, closedPath)

	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("轮转后创建新 WAL 失败: %w", err)
	}
	if err := f.Truncate(walPreallocSize); err != nil {
		f.Close()
		return fmt.Errorf("轮转后预分配失败: %w", err)
	}

	w.file = f
	w.offset = 0
	log.Printf("[wal] 轮转完成: %s → %s, closedWALs=%d", w.path, closedPath, len(w.closedWALs))
	return nil
}

// replayWALFile 从指定路径回放 WAL 记录（不修改文件）
func replayWALFile(path string) ([]walEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []walEntry
	prefix := make([]byte, walLenPrefix)
	for {
		_, err := io.ReadFull(f, prefix)
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
		if _, err := io.ReadFull(f, data); err != nil {
			break
		}
		var entry walEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			continue
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// =========================================================================
// WAL 读取（崩溃恢复）
// =========================================================================

// Replay 回放 WAL 中所有有效记录（含已轮转的 closed WALs）
func (w *WAL) Replay() ([]walEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var entries []walEntry

	// 先回放已轮转的 closed WALs（按时间顺序）
	for _, closedPath := range w.closedWALs {
		closedEntries, err := replayWALFile(closedPath)
		if err != nil {
			return nil, fmt.Errorf("回放 closed WAL %s 失败: %w", closedPath, err)
		}
		entries = append(entries, closedEntries...)
	}

	// 回放当前 WAL
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}

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
			break // 不完整记录，崩溃时未写完
		}
		var entry walEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			continue // 跳过损坏记录
		}
		entries = append(entries, entry)
	}

	// 恢复写入位置
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

// Flush 强制刷新批缓冲到磁盘
func (w *WAL) Flush() error {
	return w.doFlush()
}

// HealthCheck WAL 健康检测，不修改WAL状态，仅执行可用性检测
func (w *WAL) HealthCheck() error {
	if w.closed {
		return fmt.Errorf("WAL文件句柄已关闭")
	}

	dir := filepath.Dir(w.path)
	if dir == "" {
		dir = "."
	}

	dirInfo, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("WAL目录缺失: %s", dir)
		}
		return fmt.Errorf("WAL目录访问失败: %w", err)
	}
	if !dirInfo.IsDir() {
		return fmt.Errorf("WAL路径不是目录: %s", dir)
	}

	probeFile := filepath.Join(dir, ".hcprobe.tmp")
	f, err := os.Create(probeFile)
	if err != nil {
		return fmt.Errorf("WAL不可写: %w", err)
	}
	f.Close()
	os.Remove(probeFile)

	if w.file == nil {
		return fmt.Errorf("WAL文件句柄已关闭")
	}

	return nil
}

// walHealthCheck 独立WAL健康检查函数（不需要WAL对象）
func walHealthCheck(walPath string) error {
	dir := filepath.Dir(walPath)
	if dir == "" {
		dir = "."
	}

	dirInfo, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("WAL目录缺失: %s", dir)
		}
		return fmt.Errorf("WAL目录访问失败: %w", err)
	}
	if !dirInfo.IsDir() {
		return fmt.Errorf("WAL路径不是目录: %s", dir)
	}

	_, err = os.Stat(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("WAL文件缺失: %s", walPath)
		}
		return fmt.Errorf("WAL文件访问失败: %w", err)
	}

	probeFile := filepath.Join(dir, ".hcprobe.tmp")
	f, err := os.Create(probeFile)
	if err != nil {
		return fmt.Errorf("WAL不可写: %w", err)
	}
	f.Close()
	os.Remove(probeFile)

	return nil
}

// Offset 返回当前写入偏移量
func (w *WAL) Offset() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}

// ClosedWALs 返回已轮转的 closed WAL 文件路径列表
func (w *WAL) ClosedWALs() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.closedWALs...)
}

// RemoveClosedWALs 删除所有已轮转的 closed WAL 文件并清空列表
func (w *WAL) RemoveClosedWALs() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, p := range w.closedWALs {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("删除 closed WAL %s 失败: %w", p, err)
		}
	}
	w.closedWALs = nil
	return nil
}

// WALStatsSnapshot WAL fsync 取证快照
type WALStatsSnapshot struct {
	FsyncCount        int64   `json:"fsync_count"`
	FlushCount        int64   `json:"flush_count"`
	FsyncTotalEntries int64   `json:"fsync_total_entries"`
	FsyncTotalUs      int64   `json:"fsync_total_us"`
	FsyncAvgUs        int64   `json:"fsync_avg_us"`
	FsyncMaxUs        int64   `json:"fsync_max_us"`
	AvgBatchSize      float64 `json:"avg_batch_size"`
	BatchSize1        int64   `json:"batch_size_1"`
	BatchSize2_4      int64   `json:"batch_size_2_4"`
	BatchSize5_16     int64   `json:"batch_size_5_16"`
	BatchSize17_64    int64   `json:"batch_size_17_64"`
	BatchSize65_128   int64   `json:"batch_size_65_128"`
	BatchSize129_256  int64   `json:"batch_size_129_256"`
	BatchSize257Plus  int64   `json:"batch_size_257_plus"`
}

// Stats 返回 WAL fsync 取证快照
func (w *WAL) Stats() WALStatsSnapshot {
	fc := globalFsyncCount.Load()
	te := globalFsyncTotalEntries.Load()
	tu := globalFsyncTotalUs.Load()
	var avgUs int64
	var avgBatch float64
	if fc > 0 {
		avgUs = tu / fc
		avgBatch = float64(te) / float64(fc)
	}
	return WALStatsSnapshot{
		FsyncCount:        fc,
		FlushCount:        globalFlushCount.Load(),
		FsyncTotalEntries: te,
		FsyncTotalUs:      tu,
		FsyncAvgUs:        avgUs,
		FsyncMaxUs:        globalFsyncMaxUs.Load(),
		AvgBatchSize:      avgBatch,
		BatchSize1:        globalBatchSize1.Load(),
		BatchSize2_4:      globalBatchSize2_4.Load(),
		BatchSize5_16:     globalBatchSize5_16.Load(),
		BatchSize17_64:    globalBatchSize17_64.Load(),
		BatchSize65_128:   globalBatchSize65_128.Load(),
		BatchSize129_256:  globalBatchSize129_256.Load(),
		BatchSize257Plus:  globalBatchSize257Plus.Load(),
	}
}

// ResetGlobalStats 重置全局 fsync 取证计数器
func ResetGlobalStats() {
	globalFsyncCount.Store(0)
	globalFsyncTotalEntries.Store(0)
	globalFsyncTotalUs.Store(0)
	globalFsyncMaxUs.Store(0)
	globalFlushCount.Store(0)
	globalBatchSize1.Store(0)
	globalBatchSize2_4.Store(0)
	globalBatchSize5_16.Store(0)
	globalBatchSize17_64.Store(0)
	globalBatchSize65_128.Store(0)
	globalBatchSize129_256.Store(0)
	globalBatchSize257Plus.Store(0)
}
