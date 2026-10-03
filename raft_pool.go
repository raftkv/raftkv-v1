// =========================================================================
// RaftKV 确定性引擎 — 零拷贝内存池 + 批量提交
//
// 改造一（白皮书 Phase 2 ✅ [可立刻试点]）：
//   1. sync.Pool 复用 bytes.Buffer，消除 json.Marshal 每次分配
//   2. BatchProposer 200μs 攒批窗口，合并提交减少 RPC 轮次
//
// 设计原则：
//   - 不修改 raft.go 核心逻辑，纯新增能力层
//   - 线程安全，无锁热路径优先
//   - 优雅关闭：Close 时刷新剩余批次
// =========================================================================

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// =========================================================================
// 第一部分: 序列化缓冲池（sync.Pool 零拷贝复用）
// =========================================================================

// serializePool JSON 序列化缓冲池
var serializePool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// PooledMarshal 使用 sync.Pool 复用 buffer 进行 JSON 序列化
// 返回的 data 在调用 PutBuffer 前有效
// 典型用法：
//
//	data, buf := PooledMarshal(cmd)
//	defer PutBuffer(buf)
//	send(data)
func PooledMarshal(v interface{}) ([]byte, *bytes.Buffer, error) {
	buf := serializePool.Get().(*bytes.Buffer)
	buf.Reset()
	enc := json.NewEncoder(buf)
	if err := enc.Encode(v); err != nil {
		serializePool.Put(buf)
		return nil, nil, fmt.Errorf("pooled marshal: %w", err)
	}
	// Encode 追加换行符，裁掉
	data := buf.Bytes()
	if n := len(data); n > 0 && data[n-1] == '\n' {
		data = data[:n-1]
	}
	return data, buf, nil
}

// PutBuffer 归还 buffer 到池
func PutBuffer(buf *bytes.Buffer) {
	serializePool.Put(buf)
}

// PooledUnmarshal 使用 sync.Pool 复用 buffer 进行 JSON 反序列化
func PooledUnmarshal(data []byte, v interface{}) error {
	buf := serializePool.Get().(*bytes.Buffer)
	defer serializePool.Put(buf)
	buf.Reset()
	if _, err := buf.Write(data); err != nil {
		return fmt.Errorf("pooled unmarshal write: %w", err)
	}
	dec := json.NewDecoder(buf)
	return dec.Decode(v)
}

// =========================================================================
// 第二部分: 批量提案器（微批处理窗口）
// =========================================================================

const (
	defaultBatchSize   = 64                     // 每批最大条数
	defaultBatchWindow = 200 * time.Microsecond // 攒批时间窗口 200μs
)

// BatchProposer 批量提案器
// 在 batchWindow 时间内收集 RaftLog，批量提交以减少 RPC 轮次
type BatchProposer struct {
	mu          sync.Mutex
	batch       []RaftLog
	batchSize   int
	batchWindow time.Duration
	flushCh     chan struct{}
	commitFn    func([]RaftLog) error
	closed      bool
	closeCh     chan struct{}
	wg          sync.WaitGroup

	// 统计
	totalProposed int64
	totalFlushed  int64
	commitErrs    int64
	lastErr       error
}

// NewBatchProposer 创建批量提案器
// batchSize: 每批最大条数（<=0 用默认值 64）
// batchWindow: 攒批时间窗口（<=0 用默认值 200μs）
// commitFn: 实际提交回调，接收一批 RaftLog
func NewBatchProposer(batchSize int, batchWindow time.Duration, commitFn func([]RaftLog) error) *BatchProposer {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	if batchWindow <= 0 {
		batchWindow = defaultBatchWindow
	}

	bp := &BatchProposer{
		batch:       make([]RaftLog, 0, batchSize),
		batchSize:   batchSize,
		batchWindow: batchWindow,
		flushCh:     make(chan struct{}, 1),
		commitFn:    commitFn,
		closeCh:     make(chan struct{}),
	}
	bp.wg.Add(1)
	go bp.flushLoop()
	return bp
}

// Propose 提交一条日志到批量缓冲
// 当缓冲达到 batchSize 时立即触发刷新
func (bp *BatchProposer) Propose(log RaftLog) error {
	bp.mu.Lock()
	if bp.closed {
		bp.mu.Unlock()
		return fmt.Errorf("batch proposer 已关闭")
	}
	bp.batch = append(bp.batch, log)
	bp.totalProposed++
	shouldFlush := len(bp.batch) >= bp.batchSize
	bp.mu.Unlock()

	if shouldFlush {
		select {
		case bp.flushCh <- struct{}{}:
		default: // 已有 pending flush，不重复触发
		}
	}
	return nil
}

// flushLoop 定时 + 事件驱动刷新循环
func (bp *BatchProposer) flushLoop() {
	defer bp.wg.Done()
	timer := time.NewTimer(bp.batchWindow)
	defer timer.Stop()

	for {
		select {
		case <-bp.closeCh:
			bp.doFlush()
			return
		case <-bp.flushCh:
			bp.doFlush()
			timer.Reset(bp.batchWindow)
		case <-timer.C:
			bp.doFlush()
			timer.Reset(bp.batchWindow)
		}
	}
}

// doFlush 执行批量提交
func (bp *BatchProposer) doFlush() {
	bp.mu.Lock()
	if len(bp.batch) == 0 {
		bp.mu.Unlock()
		return
	}
	batch := bp.batch
	bp.batch = make([]RaftLog, 0, bp.batchSize)
	bp.totalFlushed += int64(len(batch))
	bp.mu.Unlock()

	if err := bp.commitFn(batch); err != nil {
		atomic.AddInt64(&bp.commitErrs, 1)
		bp.mu.Lock()
		bp.lastErr = err
		bp.mu.Unlock()
		log.Printf("[BatchProposer] 批量提交失败: %v", err)
	}
}

// CommitErrors 返回累计提交错误数与最近一次错误
func (bp *BatchProposer) CommitErrors() (int64, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return atomic.LoadInt64(&bp.commitErrs), bp.lastErr
}

// Stats 返回批量提案统计
func (bp *BatchProposer) Stats() (proposed int64, flushed int64) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.totalProposed, bp.totalFlushed
}

// Close 关闭批量提案器，刷新剩余批次后退出
func (bp *BatchProposer) Close() error {
	bp.mu.Lock()
	if bp.closed {
		bp.mu.Unlock()
		return nil
	}
	bp.closed = true
	bp.mu.Unlock()

	close(bp.closeCh)
	bp.wg.Wait()
	return nil
}
