// =========================================================================
// RaftKV Module03 — 高吞吐数据流转 Pipeline
//
// RingBuffer 环形缓冲区：用于 Stage 之间的有界缓冲，实现背压流控。
//
// 设计要点：
//   1. 固定容量，无动态扩容，内存占用可预测
//   2. 单生产者-单消费者（MPMC 由外部 sync.Mutex 保护，或仅用于 SPSC 场景）
//   3. 满时 Push 阻塞（背压），空时 Pop 阻塞
//   4. 提供 TryPush/TryPop 非阻塞变体，便于探测
//   5. 关闭后 Pop 排空剩余并返回 ok=false
//
// 在 Pipeline 中，RingBuffer 主要作为 channel 的补充：
//   - channel 适合跨 goroutine 解耦
//   - RingBuffer 适合需要显式容量管理、零分配、可观测的场景
//
// 零外部依赖：仅使用 Go 标准库
// =========================================================================

package pipeline

import (
	"sync"
	"sync/atomic"
)

// -------------------------------------------------------------------------
// RingBuffer 实现
// -------------------------------------------------------------------------

// RingBuffer 固定容量的环形缓冲区。
//
// 线程安全：内部 sync.Mutex 保护读写指针。
// 适用于 MPMC（多生产者多消费者）场景，但高并发下争用较强；
// 若需极高吞吐，建议使用 channel 或 SPSC 专用实现。
type RingBuffer struct {
	buf   []Item
	cap   int
	head  int // 下一个写入位置
	tail  int // 下一个读取位置
	count int // 当前元素数

	mu       sync.Mutex
	notFull  *sync.Cond // 等待有空位
	notEmpty *sync.Cond // 等待有数据

	closed atomic.Bool

	// 统计
	totalPushed  atomic.Int64
	totalPopped  atomic.Int64
	totalDropped atomic.Int64 // 因关闭而丢弃的 Push
}

// NewRingBuffer 创建容量为 cap 的 RingBuffer。
//
// cap < 1 时自动修正为 1。
func NewRingBuffer(capacity int) *RingBuffer {
	if capacity < 1 {
		capacity = 1
	}
	rb := &RingBuffer{
		buf: make([]Item, capacity),
		cap: capacity,
	}
	rb.notFull = sync.NewCond(&rb.mu)
	rb.notEmpty = sync.NewCond(&rb.mu)
	return rb
}

// Push 阻塞写入一个 Item。
//
//   - 缓冲区满时阻塞，直到有消费者 Pop 出空位（背压）
//   - 缓冲区已关闭时返回 false
func (rb *RingBuffer) Push(item Item) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for rb.count == rb.cap && !rb.closed.Load() {
		rb.notFull.Wait()
	}
	if rb.closed.Load() {
		rb.totalDropped.Add(1)
		return false
	}
	rb.buf[rb.head] = item
	rb.head = (rb.head + 1) % rb.cap
	rb.count++
	rb.totalPushed.Add(1)
	rb.notEmpty.Signal()
	return true
}

// TryPush 非阻塞写入。满时立即返回 false（不阻塞）。
func (rb *RingBuffer) TryPush(item Item) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.closed.Load() {
		rb.totalDropped.Add(1)
		return false
	}
	if rb.count == rb.cap {
		return false
	}
	rb.buf[rb.head] = item
	rb.head = (rb.head + 1) % rb.cap
	rb.count++
	rb.totalPushed.Add(1)
	rb.notEmpty.Signal()
	return true
}

// Pop 阻塞读取一个 Item。
//
//   - 缓冲区空时阻塞，直到有生产者 Push 或 Close
//   - 缓冲区已关闭且排空时返回 (Item{}, false)
func (rb *RingBuffer) Pop() (Item, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for rb.count == 0 && !rb.closed.Load() {
		rb.notEmpty.Wait()
	}
	if rb.count == 0 {
		return Item{}, false
	}
	item := rb.buf[rb.tail]
	rb.buf[rb.tail] = Item{} // 帮助 GC
	rb.tail = (rb.tail + 1) % rb.cap
	rb.count--
	rb.totalPopped.Add(1)
	rb.notFull.Signal()
	return item, true
}

// Close 关闭 RingBuffer。
//
//   - 之后 Push 返回 false
//   - Pop 仍可排空剩余数据，排空后返回 false
func (rb *RingBuffer) Close() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.closed.Store(true)
	rb.notFull.Broadcast()
	rb.notEmpty.Broadcast()
}

// Len 当前元素数（即时快照）。
func (rb *RingBuffer) Len() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.count
}

// Cap 容量。
func (rb *RingBuffer) Cap() int {
	return rb.cap
}

// Stats 返回累计统计。
type RingBufferStats struct {
	Pushed  int64
	Popped  int64
	Dropped int64
}

// Stats 返回累计统计快照。
func (rb *RingBuffer) Stats() RingBufferStats {
	return RingBufferStats{
		Pushed:  rb.totalPushed.Load(),
		Popped:  rb.totalPopped.Load(),
		Dropped: rb.totalDropped.Load(),
	}
}

// IsClosed 是否已关闭。
func (rb *RingBuffer) IsClosed() bool {
	return rb.closed.Load()
}
