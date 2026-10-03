// =========================================================================
// RaftKV Module03 — 高吞吐数据流转 Pipeline
//
// WorkerPool 工作池：支持多生产者并发写入 + 多消费者并发读取。
//
// 设计要点：
//   1. N 个 worker goroutine 并发消费同一 in channel
//   2. 多个生产者可并发调用 Submit（线程安全）
//   3. 处理结果通过 out channel 汇聚（保持并发完成顺序，不保证输入顺序）
//   4. 优雅关闭：Close 后排空 in，等待所有 worker 退出，关闭 out
//   5. 提供顺序保持变体 OrderedWorkerPool（单 worker，保序）
//
// 在 Pipeline 中的角色：
//   - 作为 Process Stage 的并行执行引擎
//   - 适用于 CPU 密集型处理（多核并行）
//
// 零外部依赖：仅使用 Go 标准库
// =========================================================================

package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
)

// -------------------------------------------------------------------------
// WorkerPool 实现
// -------------------------------------------------------------------------

// WorkerPool 多消费者工作池。
type WorkerPool struct {
	workers int
	proc    ProcessFunc

	in   chan Item
	Out  chan Item
	done chan struct{}

	wg sync.WaitGroup

	totalIn   atomic.Int64
	totalOut  atomic.Int64
	totalDrop atomic.Int64 // 被 proc 过滤掉的
}

// NewWorkerPool 创建工作池。
//
//   - workers: 并行度（<1 修正为 1）
//   - proc:    处理函数
//   - inBuf/outBuf: channel 缓冲
func NewWorkerPool(workers int, proc ProcessFunc, inBuf, outBuf int) *WorkerPool {
	if workers < 1 {
		workers = 1
	}
	if proc == nil {
		proc = func(item Item) (Item, bool) { return item, true }
	}
	if inBuf < 0 {
		inBuf = 0
	}
	if outBuf < 0 {
		outBuf = 0
	}
	return &WorkerPool{
		workers: workers,
		proc:    proc,
		in:      make(chan Item, inBuf),
		Out:     make(chan Item, outBuf),
		done:    make(chan struct{}),
	}
}

// Submit 提交一个 Item 到工作池（多生产者并发安全）。
//
// 若 in channel 满，将阻塞（背压）。
func (p *WorkerPool) Submit(item Item) {
	p.totalIn.Add(1)
	p.in <- item
}

// Start 启动所有 worker。
func (p *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.runWorker(ctx)
	}
	go func() {
		p.wg.Wait()
		close(p.Out)
	}()
}

// Close 关闭工作池：停止接收新提交，排空 in，等待所有 worker 退出。
//
// 调用后 Out 会被关闭。
func (p *WorkerPool) Close() {
	close(p.in)
	p.wg.Wait()
}

func (p *WorkerPool) runWorker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-p.in:
			if !ok {
				return
			}
			out, keep := p.proc(item)
			if !keep {
				p.totalDrop.Add(1)
				continue
			}
			select {
			case p.Out <- out:
				p.totalOut.Add(1)
			case <-ctx.Done():
				return
			}
		}
	}
}

// WorkerPoolStats 工作池统计。
type WorkerPoolStats struct {
	In   int64
	Out  int64
	Drop int64
}

// Stats 返回累计统计快照。
func (p *WorkerPool) Stats() WorkerPoolStats {
	return WorkerPoolStats{
		In:   p.totalIn.Load(),
		Out:  p.totalOut.Load(),
		Drop: p.totalDrop.Load(),
	}
}

// -------------------------------------------------------------------------
// OrderedWorkerPool — 单 worker，保序
// -------------------------------------------------------------------------

// OrderedWorkerPool 单 worker 工作池，保证输入顺序等于输出顺序。
//
// 适用于需要顺序保持的场景（如 WAL 顺序写入）。
type OrderedWorkerPool struct {
	proc ProcessFunc
	in   chan Item
	Out  chan Item
	wg   sync.WaitGroup

	totalIn  atomic.Int64
	totalOut atomic.Int64
}

// NewOrderedWorkerPool 创建保序工作池。
func NewOrderedWorkerPool(proc ProcessFunc, inBuf, outBuf int) *OrderedWorkerPool {
	if proc == nil {
		proc = func(item Item) (Item, bool) { return item, true }
	}
	if inBuf < 0 {
		inBuf = 0
	}
	if outBuf < 0 {
		outBuf = 0
	}
	return &OrderedWorkerPool{
		proc: proc,
		in:   make(chan Item, inBuf),
		Out:  make(chan Item, outBuf),
	}
}

// Submit 提交（多生产者并发安全，但输出顺序由提交完成顺序决定）。
func (p *OrderedWorkerPool) Submit(item Item) {
	p.totalIn.Add(1)
	p.in <- item
}

// Start 启动单 worker。
func (p *OrderedWorkerPool) Start(ctx context.Context) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(p.Out)
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-p.in:
				if !ok {
					return
				}
				out, keep := p.proc(item)
				if !keep {
					continue
				}
				select {
				case p.Out <- out:
					p.totalOut.Add(1)
				case <-ctx.Done():
					return
				}
			}
		}
	}()
}

// Close 关闭。
func (p *OrderedWorkerPool) Close() {
	close(p.in)
	p.wg.Wait()
}

// Stats 返回累计统计。
func (p *OrderedWorkerPool) Stats() WorkerPoolStats {
	return WorkerPoolStats{
		In:  p.totalIn.Load(),
		Out: p.totalOut.Load(),
	}
}
