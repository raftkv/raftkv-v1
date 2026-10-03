// =========================================================================
// RaftKV Module03 — 并发安全单元测试
//
// 用 go test -count=200 -race（Linux/有 gcc 环境）或 go test -count=200（Windows）
// 高频并发压测，增强并发安全置信度。
// =========================================================================

package pipeline

import (
	"sync"
	"sync/atomic"
	"testing"
)

// TestConcurrentPipeline 多生产者 + 多消费者 + 多 worker 并发压测。
func TestConcurrentPipeline(t *testing.T) {
	const N = 20000
	const producers = 8
	const consumers = 4

	cfg := DefaultPipelineConfig()
	cfg.IngestWorkers = 1
	cfg.ProcessWorkers = 4
	cfg.OutputWorkers = 1
	cfg.EnableBatch = false
	cfg.IngestBuf = 4096
	cfg.ProcessBuf = 4096
	cfg.OutputBuf = 4096

	ingest := IdentityFunc()
	process := MapFunc(func(item Item) Item {
		if v, ok := item.Data.(int); ok {
			item.Data = v + 1
		}
		return item
	})
	output := IdentityFunc()

	p := NewPipeline(cfg, ingest, process, output)
	outCh, err := p.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	var recvCount int64
	recvIDs := make(map[uint64]struct{})
	var mu sync.Mutex

	var consumerWg sync.WaitGroup
	for c := 0; c < consumers; c++ {
		consumerWg.Add(1)
		go func() {
			defer consumerWg.Done()
			for item := range outCh {
				p.MarkOutput()
				atomic.AddInt64(&recvCount, 1)
				mu.Lock()
				recvIDs[item.ID] = struct{}{}
				mu.Unlock()
			}
		}()
	}

	var prodWg sync.WaitGroup
	per := N / producers
	for pp := 0; pp < producers; pp++ {
		prodWg.Add(1)
		go func(base int) {
			defer prodWg.Done()
			for i := 0; i < per; i++ {
				p.Submit(Item{
					ID:   uint64(base*per + i + 1),
					Data: base*per + i,
				})
			}
		}(pp)
	}
	prodWg.Wait()
	p.Close()
	consumerWg.Wait()

	if got := atomic.LoadInt64(&recvCount); got != int64(N) {
		t.Errorf("recvCount = %d, want %d", got, N)
	}
	if got := len(recvIDs); got != N {
		t.Errorf("unique IDs = %d, want %d (duplicate or lost)", got, N)
	}
}

// TestRingBufferConcurrent RingBuffer 多生产者 + 单消费者并发压测。
func TestRingBufferConcurrent(t *testing.T) {
	const N = 10000
	const cap = 256
	const producers = 4

	rb := NewRingBuffer(cap)

	var recvCount int64
	done := make(chan struct{})
	go func() {
		for {
			_, ok := rb.Pop()
			if !ok {
				break
			}
			atomic.AddInt64(&recvCount, 1)
		}
		close(done)
	}()

	var prodWg sync.WaitGroup
	per := N / producers
	for pp := 0; pp < producers; pp++ {
		prodWg.Add(1)
		go func(base int) {
			defer prodWg.Done()
			for i := 0; i < per; i++ {
				rb.Push(Item{
					ID:   uint64(base*per + i + 1),
					Data: base*per + i,
				})
			}
		}(pp)
	}
	prodWg.Wait()
	rb.Close()
	<-done

	if got := atomic.LoadInt64(&recvCount); got != int64(N) {
		t.Errorf("recvCount = %d, want %d", got, N)
	}
	st := rb.Stats()
	if st.Dropped != 0 {
		t.Errorf("Dropped = %d, want 0", st.Dropped)
	}
}

// TestBatcherConcurrent Batcher 并发压测。
func TestBatcherConcurrent(t *testing.T) {
	const N = 10000

	cfg := DefaultPipelineConfig()
	cfg.EnableBatch = true
	cfg.BatchConfig = BatchConfig{Size: 128, FlushInterval: 1e6} // 1ms
	cfg.IngestBuf = 4096
	cfg.ProcessBuf = 4096
	cfg.OutputBuf = 4096

	ingest := IdentityFunc()
	process := IdentityFunc()
	output := IdentityFunc()

	p := NewPipeline(cfg, ingest, process, output)
	outCh, err := p.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	var recvCount int64
	done := make(chan struct{})
	go func() {
		for range outCh {
			p.MarkOutput()
			atomic.AddInt64(&recvCount, 1)
		}
		close(done)
	}()

	for i := 0; i < N; i++ {
		p.Submit(Item{ID: uint64(i + 1), Data: i})
	}
	p.Close()
	<-done

	if got := atomic.LoadInt64(&recvCount); got != int64(N) {
		t.Errorf("recvCount = %d, want %d", got, N)
	}
}
