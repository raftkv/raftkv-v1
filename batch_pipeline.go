package main

import (
	"sync"
	"time"
)

type BatchEntry struct {
	Key   string
	Value []byte
	Index int64
	Term  int64
}

type BatchPipeline struct {
	mu           sync.Mutex
	buffer       []BatchEntry
	maxBatch     int
	maxWait      time.Duration
	flushChan    chan struct{}
	onFlush      func([]BatchEntry)
	totalFlush   int64
	totalEntries int64
	stopCh       chan struct{}
}

func NewBatchPipeline(maxBatch int, maxWaitMs int, onFlush func([]BatchEntry)) *BatchPipeline {
	bp := &BatchPipeline{
		buffer:    make([]BatchEntry, 0, maxBatch),
		maxBatch:  maxBatch,
		maxWait:   time.Duration(maxWaitMs) * time.Millisecond,
		flushChan: make(chan struct{}, 1),
		onFlush:   onFlush,
		stopCh:    make(chan struct{}),
	}
	go bp.flushLoop()
	return bp
}

func (bp *BatchPipeline) Submit(entry BatchEntry) {
	bp.mu.Lock()
	bp.buffer = append(bp.buffer, entry)
	shouldFlush := len(bp.buffer) >= bp.maxBatch
	bp.mu.Unlock()
	if shouldFlush {
		select {
		case bp.flushChan <- struct{}{}:
		default:
		}
	}
}

func (bp *BatchPipeline) flushLoop() {
	ticker := time.NewTicker(bp.maxWait)
	defer ticker.Stop()
	for {
		select {
		case <-bp.stopCh:
			bp.doFlush()
			return
		case <-bp.flushChan:
			bp.doFlush()
		case <-ticker.C:
			bp.doFlush()
		}
	}
}

func (bp *BatchPipeline) doFlush() {
	bp.mu.Lock()
	if len(bp.buffer) == 0 {
		bp.mu.Unlock()
		return
	}
	batch := bp.buffer
	bp.buffer = make([]BatchEntry, 0, bp.maxBatch)
	bp.mu.Unlock()
	bp.totalFlush++
	bp.totalEntries += int64(len(batch))
	if bp.onFlush != nil {
		bp.onFlush(batch)
	}
}

func (bp *BatchPipeline) Stats() (int64, int64) {
	return bp.totalFlush, bp.totalEntries
}

func (bp *BatchPipeline) Shutdown() {
	close(bp.stopCh)
}
