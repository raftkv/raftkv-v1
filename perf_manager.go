package main

import (
	"fmt"
	"os"
	"strings"
)

type V24PerfManager struct {
	multiGroup *MultiGroupManager
	batchPipe  *BatchPipeline
	compactWAL *CompactWAL
	dbQueue    *DoubleBufferQueue
	rateLimit  *RateLimiter
	enabled    bool
}

func NewV24PerfManager() *V24PerfManager {
	pm := &V24PerfManager{enabled: false}

	if os.Getenv("V24_ENABLE") != "true" {
		return pm
	}
	pm.enabled = true

	groupCount := 4
	pm.multiGroup = NewMultiGroupManager(groupCount)

	pm.batchPipe = NewBatchPipeline(1000, 10, func(batch []BatchEntry) {
	})

	walPath := os.Getenv("V24_WAL_PATH")
	if walPath == "" {
		walPath = "/tmp/raftkv_v24_compact.wal"
	}
	cw, err := NewCompactWAL(walPath)
	if err != nil {
		fmt.Printf("[V2.4] CompactWAL初始化失败: %v\n", err)
	} else {
		pm.compactWAL = cw
	}

	pm.dbQueue = NewDoubleBufferQueue(4096)

	maxMem := int64(512)
	pm.rateLimit = NewRateLimiter(maxMem)

	fmt.Println("[V2.4] 性能优化管理器已启用:")
	fmt.Printf("  MultiGroup: %d组\n", groupCount)
	fmt.Println("  BatchPipeline: 批量=1000 等待=10ms")
	fmt.Println("  CompactWAL: 连续Term/Index压缩")
	fmt.Println("  DoubleBufferQueue: 容量=4096")
	fmt.Printf("  RateLimiter: 内存上限=%dMB\n", maxMem)

	return pm
}

func (pm *V24PerfManager) Stats() string {
	if !pm.enabled {
		return "V2.4性能优化: 未启用"
	}
	var sb strings.Builder
	sb.WriteString("=== V2.4 性能优化统计 ===\n")
	if pm.multiGroup != nil {
		sb.WriteString(pm.multiGroup.Stats())
	}
	if pm.batchPipe != nil {
		flush, entries := pm.batchPipe.Stats()
		sb.WriteString(fmt.Sprintf("\nBatchPipeline: flush=%d entries=%d", flush, entries))
	}
	if pm.compactWAL != nil {
		sb.WriteString("\n" + pm.compactWAL.Stats())
	}
	if pm.dbQueue != nil {
		pushed, popped, pending := pm.dbQueue.front.Stats()
		sb.WriteString(fmt.Sprintf("\nDBQueue: pushed=%d popped=%d pending=%d", pushed, popped, pending))
	}
	if pm.rateLimit != nil {
		sb.WriteString("\n" + pm.rateLimit.Stats())
	}
	return sb.String()
}

func (pm *V24PerfManager) Shutdown() {
	if !pm.enabled {
		return
	}
	if pm.batchPipe != nil {
		pm.batchPipe.Shutdown()
	}
	if pm.compactWAL != nil {
		pm.compactWAL.Close()
	}
	fmt.Println("[V2.4] 性能优化管理器已关闭")
}
