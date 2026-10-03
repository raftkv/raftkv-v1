package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type RateLimiter struct {
	mu             sync.Mutex
	maxMemoryMB    int64
	currentUsageMB int64
	allowedRate    int64
	blockedCount   int64
	totalRequests  int64
	lastCheck      time.Time
	checkInterval  time.Duration
	highWaterMark  int64
	lowWaterMark   int64
	throttled      int32
}

func NewRateLimiter(maxMemMB int64) *RateLimiter {
	rl := &RateLimiter{
		maxMemoryMB:   maxMemMB,
		allowedRate:   100000,
		checkInterval: 100 * time.Millisecond,
		highWaterMark: maxMemMB * 80 / 100,
		lowWaterMark:  maxMemMB * 60 / 100,
		lastCheck:     time.Now(),
	}
	go rl.monitorLoop()
	return rl
}

func (rl *RateLimiter) monitorLoop() {
	ticker := time.NewTicker(rl.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rl.updateMemoryUsage()
		}
	}
}

func (rl *RateLimiter) updateMemoryUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	usedMB := int64(m.Alloc) / (1024 * 1024)

	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.currentUsageMB = usedMB

	if usedMB > rl.highWaterMark {
		atomic.StoreInt32(&rl.throttled, 1)
		rl.allowedRate = rl.allowedRate * 70 / 100
		if rl.allowedRate < 1000 {
			rl.allowedRate = 1000
		}
	} else if usedMB < rl.lowWaterMark {
		atomic.StoreInt32(&rl.throttled, 0)
		rl.allowedRate = rl.allowedRate * 110 / 100
		if rl.allowedRate > 100000 {
			rl.allowedRate = 100000
		}
	}
}

func (rl *RateLimiter) Allow() bool {
	atomic.AddInt64(&rl.totalRequests, 1)
	if atomic.LoadInt32(&rl.throttled) == 1 {
		rl.mu.Lock()
		rl.blockedCount++
		rl.mu.Unlock()
		return false
	}
	return true
}

func (rl *RateLimiter) Stats() string {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	throttled := "OFF"
	if atomic.LoadInt32(&rl.throttled) == 1 {
		throttled = "ON"
	}
	return fmt.Sprintf("RateLimiter: mem=%dMB/%dMB rate=%d throttled=%s blocked=%d/%d",
		rl.currentUsageMB, rl.maxMemoryMB, rl.allowedRate, throttled, rl.blockedCount, rl.totalRequests)
}

func (rl *RateLimiter) IsThrottled() bool {
	return atomic.LoadInt32(&rl.throttled) == 1
}

func (rl *RateLimiter) CurrentRate() int64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.allowedRate
}
