package main

import (
	"sync/atomic"
	"time"
)

type TokenBucketLimiter struct {
	tokens        atomic.Int64
	maxTokens     int64
	refillRate    atomic.Int64
	lastRefill    atomic.Int64
	rejectedCount atomic.Int64
	totalCount    atomic.Int64
	enabled       atomic.Int32
	avgLatencyUs  atomic.Int64
	latencyWindow atomic.Int64
	adjustCounter atomic.Int64
}

func NewTokenBucketLimiter(maxTokens int64, initialRate int64) *TokenBucketLimiter {
	l := &TokenBucketLimiter{}
	l.tokens.Store(maxTokens)
	l.maxTokens = maxTokens
	l.refillRate.Store(initialRate)
	l.lastRefill.Store(time.Now().UnixNano())
	l.enabled.Store(0)
	return l
}

func (l *TokenBucketLimiter) Enable()  { l.enabled.Store(1) }
func (l *TokenBucketLimiter) Disable() { l.enabled.Store(0) }

func (l *TokenBucketLimiter) Allow() bool {
	l.totalCount.Add(1)
	if l.enabled.Load() == 0 {
		return true
	}
	now := time.Now().UnixNano()
	last := l.lastRefill.Load()
	elapsed := now - last
	if elapsed > 0 {
		refill := (elapsed * l.refillRate.Load()) / int64(time.Second)
		if refill > 0 && l.lastRefill.CompareAndSwap(last, now) {
			cur := l.tokens.Add(refill)
			if cur > l.maxTokens {
				l.tokens.Store(l.maxTokens)
			}
		}
	}
	if l.tokens.Load() <= 0 {
		l.rejectedCount.Add(1)
		return false
	}
	if l.tokens.Add(-1) >= 0 {
		return true
	}
	l.rejectedCount.Add(1)
	return false
}

func (l *TokenBucketLimiter) RecordLatency(latencyUs int64) {
	window := l.latencyWindow.Add(1)
	old := l.avgLatencyUs.Load()
	l.avgLatencyUs.Store((old*(window-1) + latencyUs) / window)
	if l.enabled.Load() == 0 {
		return
	}
	if l.adjustCounter.Add(1) < 200 {
		return
	}
	l.adjustCounter.Store(0)
	avg := l.avgLatencyUs.Load()
	if avg > 100000 {
		newRate := l.refillRate.Load() * 90 / 100
		if newRate > 100 {
			l.refillRate.Store(newRate)
		}
	} else if avg < 20000 {
		newRate := l.refillRate.Load() * 110 / 100
		maxRate := l.maxTokens * 20
		if newRate < maxRate {
			l.refillRate.Store(newRate)
		}
	}
}

func (l *TokenBucketLimiter) Stats() (rejected, total, rate int64, enabled bool) {
	return l.rejectedCount.Load(), l.totalCount.Load(), l.refillRate.Load(), l.enabled.Load() == 1
}
