package main

import (
	"sync/atomic"
)

type InFlightLimiter struct {
	cap       int64
	inFlight  atomic.Int64
	shedCount atomic.Int64
}

func NewInFlightLimiter(cap int64) *InFlightLimiter {
	if cap <= 0 {
		cap = 600
	}
	return &InFlightLimiter{cap: cap}
}

func (l *InFlightLimiter) TryAcquire() bool {
	current := l.inFlight.Load()
	if current >= l.cap {
		l.shedCount.Add(1)
		return false
	}
	newVal := l.inFlight.Add(1)
	if newVal > l.cap {
		l.inFlight.Add(-1)
		l.shedCount.Add(1)
		return false
	}
	return true
}

func (l *InFlightLimiter) Release() {
	l.inFlight.Add(-1)
}

func (l *InFlightLimiter) InFlight() int64 {
	return l.inFlight.Load()
}

func (l *InFlightLimiter) Cap() int64 {
	return l.cap
}

func (l *InFlightLimiter) ShedCount() int64 {
	return l.shedCount.Load()
}

func (l *InFlightLimiter) Utilization() float64 {
	return float64(l.inFlight.Load()) / float64(l.cap)
}
