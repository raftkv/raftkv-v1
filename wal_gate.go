package main

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type WALGate struct {
	mu             sync.RWMutex
	closed         bool
	recovering     bool
	consecutiveBad int32
	threshold      int32
	audit          *WALGateAuditLog
	node           *RaftNode
	probeInterval  time.Duration
	stopCh         chan struct{}
	reason         string
}

func NewWALGate(node *RaftNode, audit *WALGateAuditLog) *WALGate {
	threshold := 3
	probeInterval := 1 * time.Second

	if v := osGetenvInt("WAL_GATE_THRESHOLD"); v > 0 {
		threshold = v
	}
	if v := osGetenvDuration("WAL_GATE_PROBE_INTERVAL"); v > 0 {
		probeInterval = v
	}

	return &WALGate{
		threshold:     int32(threshold),
		audit:         audit,
		node:          node,
		probeInterval: probeInterval,
		stopCh:        make(chan struct{}),
	}
}

func (g *WALGate) Start(wal *WAL) {
	go g.probeLoop(wal)
}

func (g *WALGate) StartWithCheck(checkFn func() error) {
	go g.probeLoopFn(checkFn)
}

func (g *WALGate) probeLoopFn(checkFn func() error) {
	ticker := time.NewTicker(g.probeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			if err := checkFn(); err != nil {
				g.ReportUnavailable(err.Error())
			} else {
				g.ReportAvailable()
			}
		}
	}
}

func (g *WALGate) probeLoop(wal *WAL) {
	ticker := time.NewTicker(g.probeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			if err := wal.HealthCheck(); err != nil {
				g.ReportUnavailable(err.Error())
			} else {
				g.ReportAvailable()
			}
		}
	}
}

func (g *WALGate) IsClosed() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.closed
}

func (g *WALGate) IsRecovering() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.recovering
}

func (g *WALGate) ReportUnavailable(reason string) {
	g.mu.Lock()
	count := atomic.AddInt32(&g.consecutiveBad, 1)
	shouldClose := count >= g.threshold && !g.closed
	if shouldClose {
		g.closed = true
		g.reason = reason
	}
	g.mu.Unlock()

	if shouldClose && g.node != nil {
		g.node.StepDownForWALFailure(reason)
		if g.audit != nil {
			g.audit.Record(WALGateAuditRecord{
				TriggerTime:  time.Now().Unix(),
				Reason:       reason,
				PreviousRole: g.node.State().String(),
			})
		}
	}
}

func (g *WALGate) ReportAvailable() {
	atomic.StoreInt32(&g.consecutiveBad, 0)

	g.mu.Lock()
	if g.closed && !g.recovering {
		g.recovering = true
	}
	g.mu.Unlock()
}

func (g *WALGate) ReportRecovered() {
	g.mu.Lock()
	g.closed = false
	g.recovering = false
	g.reason = ""
	g.mu.Unlock()
	atomic.StoreInt32(&g.consecutiveBad, 0)

	if g.audit != nil {
		g.audit.Record(WALGateAuditRecord{
			RecoverTime: time.Now().Unix(),
			Reason:      "WAL恢复",
		})
	}
}

func (g *WALGate) Reason() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.reason
}

func (g *WALGate) Stop() {
	close(g.stopCh)
}

func osGetenvInt(key string) int {
	v := os.Getenv(key)
	if v == "" {
		return 0
	}
	var n int
	fmt.Sscanf(v, "%d", &n)
	return n
}

func osGetenvDuration(key string) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return 0
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0
	}
	return d
}
