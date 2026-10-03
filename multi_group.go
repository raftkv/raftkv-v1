package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type RaftGroup struct {
	ID       int
	Node     *RaftNode
	Active   bool
	Load     int64
	lastSync time.Time
}

type MultiGroupManager struct {
	mu        sync.RWMutex
	groups    map[int]*RaftGroup
	count     int
	rrIdx     int32
	totalLoad int64
}

func NewMultiGroupManager(groupCount int) *MultiGroupManager {
	return &MultiGroupManager{
		groups: make(map[int]*RaftGroup),
		count:  groupCount,
	}
}

func (mg *MultiGroupManager) RegisterGroup(id int, node *RaftNode) {
	mg.mu.Lock()
	defer mg.mu.Unlock()
	mg.groups[id] = &RaftGroup{
		ID:       id,
		Node:     node,
		Active:   true,
		lastSync: time.Now(),
	}
}

func (mg *MultiGroupManager) GetGroup(key string) *RaftGroup {
	mg.mu.RLock()
	defer mg.mu.RUnlock()
	if len(mg.groups) == 0 {
		return nil
	}
	hash := 0
	for _, c := range key {
		hash = (hash*31 + int(c)) % len(mg.groups)
	}
	if g, ok := mg.groups[hash]; ok && g.Active {
		return g
	}
	next := int(atomic.AddInt32(&mg.rrIdx, 1))
	return mg.groups[next%len(mg.groups)]
}

func (mg *MultiGroupManager) UpdateLoad(id int, load int64) {
	mg.mu.Lock()
	defer mg.mu.Unlock()
	if g, ok := mg.groups[id]; ok {
		g.Load = load
		mg.totalLoad = 0
		for _, g := range mg.groups {
			mg.totalLoad += g.Load
		}
	}
}

func (mg *MultiGroupManager) Stats() string {
	mg.mu.RLock()
	defer mg.mu.RUnlock()
	result := fmt.Sprintf("MultiGroup: count=%d totalLoad=%d\n", len(mg.groups), mg.totalLoad)
	for id, g := range mg.groups {
		result += fmt.Sprintf("  group-%d: active=%v load=%d\n", id, g.Active, g.Load)
	}
	return result
}

func (mg *MultiGroupManager) Rebalance() {
	mg.mu.Lock()
	defer mg.mu.Unlock()
	if len(mg.groups) < 2 {
		return
	}
	avgLoad := mg.totalLoad / int64(len(mg.groups))
	for _, g := range mg.groups {
		if g.Load > avgLoad*2 {
			g.lastSync = time.Now()
		}
	}
}
