package main

import (
	"container/list"
	"os"
	"strconv"
	"sync"
)

type idemEntry struct {
	token string
	index int64
	err   error
	done  chan struct{}
	elem  *list.Element
}

type IdemTokenTable struct {
	mu       sync.Mutex
	entries  map[string]*idemEntry
	order    *list.List
	capacity int
}

func NewIdemTokenTable() *IdemTokenTable {
	cap := 10000
	if v := os.Getenv("IDEM_TOKEN_CAPACITY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cap = n
		}
	}
	return &IdemTokenTable{
		entries:  make(map[string]*idemEntry),
		order:    list.New(),
		capacity: cap,
	}
}

func (t *IdemTokenTable) GetOrCreate(token string) (*idemEntry, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if e, ok := t.entries[token]; ok {
		t.order.MoveToFront(e.elem)
		return e, true
	}

	e := &idemEntry{
		token: token,
		done:  make(chan struct{}),
	}
	e.elem = t.order.PushFront(e)
	t.entries[token] = e

	if t.order.Len() > t.capacity {
		oldest := t.order.Back()
		if oldest != nil {
			oe := oldest.Value.(*idemEntry)
			t.order.Remove(oldest)
			delete(t.entries, oe.token)
		}
	}

	return e, false
}

func (t *IdemTokenTable) SetResult(token string, index int64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if e, ok := t.entries[token]; ok {
		e.index = index
		e.err = err
		close(e.done)
	}
}

func (t *IdemTokenTable) Remove(token string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.entries[token]; ok {
		t.order.Remove(e.elem)
		delete(t.entries, token)
	}
}

func (t *IdemTokenTable) Size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.order.Len()
}
