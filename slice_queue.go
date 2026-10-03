package main

import (
	"sync"
)

type SliceQueue struct {
	mu          sync.Mutex
	buf         [][]byte
	head        int
	tail        int
	count       int
	cap         int
	totalPushed int64
	totalPopped int64
}

func NewSliceQueue(capacity int) *SliceQueue {
	return &SliceQueue{
		buf: make([][]byte, capacity),
		cap: capacity,
	}
}

func (sq *SliceQueue) Push(data []byte) bool {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	if sq.count >= sq.cap {
		newCap := sq.cap * 2
		newBuf := make([][]byte, newCap)
		for i := 0; i < sq.count; i++ {
			newBuf[i] = sq.buf[(sq.head+i)%sq.cap]
		}
		sq.buf = newBuf
		sq.head = 0
		sq.tail = sq.count
		sq.cap = newCap
	}
	sq.buf[sq.tail] = data
	sq.tail = (sq.tail + 1) % sq.cap
	sq.count++
	sq.totalPushed++
	return true
}

func (sq *SliceQueue) Pop() ([]byte, bool) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	if sq.count == 0 {
		return nil, false
	}
	data := sq.buf[sq.head]
	sq.buf[sq.head] = nil
	sq.head = (sq.head + 1) % sq.cap
	sq.count--
	sq.totalPopped++
	return data, true
}

func (sq *SliceQueue) PopBatch(max int) [][]byte {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	n := sq.count
	if n > max {
		n = max
	}
	result := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		result = append(result, sq.buf[sq.head])
		sq.buf[sq.head] = nil
		sq.head = (sq.head + 1) % sq.cap
		sq.totalPopped++
	}
	sq.count -= n
	return result
}

func (sq *SliceQueue) Len() int {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	return sq.count
}

func (sq *SliceQueue) Stats() (int64, int64, int) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	return sq.totalPushed, sq.totalPopped, sq.count
}

type DoubleBufferQueue struct {
	front *SliceQueue
	back  *SliceQueue
	mu    sync.Mutex
}

func NewDoubleBufferQueue(capacity int) *DoubleBufferQueue {
	return &DoubleBufferQueue{
		front: NewSliceQueue(capacity),
		back:  NewSliceQueue(capacity),
	}
}

func (dbq *DoubleBufferQueue) Push(data []byte) bool {
	return dbq.front.Push(data)
}

func (dbq *DoubleBufferQueue) Swap() [][]byte {
	dbq.mu.Lock()
	defer dbq.mu.Unlock()
	tmp := dbq.front
	dbq.front = dbq.back
	dbq.back = tmp
	return dbq.back.PopBatch(dbq.back.cap)
}
