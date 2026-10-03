package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

type CompactWAL struct {
	mu                sync.Mutex
	file              *os.File
	filePath          string
	lastTerm          int64
	lastIndex         int64
	termRun           int64
	totalEntries      int64
	compressedEntries int64
}

func NewCompactWAL(path string) (*CompactWAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open compact wal: %w", err)
	}
	return &CompactWAL{
		file:     f,
		filePath: path,
	}, nil
}

func (cw *CompactWAL) Append(term, index int64, data []byte) error {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	var buf []byte
	if cw.lastTerm == term && cw.lastIndex > 0 && index == cw.lastIndex+1 {
		buf = make([]byte, 0, 4+len(data))
		buf = append(buf, 0x00)
		var idxBuf [8]byte
		binary.BigEndian.PutUint64(idxBuf[:], uint64(index))
		buf = append(buf, idxBuf[:]...)
		buf = append(buf, data...)
		cw.compressedEntries++
	} else {
		buf = make([]byte, 0, 17+len(data))
		buf = append(buf, 0x01)
		var termBuf [8]byte
		binary.BigEndian.PutUint64(termBuf[:], uint64(term))
		buf = append(buf, termBuf[:]...)
		var idxBuf [8]byte
		binary.BigEndian.PutUint64(idxBuf[:], uint64(index))
		buf = append(buf, idxBuf[:]...)
		buf = append(buf, data...)
	}

	_, err := cw.file.Write(buf)
	if err != nil {
		return err
	}

	cw.lastTerm = term
	cw.lastIndex = index
	cw.totalEntries++
	return nil
}

func (cw *CompactWAL) Sync() error {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.file.Sync()
}

func (cw *CompactWAL) Stats() string {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	ratio := float64(0)
	if cw.totalEntries > 0 {
		ratio = float64(cw.compressedEntries) / float64(cw.totalEntries) * 100
	}
	return fmt.Sprintf("CompactWAL: entries=%d compressed=%d ratio=%.1f%%",
		cw.totalEntries, cw.compressedEntries, ratio)
}

func (cw *CompactWAL) Close() error {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.file.Close()
}
