package main

import (
	"fmt"
	"sync/atomic"
)

type TriStats struct {
	success atomic.Int64
	shed    atomic.Int64
	fail    atomic.Int64
}

func NewTriStats() *TriStats {
	return &TriStats{}
}

func (t *TriStats) IncSuccess() {
	t.success.Add(1)
}

func (t *TriStats) IncShed() {
	t.shed.Add(1)
}

func (t *TriStats) IncFail() {
	t.fail.Add(1)
}

type TriStatsSnapshot struct {
	Success     int64   `json:"success"`
	Shed        int64   `json:"shed"`
	Fail        int64   `json:"fail"`
	Total       int64   `json:"total"`
	SuccessRate float64 `json:"success_rate"`
	ShedRate    float64 `json:"shed_rate"`
	FailRate    float64 `json:"fail_rate"`
}

func (t *TriStats) Snapshot() TriStatsSnapshot {
	s := t.success.Load()
	h := t.shed.Load()
	f := t.fail.Load()
	total := s + h + f
	var sr, hr, fr float64
	if total > 0 {
		sr = float64(s) / float64(total)
		hr = float64(h) / float64(total)
		fr = float64(f) / float64(total)
	}
	return TriStatsSnapshot{
		Success:     s,
		Shed:        h,
		Fail:        f,
		Total:       total,
		SuccessRate: sr,
		ShedRate:    hr,
		FailRate:    fr,
	}
}

func (t *TriStats) RenderPrometheus() string {
	s := t.success.Load()
	h := t.shed.Load()
	f := t.fail.Load()
	total := s + h + f
	var sr float64
	if total > 0 {
		sr = float64(s) / float64(total)
	}
	return fmt.Sprintf(`# HELP raft_success_total Total successful requests
# TYPE raft_success_total counter
raft_success_total %d
# HELP raft_shed_total Total shedded requests (in-flight cap or rate limited)
# TYPE raft_shed_total counter
raft_shed_total %d
# HELP raft_fail_total Total failed requests (true failures, not shed)
# TYPE raft_fail_total counter
raft_fail_total %d
# HELP raft_success_rate Current success rate (success / total)
# TYPE raft_success_rate gauge
raft_success_rate %.6f
`, s, h, f, sr)
}
