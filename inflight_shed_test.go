package main

import (
	"testing"
)

func TestBatch17_InFlightLimiter_BasicAcquireRelease(t *testing.T) {
	l := NewInFlightLimiter(3)
	if l.InFlight() != 0 {
		t.Fatal("初始在途数应为0")
	}
	if !l.TryAcquire() {
		t.Fatal("cap=3 时首次 Acquire 应成功")
	}
	if !l.TryAcquire() {
		t.Fatal("cap=3 时第二次 Acquire 应成功")
	}
	if !l.TryAcquire() {
		t.Fatal("cap=3 时第三次 Acquire 应成功")
	}
	if l.TryAcquire() {
		t.Fatal("cap=3 时第四次 Acquire 应失败")
	}
	if l.InFlight() != 3 {
		t.Fatalf("在途数应为3, 实际=%d", l.InFlight())
	}
	l.Release()
	if l.InFlight() != 2 {
		t.Fatalf("Release 后在途数应为2, 实际=%d", l.InFlight())
	}
	if !l.TryAcquire() {
		t.Fatal("Release 后 Acquire 应成功")
	}
}

func TestBatch17_InFlightLimiter_ShedCount(t *testing.T) {
	l := NewInFlightLimiter(2)
	l.TryAcquire()
	l.TryAcquire()
	l.TryAcquire()
	l.TryAcquire()
	if l.ShedCount() != 2 {
		t.Fatalf("shed 计数应为2, 实际=%d", l.ShedCount())
	}
}

func TestBatch17_InFlightLimiter_Utilization(t *testing.T) {
	l := NewInFlightLimiter(100)
	l.TryAcquire()
	l.TryAcquire()
	u := l.Utilization()
	if u < 0.019 || u > 0.021 {
		t.Fatalf("利用率应≈0.02, 实际=%.4f", u)
	}
}

func TestBatch17_InFlightLimiter_ConcurrentSafe(t *testing.T) {
	l := NewInFlightLimiter(100)
	done := make(chan bool, 200)
	for i := 0; i < 200; i++ {
		go func() {
			if l.TryAcquire() {
				l.Release()
			}
			done <- true
		}()
	}
	for i := 0; i < 200; i++ {
		<-done
	}
	if l.InFlight() != 0 {
		t.Fatalf("全部释放后在途数应为0, 实际=%d", l.InFlight())
	}
}

func TestBatch17_TriStats_Independence(t *testing.T) {
	ts := NewTriStats()
	ts.IncSuccess()
	ts.IncSuccess()
	ts.IncShed()
	ts.IncShed()
	ts.IncShed()
	ts.IncFail()
	snap := ts.Snapshot()
	if snap.Success != 2 {
		t.Fatalf("success 应为2, 实际=%d", snap.Success)
	}
	if snap.Shed != 3 {
		t.Fatalf("shed 应为3, 实际=%d", snap.Shed)
	}
	if snap.Fail != 1 {
		t.Fatalf("fail 应为1, 实际=%d", snap.Fail)
	}
	if snap.Total != 6 {
		t.Fatalf("total 应为6, 实际=%d", snap.Total)
	}
}

func TestBatch17_TriStats_PrometheusFormat(t *testing.T) {
	ts := NewTriStats()
	ts.IncSuccess()
	ts.IncShed()
	out := ts.RenderPrometheus()
	if !stringContains(out, "raft_success_total") {
		t.Error("应输出 raft_success_total")
	}
	if !stringContains(out, "raft_shed_total") {
		t.Error("应输出 raft_shed_total")
	}
	if !stringContains(out, "raft_fail_total") {
		t.Error("应输出 raft_fail_total")
	}
	if !stringContains(out, "# HELP") {
		t.Error("应含 # HELP 行")
	}
	if !stringContains(out, "# TYPE") {
		t.Error("应含 # TYPE 行")
	}
}

func TestBatch17_TriStats_ShedNotInFail(t *testing.T) {
	ts := NewTriStats()
	for i := 0; i < 100; i++ {
		ts.IncSuccess()
	}
	for i := 0; i < 30; i++ {
		ts.IncShed()
	}
	for i := 0; i < 5; i++ {
		ts.IncFail()
	}
	snap := ts.Snapshot()
	if snap.Fail != 5 {
		t.Fatalf("fail 应仅含真失败=5, 实际=%d", snap.Fail)
	}
	if snap.Shed != 30 {
		t.Fatalf("shed 应独立统计=30, 实际=%d", snap.Shed)
	}
	if snap.Success != 100 {
		t.Fatalf("success 应=100, 实际=%d", snap.Success)
	}
}

func TestBatch17_Gate_DoubleLayerShed(t *testing.T) {
	ifl := NewInFlightLimiter(2)
	ts := NewTriStats()
	ifl.TryAcquire()
	ifl.TryAcquire()
	if ifl.TryAcquire() {
		t.Fatal("cap=2 时第三次应被拒载")
	}
	ts.IncShed()
	if ifl.ShedCount() != 1 {
		t.Fatalf("in-flight shed 计数应为1, 实际=%d", ifl.ShedCount())
	}
	if ts.Snapshot().Shed != 1 {
		t.Fatalf("triStats shed 应为1, 实际=%d", ts.Snapshot().Shed)
	}
	ifl.Release()
	if !ifl.TryAcquire() {
		t.Fatal("Release 后应可再次 Acquire")
	}
}
