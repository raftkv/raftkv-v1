package main

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"

	"sync"
	"sync/atomic"
	"time"
)

const licenseSalt = "raftkv_diag_v11_auth"

type DiagResult struct {
	Mode     string       `json:"mode"`
	Target   string       `json:"target"`
	Status   string       `json:"status"`
	License  string       `json:"license"`
	Metrics  *DiagMetrics `json:"metrics"`
	ErrorMsg string       `json:"error_msg,omitempty"`
}

type DiagMetrics struct {
	AvgLatencyMs   float64 `json:"avg_latency_ms"`
	P95LatencyMs   float64 `json:"p95_latency_ms"`
	P99LatencyMs   float64 `json:"p99_latency_ms"`
	ThroughputReqS float64 `json:"throughput_req_s"`
	FailRate       float64 `json:"fail_rate"`
	Concurrency    int     `json:"concurrency"`
	HealOk         bool    `json:"heal_ok,omitempty"`
	HealTimeMs     float64 `json:"heal_time_ms,omitempty"`
}

type diagKey struct {
	Fingerprint string `json:"fingerprint"`
	ExpiresAt   string `json:"expires_at"`
	Signature   string `json:"signature"`
}

func getMachineFingerprint() string {
	hostname, _ := os.Hostname()
	h := sha256.Sum256([]byte(hostname))
	return fmt.Sprintf("%x", h[:16])
}

func checkLicense() (bool, string) {
	exePath, err := os.Executable()
	if err != nil {
		return false, ""
	}
	keyPath := filepath.Join(filepath.Dir(exePath), "diag_tool.key")
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return false, ""
	}
	var key diagKey
	if err := json.Unmarshal(data, &key); err != nil {
		return false, ""
	}
	fp := getMachineFingerprint()
	if key.Fingerprint != fp {
		return false, ""
	}
	expiry, err := time.Parse(time.RFC3339, key.ExpiresAt)
	if err != nil || time.Now().After(expiry) {
		return false, ""
	}
	expected := fmt.Sprintf("%x", sha256.Sum256([]byte(fp+key.ExpiresAt+licenseSalt)))
	if key.Signature != expected {
		return false, ""
	}
	return true, key.ExpiresAt
}

func tcpPing(target string, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		return 0, err
	}
	elapsed := time.Since(start)
	conn.Close()
	return elapsed, nil
}

func runPing(target string, timeout time.Duration) *DiagResult {
	result := &DiagResult{Mode: "ping", Target: target}
	lat, err := tcpPing(target, timeout)
	if err != nil {
		result.Status = "FAIL"
		result.ErrorMsg = err.Error()
		return result
	}
	result.Status = "PASS"
	result.Metrics = &DiagMetrics{
		AvgLatencyMs: float64(lat.Microseconds()) / 1000.0,
	}
	return result
}

func runLoad(target string, concurrency int, timeout time.Duration) *DiagResult {
	result := &DiagResult{Mode: "load", Target: target}

	var successCount int64
	var failCount int64
	var latencies sync.Mutex
	var latList []float64

	startWall := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t0 := time.Now()
			conn, err := net.DialTimeout("tcp", target, timeout)
			elapsed := time.Since(t0)
			if err != nil {
				atomic.AddInt64(&failCount, 1)
				return
			}
			conn.Close()
			atomic.AddInt64(&successCount, 1)
			ms := float64(elapsed.Microseconds()) / 1000.0
			latencies.Lock()
			latList = append(latList, ms)
			latencies.Unlock()
		}()
	}
	wg.Wait()
	wallElapsed := time.Since(startWall).Seconds()

	total := float64(successCount + failCount)
	failRate := float64(0)
	if total > 0 {
		failRate = float64(failCount) / total
	}

	sort.Float64s(latList)

	var avg, p95, p99 float64
	if len(latList) > 0 {
		sum := float64(0)
		for _, v := range latList {
			sum += v
		}
		avg = sum / float64(len(latList))
		p95 = latList[len(latList)*95/100]
		p99 = latList[len(latList)*99/100]
	}

	throughput := float64(0)
	if wallElapsed > 0 {
		throughput = float64(successCount) / wallElapsed
	}

	result.Status = "PASS"
	if failRate > 0.5 {
		result.Status = "FAIL"
		result.ErrorMsg = fmt.Sprintf("fail_rate %.2f exceeds 0.5", failRate)
	}
	result.Metrics = &DiagMetrics{
		AvgLatencyMs:   avg,
		P95LatencyMs:   p95,
		P99LatencyMs:   p99,
		ThroughputReqS: throughput,
		FailRate:       failRate,
		Concurrency:    concurrency,
	}
	return result
}

func runChaos(target string, timeout time.Duration) *DiagResult {
	result := &DiagResult{Mode: "chaos", Target: target}

	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		result.Status = "FAIL"
		result.ErrorMsg = fmt.Sprintf("initial connect failed: %v", err)
		return result
	}
	conn.Close()

	healStart := time.Now()
	var healed bool
	deadline := time.Now().Add(timeout * 3)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", target, timeout)
		if err == nil {
			c.Close()
			healed = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	healMs := float64(time.Since(healStart).Milliseconds())

	if healed {
		result.Status = "PASS"
		result.Metrics = &DiagMetrics{
			HealOk:     true,
			HealTimeMs: healMs,
		}
	} else {
		result.Status = "FAIL"
		result.ErrorMsg = "target did not self-heal within timeout"
		result.Metrics = &DiagMetrics{
			HealOk:     false,
			HealTimeMs: healMs,
		}
	}
	return result
}

func runFull(target string, concurrency int, timeout time.Duration) *DiagResult {
	result := &DiagResult{Mode: "full", Target: target}

	pingRes := runPing(target, timeout)
	loadRes := runLoad(target, concurrency, timeout)
	chaosRes := runChaos(target, timeout)

	overallStatus := "PASS"
	errMsg := ""
	if pingRes.Status == "FAIL" {
		overallStatus = "FAIL"
		errMsg = "ping: " + pingRes.ErrorMsg
	}
	if loadRes.Status == "FAIL" {
		overallStatus = "FAIL"
		if errMsg != "" {
			errMsg += "; "
		}
		errMsg += "load: " + loadRes.ErrorMsg
	}
	if chaosRes.Status == "FAIL" {
		overallStatus = "FAIL"
		if errMsg != "" {
			errMsg += "; "
		}
		errMsg += "chaos: " + chaosRes.ErrorMsg
	}

	result.Status = overallStatus
	result.ErrorMsg = errMsg
	result.Metrics = &DiagMetrics{
		AvgLatencyMs:   loadRes.Metrics.AvgLatencyMs,
		P95LatencyMs:   loadRes.Metrics.P95LatencyMs,
		P99LatencyMs:   loadRes.Metrics.P99LatencyMs,
		ThroughputReqS: loadRes.Metrics.ThroughputReqS,
		FailRate:       loadRes.Metrics.FailRate,
		Concurrency:    loadRes.Metrics.Concurrency,
		HealOk:         chaosRes.Metrics.HealOk,
		HealTimeMs:     chaosRes.Metrics.HealTimeMs,
	}
	return result
}

func generatePDFReport(result *DiagResult, filename string) error {
	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("cannot locate executable: %v", err)
	}
	scriptPath := filepath.Join(filepath.Dir(exePath), "format_diag_report.py")

	jsonData, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("json marshal failed: %v", err)
	}

	tmpJson := filepath.Join(os.TempDir(), "diag_result_tmp.json")
	if err := os.WriteFile(tmpJson, jsonData, 0644); err != nil {
		return fmt.Errorf("write temp json failed: %v", err)
	}
	defer os.Remove(tmpJson)

	cmd := exec.Command("python", scriptPath, tmpJson, filename)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("python script failed: %v, output: %s", err, string(output))
	}
	return nil
}

func main() {
	target := flag.String("target", "", "Target address (e.g. 192.168.1.10:9501)")
	mode := flag.String("mode", "full", "Diag mode: ping|load|chaos|full")
	concurrency := flag.Int("concurrency", 50, "Concurrency for load test")
	timeoutStr := flag.String("timeout", "5s", "Per-operation timeout")
	exportReport := flag.Bool("export-report", false, "Export PDF report (requires license, default filename: diag_report.pdf)")
	flag.Parse()

	licensed, _ := checkLicense()
	licenseTag := "TRIAL"
	if licensed {
		licenseTag = "OFFICIAL"
	}

	if *target == "" {
		res := DiagResult{Mode: *mode, Target: "", Status: "FAIL", License: licenseTag, ErrorMsg: "--target is required"}
		out, _ := json.Marshal(res)
		fmt.Println(string(out))
		return
	}

	timeout, err := time.ParseDuration(*timeoutStr)
	if err != nil {
		timeout = 5 * time.Second
	}

	var result *DiagResult
	switch *mode {
	case "ping":
		result = runPing(*target, timeout)
	case "load":
		result = runLoad(*target, *concurrency, timeout)
	case "chaos":
		result = runChaos(*target, timeout)
	case "full":
		result = runFull(*target, *concurrency, timeout)
	default:
		result = &DiagResult{Mode: *mode, Target: *target, Status: "FAIL", License: licenseTag, ErrorMsg: "unknown mode: " + *mode}
	}

	result.License = licenseTag

	if *exportReport {
		if !licensed {
			result.ErrorMsg = "PDF report export requires authorized diag_tool.key"
			out, _ := json.MarshalIndent(result, "", "  ")
			fmt.Println(string(out))
			return
		}
		if err := generatePDFReport(result, "diag_report.pdf"); err != nil {
			result.ErrorMsg = fmt.Sprintf("report generation failed: %v", err)
		}
	}

	out, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(out))
}
