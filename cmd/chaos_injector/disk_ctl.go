package main

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

type DiskController struct{}

func NewDiskController() *DiskController {
	return &DiskController{}
}

const walPath = "/app/wal-data"
const fillfilePath = walPath + "/fillfile"

func (dc *DiskController) InjectDiskFull(container string, pressureLevel string) error {
	usage, err := dc.DetectDiskUsage(container)
	if err != nil {
		return fmt.Errorf("detect disk usage: %v", err)
	}

	var targetPercent int
	switch pressureLevel {
	case "soft":
		targetPercent = 90
	case "hard":
		targetPercent = 99
	default:
		targetPercent = 90
	}

	if usage >= targetPercent {
		return nil
	}

	sizeMB, err := dc.calculateFillSizeMB(container, targetPercent)
	if err != nil {
		return fmt.Errorf("calculate fill size: %v", err)
	}

	cmd := exec.Command("docker", "exec", container, "fallocate", "-l", fmt.Sprintf("%dM", sizeMB), fillfilePath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("fallocate failed: %v, output: %s", err, string(out))
	}

	afterUsage, _ := dc.DetectDiskUsage(container)
	if afterUsage < targetPercent-5 {
		return fmt.Errorf("disk usage after inject: %d%% < target %d%%", afterUsage, targetPercent)
	}

	return nil
}

func (dc *DiskController) calculateFillSizeMB(container string, targetPercent int) (int, error) {
	cmd := exec.Command("docker", "exec", container, "df", "-m", walPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("df -m failed: %v", err)
	}

	lines := strings.Split(string(out), "\n")
	if len(lines) < 2 {
		return 0, fmt.Errorf("df output too short")
	}

	fields := strings.Fields(lines[1])
	if len(fields) < 4 {
		return 0, fmt.Errorf("df fields insufficient: %v", fields)
	}

	totalMB, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, fmt.Errorf("parse total: %v", err)
	}
	usedMB, err := strconv.Atoi(fields[2])
	if err != nil {
		return 0, fmt.Errorf("parse used: %v", err)
	}

	targetUsedMB := totalMB * targetPercent / 100
	fillMB := targetUsedMB - usedMB
	if fillMB <= 0 {
		return 0, fmt.Errorf("already at or above target")
	}
	return fillMB, nil
}

func (dc *DiskController) DetectDiskUsage(container string) (int, error) {
	cmd := exec.Command("docker", "exec", container, "df", "-h", walPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("df failed: %v", err)
	}

	lines := strings.Split(string(out), "\n")
	if len(lines) < 2 {
		return 0, fmt.Errorf("df output too short")
	}

	fields := strings.Fields(lines[1])
	if len(fields) < 5 {
		return 0, fmt.Errorf("df fields insufficient: %v", fields)
	}

	useStr := fields[4]
	useStr = strings.TrimSuffix(useStr, "%")
	usage, err := strconv.Atoi(useStr)
	if err != nil {
		return 0, fmt.Errorf("parse use%%: %v", err)
	}
	return usage, nil
}

func (dc *DiskController) CleanupDiskFull(container string) error {
	cmd := exec.Command("docker", "exec", container, "rm", "-f", fillfilePath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("rm fillfile failed: %v, output: %s", err, string(out))
	}
	return nil
}

type DiskFullEvidence struct {
	ScenarioID          string  `json:"scenario_id"`
	TargetNode          string  `json:"target_node"`
	PressureLevel       string  `json:"pressure_level"`
	InjectTimestamp     string  `json:"inject_timestamp"`
	DiskUsageBefore     int     `json:"disk_usage_before"`
	DiskUsageAfter      int     `json:"disk_usage_after"`
	ClusterAvailable    bool    `json:"cluster_available"`
	LeaderChanged       bool    `json:"leader_changed"`
	ElectionCompletionS float64 `json:"election_completion_s"`
	SurvivalRate        float64 `json:"survival_rate"`
	NodeCrashed         bool    `json:"node_crashed"`
	RecoveryTimestamp   string  `json:"recovery_timestamp"`
	LogCatchupDurationS float64 `json:"log_catchup_duration_s"`
	Status              string  `json:"status"`
}
