package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type ResumeManager struct {
	resumePath string
	fullRerun  bool
}

func NewResumeManager(resumePath string, fullRerun bool) *ResumeManager {
	return &ResumeManager{resumePath: resumePath, fullRerun: fullRerun}
}

func (rm *ResumeManager) LoadResume() (*ResumeState, error) {
	data, err := os.ReadFile(rm.resumePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &ResumeState{}, nil
		}
		backupPath := rm.resumePath + ".bak"
		os.WriteFile(backupPath, data, 0644)
		return &ResumeState{}, fmt.Errorf("resume corrupted, backed up to %s", backupPath)
	}

	state := &ResumeState{}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "- ") {
			scenarioID := strings.TrimSpace(strings.TrimPrefix(line, "- "))
			if strings.Contains(scenarioID, "commit") || strings.Contains(scenarioID, "SDD") || strings.Contains(scenarioID, "T0.") || strings.Contains(scenarioID, "T1.") {
				continue
			}
			state.CompletedScenarios = append(state.CompletedScenarios, scenarioID)
		}
	}
	state.LastUpdated = time.Now()
	return state, nil
}

func (rm *ResumeManager) MarkCompleted(scenarioID string) error {
	state, err := rm.LoadResume()
	if err != nil {
		state = &ResumeState{}
	}
	state.CompletedScenarios = append(state.CompletedScenarios, scenarioID)
	state.LastUpdated = time.Now()

	dir := filepath.Dir(rm.resumePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("mkdir: %v", err)
	}

	f, err := os.OpenFile(rm.resumePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open resume: %v", err)
	}
	defer f.Close()

	fmt.Fprintf(f, "- %s (completed at %s)\n", scenarioID, time.Now().Format("2006-01-02T15:04:05"))
	return nil
}

func (rm *ResumeManager) IsCompleted(scenarioID string) bool {
	state, err := rm.LoadResume()
	if err != nil {
		return false
	}
	for _, s := range state.CompletedScenarios {
		if s == scenarioID {
			return true
		}
	}
	return false
}

func (rm *ResumeManager) ShouldSkip(scenarioID string) bool {
	if rm.fullRerun {
		return false
	}
	return rm.IsCompleted(scenarioID)
}
