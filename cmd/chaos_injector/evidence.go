package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type EvidenceManager struct {
	evidenceDir string
}

func NewEvidenceManager(evidenceDir string) *EvidenceManager {
	return &EvidenceManager{evidenceDir: evidenceDir}
}

func (em *EvidenceManager) PersistScenario(result ScenarioResult) error {
	if err := os.MkdirAll(em.evidenceDir, 0755); err != nil {
		return fmt.Errorf("mkdir evidence: %v", err)
	}
	filename := fmt.Sprintf("scenario_%s.json", result.ScenarioID)
	path := filepath.Join(em.evidenceDir, filename)
	result.EvidencePath = path
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %v", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write %s: %v", path, err)
	}
	return nil
}

func (em *EvidenceManager) LoadScenario(scenarioID string) (*ScenarioResult, error) {
	filename := fmt.Sprintf("scenario_%s.json", scenarioID)
	path := filepath.Join(em.evidenceDir, filename)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %v", path, err)
	}
	var result ScenarioResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("unmarshal: %v", err)
	}
	return &result, nil
}

func (em *EvidenceManager) ListScenarios() ([]string, error) {
	entries, err := os.ReadDir(em.evidenceDir)
	if err != nil {
		return nil, fmt.Errorf("readdir: %v", err)
	}
	var scenarios []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "scenario_") && strings.HasSuffix(entry.Name(), ".json") {
			id := strings.TrimPrefix(entry.Name(), "scenario_")
			id = strings.TrimSuffix(id, ".json")
			scenarios = append(scenarios, id)
		}
	}
	return scenarios, nil
}
