package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

func main() {
	var (
		contractPath  string
		evidenceDir   string
		clusterConfig string
		fullRerun     bool
		scenarioType  string
		timebox       string
	)

	flag.StringVar(&contractPath, "contract", "", "path to batch21.yaml")
	flag.StringVar(&evidenceDir, "evidence-dir", "", "path to evidence directory")
	flag.StringVar(&clusterConfig, "cluster-config", "", "path to cluster config.toml")
	flag.BoolVar(&fullRerun, "full-rerun", false, "full rerun")
	flag.StringVar(&scenarioType, "scenario-type", "all", "steady|under_load|cascading|all")
	flag.StringVar(&timebox, "timebox", "6h", "timebox duration")
	flag.Parse()

	if contractPath == "" || evidenceDir == "" {
		fmt.Fprintln(os.Stderr, "ERROR: --contract and --evidence-dir required")
		os.Exit(1)
	}

	timeboxDuration, err := time.ParseDuration(timebox)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: invalid timebox: %v\n", err)
		os.Exit(1)
	}

	resumePath := filepath.Join(evidenceDir, "RESUME.md")
	rm := NewResumeManager(resumePath, fullRerun)
	resumeState, err := rm.LoadResume()
	if err != nil {
		log.Printf("WARN: LoadResume: %v, starting fresh", err)
		resumeState = &ResumeState{}
	}
	_ = resumeState

	em := NewEvidenceManager(evidenceDir)
	nc := NewNodeController("raft-node-", 9001)
	collector := NewCollector(nc)
	scheduler := NewScheduler(nc, collector, em, rm, contractPath)

	scenarios := scheduler.BuildScenarioMatrix(scenarioType)
	log.Printf("Built %d scenarios (type=%s)", len(scenarios), scenarioType)

	deadline := time.Now().Add(timeboxDuration)
	completed := 0

	for _, sid := range scenarios {
		if rm.ShouldSkip(sid) {
			log.Printf("SKIP %s (completed)", sid)
			continue
		}
		if time.Now().After(deadline) {
			log.Printf("TIMEBOX expired. Completed: %d/%d", completed, len(scenarios))
			break
		}
		log.Printf("Executing %s ...", sid)
		result, err := scheduler.ExecuteScenario(sid)
		if err != nil {
			log.Printf("ERROR %s: %v", sid, err)
			result = &ScenarioResult{ScenarioID: sid, Status: "BLOCKED"}
		}
		if err := em.PersistScenario(*result); err != nil {
			log.Printf("ERROR persist %s: %v", sid, err)
		}
		if err := rm.MarkCompleted(sid); err != nil {
			log.Printf("WARN MarkCompleted %s: %v", sid, err)
		}
		completed++
	}
	log.Printf("Done. Completed: %d/%d", completed, len(scenarios))
}
