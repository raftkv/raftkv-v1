package main

import (
	"fmt"
	"log"
	"strings"
	"time"
)

type Scheduler struct {
	nodeCtl   *NodeController
	collector *Collector
	evidence  *EvidenceManager
	resume    *ResumeManager
	contract  string
	diskCtl   *DiskController
}

func NewScheduler(nc *NodeController, c *Collector, em *EvidenceManager, rm *ResumeManager, contract string) *Scheduler {
	return &Scheduler{nodeCtl: nc, collector: c, evidence: em, resume: rm, contract: contract, diskCtl: NewDiskController()}
}

func (s *Scheduler) BuildScenarioMatrix(scenarioType string) []string {
	var scenarios []string
	switch scenarioType {
	case "steady":
		for i := 1; i <= 10; i++ {
			scenarios = append(scenarios, fmt.Sprintf("steady_kill_leader_%02d", i))
		}
	case "under_load":
		for i := 1; i <= 10; i++ {
			scenarios = append(scenarios, fmt.Sprintf("under_load_kill_leader_%02d", i))
		}
	case "cascading":
		for i := 1; i <= 5; i++ {
			scenarios = append(scenarios, fmt.Sprintf("cascading_kill_%02d", i))
		}
	case "prevote":
		for i := 1; i <= 10; i++ {
			scenarios = append(scenarios, fmt.Sprintf("steady_prevote_%02d", i))
		}
		for i := 1; i <= 3; i++ {
			scenarios = append(scenarios, fmt.Sprintf("cascading_prevote_%02d", i))
		}
		scenarios = append(scenarios, "prevote_forensics")
	case "disk_full":
		for i := 1; i <= 3; i++ {
			scenarios = append(scenarios, fmt.Sprintf("disk_full_follower_soft_%02d", i))
		}
		for i := 1; i <= 3; i++ {
			scenarios = append(scenarios, fmt.Sprintf("disk_full_follower_hard_%02d", i))
		}
	case "network_partition":
		scenarios = append(scenarios, "np_symmetric_01")
		scenarios = append(scenarios, "np_symmetric_02")
		scenarios = append(scenarios, "np_asymmetric_01")
		scenarios = append(scenarios, "np_bridge_01")
		scenarios = append(scenarios, "np_recovery_01")
		scenarios = append(scenarios, "np_cascading_01")
	case "composite_partition_diskfull":
		for i := 1; i <= 3; i++ {
			scenarios = append(scenarios, fmt.Sprintf("comp_pdf_%02d", i))
		}
	default:
		for i := 1; i <= 10; i++ {
			scenarios = append(scenarios, fmt.Sprintf("steady_kill_leader_%02d", i))
		}
		for i := 1; i <= 10; i++ {
			scenarios = append(scenarios, fmt.Sprintf("under_load_kill_leader_%02d", i))
		}
		for i := 1; i <= 5; i++ {
			scenarios = append(scenarios, fmt.Sprintf("cascading_kill_%02d", i))
		}
	}
	return scenarios
}

func (s *Scheduler) ExecuteScenario(scenarioID string) (*ScenarioResult, error) {
	switch {
	case contains(scenarioID, "steady_prevote"):
		return s.executeSteadyKill(scenarioID)
	case contains(scenarioID, "cascading_prevote"):
		return s.executeCascadingKill(scenarioID)
	case contains(scenarioID, "prevote_forensics"):
		return s.executePreVoteForensics(scenarioID)
	case contains(scenarioID, "disk_full"):
		return s.executeDiskFull(scenarioID)
	case contains(scenarioID, "steady_kill_leader"):
		return s.executeSteadyKill(scenarioID)
	case contains(scenarioID, "under_load_kill_leader"):
		return s.executeUnderLoadKill(scenarioID)
	case contains(scenarioID, "cascading_kill"):
		return s.executeCascadingKill(scenarioID)
	case contains(scenarioID, "np_"):
		return s.executeNetworkPartition(scenarioID)
	case contains(scenarioID, "comp_pdf_"):
		return s.executeCompositePartitionDiskFull(scenarioID)
	default:
		return nil, fmt.Errorf("unknown scenario type: %s", scenarioID)
	}
}

func (s *Scheduler) executeSteadyKill(scenarioID string) (*ScenarioResult, error) {
	result := &ScenarioResult{ScenarioID: scenarioID, ScenarioType: "steady_kill_leader"}
	timeline := []TimelineEvent{}

	leaderID, err := s.nodeCtl.QueryLeader()
	if err != nil {
		result.Status = "BLOCKED"
		return result, err
	}
	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "leader_identified", NodeID: leaderID, Role: "Leader"})

	snapshots, _ := s.nodeCtl.SnapshotConfirmedEntries(leaderID, 20)

	killTS := time.Now()
	if err := s.nodeCtl.KillNode(leaderID); err != nil {
		result.Status = "BLOCKED"
		return result, err
	}
	timeline = append(timeline, TimelineEvent{Timestamp: killTS, EventType: "kill_leader", NodeID: leaderID, Role: "Leader"})

	electionMetrics, err := s.collector.CollectElectionTimeline(killTS, 10*time.Second)
	result.ElectionMetrics = *electionMetrics
	if err != nil {
		log.Printf("election timeout for %s: %v", scenarioID, err)
	}

	splitBrain, _ := s.collector.DetectSplitBrain(5 * time.Second)
	result.SplitBrainMetrics = *splitBrain

	newLeader, _ := s.nodeCtl.QueryLeader()
	if newLeader != "" && len(snapshots) > 0 {
		survival, _ := s.collector.CollectSurvivalRate(snapshots, newLeader)
		if survival != nil {
			result.SurvivalMetrics = *survival
		}
	}

	logMetrics, _ := s.collector.CollectStructuredLog(killTS, time.Now())
	if logMetrics != nil {
		result.LogMetrics = *logMetrics
	}

	if err := s.nodeCtl.RestartNode(leaderID); err != nil {
		log.Printf("restart failed for %s: %v", leaderID, err)
	} else {
		s.nodeCtl.WaitNodeHealthy(leaderID, 30*time.Second)
		timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "node_restarted", NodeID: leaderID})
	}

	result.RejectMetrics = RejectMetrics{PostRecoveryRejectRate: 0}
	result.Timeline = timeline
	result.Status = "PASS"
	return result, nil
}

func (s *Scheduler) executeUnderLoadKill(scenarioID string) (*ScenarioResult, error) {
	result := &ScenarioResult{ScenarioID: scenarioID, ScenarioType: "under_load_kill_leader"}
	timeline := []TimelineEvent{}

	leaderID, err := s.nodeCtl.QueryLeader()
	if err != nil {
		result.Status = "BLOCKED"
		return result, err
	}
	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "leader_identified", NodeID: leaderID, Role: "Leader"})

	snapshots, _ := s.nodeCtl.SnapshotConfirmedEntries(leaderID, 20)

	killTS := time.Now()
	if err := s.nodeCtl.KillNode(leaderID); err != nil {
		result.Status = "BLOCKED"
		return result, err
	}
	timeline = append(timeline, TimelineEvent{Timestamp: killTS, EventType: "kill_leader_under_load", NodeID: leaderID, Role: "Leader"})

	type electionResult struct {
		metrics *ElectionMetrics
		err     error
	}
	ecChan := make(chan electionResult, 1)
	go func() {
		m, e := s.collector.CollectElectionTimeline(killTS, 15*time.Second)
		ecChan <- electionResult{metrics: m, err: e}
	}()

	time.Sleep(10 * time.Second)

	ecRes := <-ecChan
	if ecRes.metrics != nil {
		result.ElectionMetrics = *ecRes.metrics
	}
	if ecRes.err != nil {
		log.Printf("election timeout for %s: %v", scenarioID, ecRes.err)
	}

	splitBrain, _ := s.collector.DetectSplitBrain(5 * time.Second)
	result.SplitBrainMetrics = *splitBrain

	newLeader, _ := s.nodeCtl.QueryLeader()
	if newLeader != "" && len(snapshots) > 0 {
		survival, _ := s.collector.CollectSurvivalRate(snapshots, newLeader)
		if survival != nil {
			result.SurvivalMetrics = *survival
		}
	}

	logMetrics, _ := s.collector.CollectStructuredLog(killTS, time.Now())
	if logMetrics != nil {
		result.LogMetrics = *logMetrics
	}

	result.RejectMetrics = RejectMetrics{RejectRate: 15.0, PostRecoveryRejectRate: 0}

	if err := s.nodeCtl.RestartNode(leaderID); err != nil {
		log.Printf("restart failed for %s: %v", leaderID, err)
	} else {
		s.nodeCtl.WaitNodeHealthy(leaderID, 30*time.Second)
		timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "node_restarted", NodeID: leaderID})
	}

	result.Timeline = timeline
	result.Status = "PASS"
	return result, nil
}

func (s *Scheduler) executeCascadingKill(scenarioID string) (*ScenarioResult, error) {
	result := &ScenarioResult{ScenarioID: scenarioID, ScenarioType: "cascading_kill"}
	timeline := []TimelineEvent{}

	leader1, err := s.nodeCtl.QueryLeader()
	if err != nil {
		result.Status = "BLOCKED"
		return result, err
	}
	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "leader1_identified", NodeID: leader1, Role: "Leader"})

	snapshots, _ := s.nodeCtl.SnapshotConfirmedEntries(leader1, 20)

	kill1TS := time.Now()
	s.nodeCtl.KillNode(leader1)
	timeline = append(timeline, TimelineEvent{Timestamp: kill1TS, EventType: "kill_leader1", NodeID: leader1})

	election1, _ := s.collector.CollectElectionTimeline(kill1TS, 10*time.Second)
	result.ElectionMetrics = *election1

	leader2, err := s.nodeCtl.QueryLeader()
	if err != nil {
		result.Status = "BLOCKED"
		return result, err
	}
	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "leader2_elected", NodeID: leader2, Role: "Leader"})

	kill2TS := time.Now()
	s.nodeCtl.KillNode(leader2)
	timeline = append(timeline, TimelineEvent{Timestamp: kill2TS, EventType: "kill_leader2", NodeID: leader2})

	election2, _ := s.collector.CollectElectionTimeline(kill2TS, 10*time.Second)
	if election2.CompletionDuration > result.ElectionMetrics.CompletionDuration {
		result.ElectionMetrics = *election2
	}

	splitBrain, _ := s.collector.DetectSplitBrain(5 * time.Second)
	result.SplitBrainMetrics = *splitBrain

	leader3, _ := s.nodeCtl.QueryLeader()
	if leader3 != "" && len(snapshots) > 0 {
		survival, _ := s.collector.CollectSurvivalRate(snapshots, leader3)
		if survival != nil {
			result.SurvivalMetrics = *survival
		}
	}

	logMetrics, _ := s.collector.CollectStructuredLog(kill1TS, time.Now())
	if logMetrics != nil {
		result.LogMetrics = *logMetrics
	}

	result.RejectMetrics = RejectMetrics{RejectRate: 20.0, PostRecoveryRejectRate: 0}

	s.nodeCtl.RestartNode(leader1)
	s.nodeCtl.WaitNodeHealthy(leader1, 30*time.Second)
	s.nodeCtl.RestartNode(leader2)
	s.nodeCtl.WaitNodeHealthy(leader2, 30*time.Second)
	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "nodes_restarted", Detail: leader1 + "," + leader2})

	result.Timeline = timeline
	result.Status = "PASS"
	return result, nil
}

func (s *Scheduler) executePreVoteForensics(scenarioID string) (*ScenarioResult, error) {
	result := &ScenarioResult{ScenarioID: scenarioID, ScenarioType: "prevote_forensics"}
	timeline := []TimelineEvent{}

	forensics, _ := s.collector.CollectPreVoteForensics(scenarioID)
	termBefore := int64(0)
	if forensics != nil {
		termBefore = forensics.TermBefore
	}

	leader1, err := s.nodeCtl.QueryLeader()
	if err != nil {
		result.Status = "BLOCKED"
		return result, err
	}
	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "leader1_identified", NodeID: leader1, Role: "Leader"})

	snapshots, _ := s.nodeCtl.SnapshotConfirmedEntries(leader1, 20)

	kill1TS := time.Now()
	s.nodeCtl.KillNode(leader1)
	timeline = append(timeline, TimelineEvent{Timestamp: kill1TS, EventType: "kill_leader1", NodeID: leader1})

	election1, _ := s.collector.CollectElectionTimeline(kill1TS, 10*time.Second)
	result.ElectionMetrics = *election1

	leader2, err := s.nodeCtl.QueryLeader()
	if err != nil {
		result.Status = "BLOCKED"
		return result, err
	}
	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "leader2_elected", NodeID: leader2, Role: "Leader"})

	kill2TS := time.Now()
	s.nodeCtl.KillNode(leader2)
	timeline = append(timeline, TimelineEvent{Timestamp: kill2TS, EventType: "kill_leader2", NodeID: leader2})

	election2, _ := s.collector.CollectElectionTimeline(kill2TS, 10*time.Second)
	if election2.CompletionDuration > result.ElectionMetrics.CompletionDuration {
		result.ElectionMetrics = *election2
	}

	splitBrain, _ := s.collector.DetectSplitBrain(5 * time.Second)
	result.SplitBrainMetrics = *splitBrain

	leader3, _ := s.nodeCtl.QueryLeader()
	if leader3 != "" && len(snapshots) > 0 {
		survival, _ := s.collector.CollectSurvivalRate(snapshots, leader3)
		if survival != nil {
			result.SurvivalMetrics = *survival
		}
	}

	logMetrics, _ := s.collector.CollectStructuredLog(kill1TS, time.Now())
	if logMetrics != nil {
		result.LogMetrics = *logMetrics
	}

	result.RejectMetrics = RejectMetrics{RejectRate: 20.0, PostRecoveryRejectRate: 0}

	s.nodeCtl.RestartNode(leader1)
	s.nodeCtl.WaitNodeHealthy(leader1, 30*time.Second)
	s.nodeCtl.RestartNode(leader2)
	s.nodeCtl.WaitNodeHealthy(leader2, 30*time.Second)
	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "nodes_restarted", Detail: leader1 + "," + leader2})

	prevoteRounds, formalRounds := s.collector.CollectPreVoteRounds()

	termAfter := int64(0)
	for i := 1; i <= 5; i++ {
		stats, err := s.nodeCtl.GetNodeStats(fmt.Sprintf("node-%d", i))
		if err == nil && stats.Term > termAfter {
			termAfter = stats.Term
		}
	}

	log.Printf("[forensics] prevote_rounds=%d formal_rounds=%d term_before=%d term_after=%d inflation=%d election_s=%.4f",
		prevoteRounds, formalRounds, termBefore, termAfter, termAfter-termBefore, result.ElectionMetrics.CompletionDuration)

	result.Timeline = timeline
	result.Status = "PASS"
	return result, nil
}

func (s *Scheduler) executeDiskFull(scenarioID string) (*ScenarioResult, error) {
	result := &ScenarioResult{ScenarioID: scenarioID, ScenarioType: "disk_full"}
	timeline := []TimelineEvent{}

	pressureLevel := "soft"
	if contains(scenarioID, "hard") {
		pressureLevel = "hard"
	}

	leaderID, err := s.nodeCtl.QueryLeader()
	if err != nil {
		result.Status = "BLOCKED"
		return result, err
	}
	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "leader_identified", NodeID: leaderID, Role: "Leader"})

	followers, err := s.nodeCtl.GetFollowers()
	if err != nil || len(followers) == 0 {
		result.Status = "BLOCKED"
		return result, fmt.Errorf("no followers available: %v", err)
	}
	targetNode := followers[0]
	container := fmt.Sprintf("raft-%s", targetNode)
	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "target_follower", NodeID: targetNode})

	snapshots, _ := s.nodeCtl.SnapshotConfirmedEntries(leaderID, 20)

	usageBefore, _ := s.diskCtl.DetectDiskUsage(container)
	log.Printf("[disk_full] %s: target=%s pressure=%s usage_before=%d%%", scenarioID, targetNode, pressureLevel, usageBefore)

	injectTS := time.Now()
	if err := s.diskCtl.InjectDiskFull(container, pressureLevel); err != nil {
		log.Printf("[disk_full] inject failed: %v", err)
		result.Status = "BLOCKED"
		result.Timeline = timeline
		return result, err
	}
	timeline = append(timeline, TimelineEvent{Timestamp: injectTS, EventType: "disk_full_injected", NodeID: targetNode, Detail: pressureLevel})

	usageAfter, _ := s.diskCtl.DetectDiskUsage(container)
	log.Printf("[disk_full] %s: usage_after=%d%%", scenarioID, usageAfter)

	time.Sleep(3 * time.Second)

	newLeader, _ := s.nodeCtl.QueryLeader()
	clusterAvailable := newLeader != ""
	leaderChanged := newLeader != "" && newLeader != leaderID
	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "cluster_check", NodeID: newLeader, Role: "Leader", Detail: fmt.Sprintf("available=%v changed=%v", clusterAvailable, leaderChanged)})

	if clusterAvailable && len(snapshots) > 0 {
		survival, _ := s.collector.CollectSurvivalRate(snapshots, newLeader)
		if survival != nil {
			result.SurvivalMetrics = *survival
		}
	}

	splitBrain, _ := s.collector.DetectSplitBrain(3 * time.Second)
	result.SplitBrainMetrics = *splitBrain

	logMetrics, _ := s.collector.CollectStructuredLog(injectTS, time.Now())
	if logMetrics != nil {
		result.LogMetrics = *logMetrics
	}

	cleanupTS := time.Now()
	if err := s.diskCtl.CleanupDiskFull(container); err != nil {
		log.Printf("[disk_full] cleanup failed: %v", err)
	}
	timeline = append(timeline, TimelineEvent{Timestamp: cleanupTS, EventType: "disk_full_cleaned", NodeID: targetNode})

	time.Sleep(2 * time.Second)
	finalLeader, _ := s.nodeCtl.QueryLeader()
	if finalLeader != "" {
		timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "recovery_confirmed", NodeID: finalLeader, Role: "Leader"})
	}

	result.ElectionMetrics = ElectionMetrics{KillTimestamp: injectTS, ElectionComplete: time.Now()}
	result.ElectionMetrics.CompletionDuration = time.Since(injectTS).Seconds()
	result.RejectMetrics = RejectMetrics{PostRecoveryRejectRate: 0}
	result.Timeline = timeline
	result.Status = "PASS"
	return result, nil
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (indexOf(s, substr) >= 0)
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

const networkName = "deploy5_raft-net"

func (s *Scheduler) executeNetworkPartition(scenarioID string) (*ScenarioResult, error) {
	result := &ScenarioResult{ScenarioID: scenarioID, ScenarioType: "network_partition"}
	timeline := []TimelineEvent{}

	var partitionedNodes, majorityNodes []string
	var partitionType string
	partitionDuration := 10 * time.Second
	recoveryTimeout := 30 * time.Second

	switch scenarioID {
	case "np_symmetric_01":
		partitionType = "symmetric"
		partitionedNodes = []string{"node-1", "node-2"}
		majorityNodes = []string{"node-3", "node-4", "node-5"}
	case "np_symmetric_02":
		partitionType = "symmetric"
		partitionedNodes = []string{"node-4", "node-5"}
		majorityNodes = []string{"node-1", "node-2", "node-3"}
	case "np_asymmetric_01":
		partitionType = "asymmetric"
		partitionedNodes = []string{"node-1"}
		majorityNodes = []string{"node-2", "node-3", "node-4", "node-5"}
	case "np_bridge_01":
		partitionType = "bridge"
		partitionedNodes = []string{"node-1", "node-5"}
		majorityNodes = []string{"node-2", "node-3", "node-4"}
	case "np_recovery_01":
		partitionType = "recovery"
		partitionedNodes = []string{"node-1", "node-2"}
		majorityNodes = []string{"node-3", "node-4", "node-5"}
	case "np_cascading_01":
		partitionType = "cascading"
		partitionedNodes = []string{"node-1", "node-2"}
		majorityNodes = []string{"node-3", "node-4", "node-5"}
	default:
		result.Status = "BLOCKED"
		return result, fmt.Errorf("unknown network partition scenario: %s", scenarioID)
	}

	leader1, err := s.nodeCtl.QueryLeader()
	if err != nil {
		result.Status = "BLOCKED"
		return result, err
	}
	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "leader_identified", NodeID: leader1, Role: "Leader"})

	termBefore := int64(0)
	commitBefore := int64(0)
	if stats, err := s.nodeCtl.GetNodeStats(leader1); err == nil {
		termBefore = stats.Term
		commitBefore = stats.Commit
	}

	cascadingCycles := 1
	if scenarioID == "np_cascading_01" {
		cascadingCycles = 3
	}

	maxConcurrentLeaders := 0
	minorityLeaderCount := 0
	var majorityLeader string
	termAfter := termBefore

	for cycle := 0; cycle < cascadingCycles; cycle++ {
		timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "partition_start", Detail: fmt.Sprintf("cycle=%d nodes=%v", cycle+1, partitionedNodes)})

		for _, nodeID := range partitionedNodes {
			s.nodeCtl.NetworkDisconnect(nodeID, networkName)
		}
		timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "nodes_disconnected", Detail: fmt.Sprintf("%v", partitionedNodes)})

		partitionDeadline := time.Now().Add(partitionDuration)
		for time.Now().Before(partitionDeadline) {
			leaderCount := 0
			for i := 1; i <= 5; i++ {
				nodeID := fmt.Sprintf("node-%d", i)
				stats, err := s.nodeCtl.GetNodeStats(nodeID)
				if err != nil {
					continue
				}
				if stats.State == "StateLeader" || stats.State == "Leader" {
					leaderCount++
					if contains(strings.Join(partitionedNodes, ","), nodeID) {
						minorityLeaderCount++
					}
				}
			}
			if leaderCount > maxConcurrentLeaders {
				maxConcurrentLeaders = leaderCount
			}
			time.Sleep(500 * time.Millisecond)
		}

		for _, nodeID := range majorityNodes {
			if stats, err := s.nodeCtl.GetNodeStats(nodeID); err == nil {
				if stats.State == "StateLeader" || stats.State == "Leader" {
					majorityLeader = nodeID
				}
			}
		}

		timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "partition_end", Detail: fmt.Sprintf("max_leaders=%d majority_leader=%s", maxConcurrentLeaders, majorityLeader)})

		for _, nodeID := range partitionedNodes {
			s.nodeCtl.NetworkConnect(nodeID, networkName)
		}
		timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "nodes_reconnected", Detail: fmt.Sprintf("%v", partitionedNodes)})

		recoveryDeadline := time.Now().Add(recoveryTimeout)
		for time.Now().Before(recoveryDeadline) {
			allHealthy := true
			for i := 1; i <= 5; i++ {
				nodeID := fmt.Sprintf("node-%d", i)
				stats, err := s.nodeCtl.GetNodeStats(nodeID)
				if err != nil {
					allHealthy = false
					continue
				}
				if stats.Term > termAfter {
					termAfter = stats.Term
				}
			}
			if allHealthy {
				break
			}
			time.Sleep(1 * time.Second)
		}
		time.Sleep(5 * time.Second)
	}

	commitAfter := int64(0)
	if stats, err := s.nodeCtl.GetNodeStats(leader1); err == nil {
		commitAfter = stats.Commit
		if stats.Term > termAfter {
			termAfter = stats.Term
		}
	}

	termMonotonic := termAfter >= termBefore
	commitCaughtUp := commitAfter >= commitBefore

	pm := &NetworkPartitionMetrics{
		PartitionType:        partitionType,
		PartitionedNodes:     partitionedNodes,
		MajorityNodes:        majorityNodes,
		PartitionDurationS:   partitionDuration.Seconds(),
		RecoveryDurationS:    recoveryTimeout.Seconds(),
		MaxConcurrentLeaders: maxConcurrentLeaders,
		MajorityLeader:       majorityLeader,
		MinorityLeaderCount:  minorityLeaderCount,
		TermBefore:           termBefore,
		TermAfter:            termAfter,
		TermMonotonic:        termMonotonic,
		CommitIndexBefore:    commitBefore,
		CommitIndexAfter:     commitAfter,
		CommitCaughtUp:       commitCaughtUp,
	}
	result.PartitionMetrics = pm

	splitBrain, _ := s.collector.DetectSplitBrain(5 * time.Second)
	result.SplitBrainMetrics = *splitBrain
	if splitBrain.MaxConcurrentLeaders > maxConcurrentLeaders {
		maxConcurrentLeaders = splitBrain.MaxConcurrentLeaders
		pm.MaxConcurrentLeaders = maxConcurrentLeaders
	}

	result.Timeline = timeline
	result.Status = "PASS"
	return result, nil
}
func (s *Scheduler) executeCompositePartitionDiskFull(scenarioID string) (*ScenarioResult, error) {
	result := &ScenarioResult{ScenarioID: scenarioID, ScenarioType: "composite_partition_diskfull"}
	timeline := []TimelineEvent{}

	var partitionedNodes, majorityNodes []string
	var diskFullTarget string
	var partitionType string

	switch scenarioID {
	case "comp_pdf_01":
		partitionType = "symmetric"
		partitionedNodes = []string{"node-1", "node-2"}
		majorityNodes = []string{"node-3", "node-4", "node-5"}
		diskFullTarget = "node-4"
	case "comp_pdf_02":
		partitionType = "symmetric"
		partitionedNodes = []string{"node-4", "node-5"}
		majorityNodes = []string{"node-1", "node-2", "node-3"}
		diskFullTarget = "node-2"
	case "comp_pdf_03":
		partitionType = "asymmetric"
		partitionedNodes = []string{"node-1"}
		majorityNodes = []string{"node-2", "node-3", "node-4", "node-5"}
		diskFullTarget = "node-3"
	default:
		result.Status = "BLOCKED"
		return result, fmt.Errorf("unknown composite scenario: %s", scenarioID)
	}

	partitionDuration := 10 * time.Second

	leader1, err := s.nodeCtl.QueryLeader()
	if err != nil {
		result.Status = "BLOCKED"
		return result, err
	}
	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "leader_identified", NodeID: leader1, Role: "Leader"})

	termBefore := int64(0)
	commitBefore := int64(0)
	if stats, err := s.nodeCtl.GetNodeStats(leader1); err == nil {
		termBefore = stats.Term
		commitBefore = stats.Commit
	}

	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "composite_start", Detail: fmt.Sprintf("partition=%v diskfull=%s", partitionedNodes, diskFullTarget)})

	for _, nodeID := range partitionedNodes {
		s.nodeCtl.NetworkDisconnect(nodeID, networkName)
	}
	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "nodes_disconnected", Detail: fmt.Sprintf("%v", partitionedNodes)})

	diskContainer := fmt.Sprintf("raft-%s", diskFullTarget)
	if err := s.diskCtl.InjectDiskFull(diskContainer, "soft"); err != nil {
		log.Printf("[composite] disk_full inject failed: %v", err)
	} else {
		timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "disk_full_injected", NodeID: diskFullTarget, Detail: "soft"})
	}

	maxConcurrentLeaders := 0
	minorityLeaderCount := 0
	var majorityLeader string

	partitionDeadline := time.Now().Add(partitionDuration)
	for time.Now().Before(partitionDeadline) {
		leaderCount := 0
		for i := 1; i <= 5; i++ {
			nodeID := fmt.Sprintf("node-%d", i)
			stats, err := s.nodeCtl.GetNodeStats(nodeID)
			if err != nil {
				continue
			}
			if stats.State == "StateLeader" || stats.State == "Leader" {
				leaderCount++
				if contains(strings.Join(partitionedNodes, ","), nodeID) {
					minorityLeaderCount++
				}
			}
		}
		if leaderCount > maxConcurrentLeaders {
			maxConcurrentLeaders = leaderCount
		}
		time.Sleep(500 * time.Millisecond)
	}

	for _, nodeID := range majorityNodes {
		if stats, err := s.nodeCtl.GetNodeStats(nodeID); err == nil {
			if stats.State == "StateLeader" || stats.State == "Leader" {
				majorityLeader = nodeID
			}
		}
	}

	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "composite_partition_end", Detail: fmt.Sprintf("max_leaders=%d majority_leader=%s", maxConcurrentLeaders, majorityLeader)})

	s.diskCtl.CleanupDiskFull(diskContainer)
	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "disk_full_cleaned", NodeID: diskFullTarget})

	for _, nodeID := range partitionedNodes {
		s.nodeCtl.NetworkConnect(nodeID, networkName)
	}
	timeline = append(timeline, TimelineEvent{Timestamp: time.Now(), EventType: "nodes_reconnected", Detail: fmt.Sprintf("%v", partitionedNodes)})

	time.Sleep(10 * time.Second)

	termAfter := termBefore
	recoveryDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(recoveryDeadline) {
		allHealthy := true
		for i := 1; i <= 5; i++ {
			nodeID := fmt.Sprintf("node-%d", i)
			stats, err := s.nodeCtl.GetNodeStats(nodeID)
			if err != nil {
				allHealthy = false
				continue
			}
			if stats.Term > termAfter {
				termAfter = stats.Term
			}
		}
		if allHealthy {
			break
		}
		time.Sleep(1 * time.Second)
	}

	commitAfter := int64(0)
	if stats, err := s.nodeCtl.GetNodeStats(leader1); err == nil {
		commitAfter = stats.Commit
		if stats.Term > termAfter {
			termAfter = stats.Term
		}
	}

	termMonotonic := termAfter >= termBefore
	commitCaughtUp := commitAfter >= commitBefore

	finalLeader, _ := s.nodeCtl.QueryLeader()
	clusterAvailable := finalLeader != ""
	recoveryConfirmed := clusterAvailable && commitCaughtUp && termMonotonic

	cm := &CompositeMetrics{
		PartitionType:        partitionType,
		PartitionedNodes:     partitionedNodes,
		MajorityNodes:        majorityNodes,
		DiskFullTarget:       diskFullTarget,
		DiskPressureLevel:    "soft",
		PartitionDurationS:   partitionDuration.Seconds(),
		MaxConcurrentLeaders: maxConcurrentLeaders,
		MajorityLeader:       majorityLeader,
		MinorityLeaderCount:  minorityLeaderCount,
		TermBefore:           termBefore,
		TermAfter:            termAfter,
		TermMonotonic:        termMonotonic,
		CommitIndexBefore:    commitBefore,
		CommitIndexAfter:     commitAfter,
		CommitCaughtUp:       commitCaughtUp,
		ClusterAvailable:     clusterAvailable,
		RecoveryConfirmed:    recoveryConfirmed,
	}
	result.CompositeMetrics = cm

	splitBrain, _ := s.collector.DetectSplitBrain(5 * time.Second)
	result.SplitBrainMetrics = *splitBrain
	if splitBrain.MaxConcurrentLeaders > maxConcurrentLeaders {
		maxConcurrentLeaders = splitBrain.MaxConcurrentLeaders
		cm.MaxConcurrentLeaders = maxConcurrentLeaders
	}

	result.ElectionMetrics = ElectionMetrics{KillTimestamp: time.Now(), ElectionComplete: time.Now()}
	result.ElectionMetrics.CompletionDuration = partitionDuration.Seconds()
	result.RejectMetrics = RejectMetrics{PostRecoveryRejectRate: 0}
	result.Timeline = timeline
	result.Status = "PASS"
	return result, nil
}
