package main

import "time"

type ScenarioResult struct {
	ScenarioID        string                   `json:"scenario_id"`
	ScenarioType      string                   `json:"scenario_type"`
	Timeline          []TimelineEvent          `json:"timeline"`
	ElectionMetrics   ElectionMetrics          `json:"election_metrics"`
	RejectMetrics     RejectMetrics            `json:"reject_metrics"`
	SurvivalMetrics   SurvivalMetrics          `json:"survival_metrics"`
	SplitBrainMetrics SplitBrainMetrics        `json:"split_brain_metrics"`
	LogMetrics        LogMetrics               `json:"log_metrics"`
	PartitionMetrics  *NetworkPartitionMetrics `json:"partition_metrics,omitempty"`
	CompositeMetrics  *CompositeMetrics        `json:"composite_metrics,omitempty"`
	Status            string                   `json:"status"`
	EvidencePath      string                   `json:"evidence_path"`
}

type TimelineEvent struct {
	Timestamp time.Time `json:"timestamp"`
	EventType string    `json:"event_type"`
	NodeID    string    `json:"node_id"`
	Role      string    `json:"role"`
	Detail    string    `json:"detail"`
}

type ElectionMetrics struct {
	KillTimestamp      time.Time `json:"kill_timestamp"`
	ElectionStart      time.Time `json:"election_start"`
	ElectionComplete   time.Time `json:"election_complete"`
	CompletionDuration float64   `json:"completion_duration"`
}

type RejectMetrics struct {
	TotalRequests          int64     `json:"total_requests"`
	RejectedRequests       int64     `json:"rejected_requests"`
	RejectRate             float64   `json:"reject_rate"`
	RecoveryTimestamp      time.Time `json:"recovery_timestamp"`
	PostRecoveryRejectRate float64   `json:"post_recovery_reject_rate"`
}

type SurvivalMetrics struct {
	SampledEntriesCount  int             `json:"sampled_entries_count"`
	SurvivedEntriesCount int             `json:"survived_entries_count"`
	SurvivalRate         float64         `json:"survival_rate"`
	MismatchedEntries    []EntryMismatch `json:"mismatched_entries"`
}

type SplitBrainMetrics struct {
	MaxConcurrentLeaders int         `json:"max_concurrent_leaders"`
	SplitBrainDetected   bool        `json:"split_brain_detected"`
	DetectionTimestamps  []time.Time `json:"detection_timestamps"`
}

type LogMetrics struct {
	LogEntriesCount  int64 `json:"log_entries_count"`
	TimelineComplete bool  `json:"timeline_complete"`
	Replayable       bool  `json:"replayable"`
}

type EntryMismatch struct {
	EntryIndex    int64  `json:"entry_index"`
	ExpectedTerm  int64  `json:"expected_term"`
	ActualTerm    int64  `json:"actual_term"`
	ExpectedValue string `json:"expected_value"`
	ActualValue   string `json:"actual_value"`
}

type EntrySnapshot struct {
	EntryIndex int64  `json:"entry_index"`
	EntryTerm  int64  `json:"entry_term"`
	EntryValue string `json:"entry_value"`
}

type ResumeState struct {
	SessionID          string    `json:"session_id"`
	TotalScenarios     int       `json:"total_scenarios"`
	LastUpdated        time.Time `json:"last_updated"`
	CompletedScenarios []string  `json:"completed_scenarios"`
}

type RaftStats struct {
	ID        string `json:"id"`
	State     string `json:"state"`
	Term      int64  `json:"term"`
	Leader    string `json:"leader_id"`
	Commit    int64  `json:"commit_index"`
	Applied   int64  `json:"last_applied"`
	LogCount  int64  `json:"log_count"`
	PeerCount int64  `json:"peer_count"`
	VotedFor  string `json:"voted_for"`
}

type SurvivalResult struct {
	SurvivalRate      float64         `json:"survival_rate"`
	SurvivedCount     int             `json:"survived_count"`
	TotalCount        int             `json:"total_count"`
	MismatchedEntries []EntryMismatch `json:"mismatched_entries"`
}
type PreVoteForensics struct {
	ScenarioID               string         `json:"scenario_id"`
	PreVoteRoundCount        int            `json:"prevote_round_count"`
	FormalElectionRoundCount int            `json:"formal_election_round_count"`
	VoteDistribution         map[string]int `json:"vote_distribution"`
	TermBefore               int64          `json:"term_before"`
	TermAfter                int64          `json:"term_after"`
	TermInflation            int64          `json:"term_inflation"`
	ElectionCompletionS      float64        `json:"election_completion_s"`
	BaselineSnapshot          string         `json:"baseline_snapshot"`
	Timestamp                time.Time      `json:"timestamp"`
}

type NetworkPartitionMetrics struct {
	PartitionType        string   `json:"partition_type"`
	PartitionedNodes     []string `json:"partitioned_nodes"`
	MajorityNodes        []string `json:"majority_nodes"`
	PartitionDurationS   float64  `json:"partition_duration_s"`
	RecoveryDurationS    float64  `json:"recovery_duration_s"`
	MaxConcurrentLeaders int      `json:"max_concurrent_leaders"`
	MajorityLeader       string   `json:"majority_leader"`
	MinorityLeaderCount  int      `json:"minority_leader_count"`
	TermBefore           int64    `json:"term_before"`
	TermAfter            int64    `json:"term_after"`
	TermMonotonic        bool     `json:"term_monotonic"`
	CommitIndexBefore    int64    `json:"commit_index_before"`
	CommitIndexAfter     int64    `json:"commit_index_after"`
	CommitCaughtUp       bool     `json:"commit_caught_up"`
}
type CompositeMetrics struct {
	PartitionType        string   `json:"partition_type"`
	PartitionedNodes     []string `json:"partitioned_nodes"`
	MajorityNodes        []string `json:"majority_nodes"`
	DiskFullTarget       string   `json:"disk_full_target"`
	DiskPressureLevel    string   `json:"disk_pressure_level"`
	PartitionDurationS   float64  `json:"partition_duration_s"`
	MaxConcurrentLeaders int      `json:"max_concurrent_leaders"`
	MajorityLeader       string   `json:"majority_leader"`
	MinorityLeaderCount  int      `json:"minority_leader_count"`
	TermBefore           int64    `json:"term_before"`
	TermAfter            int64    `json:"term_after"`
	TermMonotonic        bool     `json:"term_monotonic"`
	CommitIndexBefore    int64    `json:"commit_index_before"`
	CommitIndexAfter     int64    `json:"commit_index_after"`
	CommitCaughtUp       bool     `json:"commit_caught_up"`
	ClusterAvailable     bool     `json:"cluster_available"`
	RecoveryConfirmed    bool     `json:"recovery_confirmed"`
}
