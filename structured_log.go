package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type StructuredLogger struct {
	mu     sync.Mutex
	nodeID string
	enable bool
}

type LogEntry struct {
	Timestamp string                 `json:"ts"`
	Level     string                 `json:"level"`
	Event     string                 `json:"event"`
	NodeID    string                 `json:"node_id,omitempty"`
	Message   string                 `json:"msg,omitempty"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

var globalStructLogger *StructuredLogger

func InitStructuredLogger(nodeID string) *StructuredLogger {
	enable := os.Getenv("STRUCTURED_LOG") == "true" || os.Getenv("STRUCTURED_LOG") == "1"
	sl := &StructuredLogger{
		nodeID: nodeID,
		enable: enable,
	}
	globalStructLogger = sl
	return sl
}

func (sl *StructuredLogger) log(level, event, msg string, fields map[string]interface{}) {
	if sl == nil || !sl.enable {
		return
	}
	entry := LogEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Level:     level,
		Event:     event,
		NodeID:    sl.nodeID,
		Message:   msg,
		Fields:    fields,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}
	sl.mu.Lock()
	fmt.Fprintln(os.Stdout, string(data))
	sl.mu.Unlock()
}

func (sl *StructuredLogger) Info(event, msg string, fields map[string]interface{}) {
	sl.log("info", event, msg, fields)
}

func (sl *StructuredLogger) Warn(event, msg string, fields map[string]interface{}) {
	sl.log("warn", event, msg, fields)
}

func (sl *StructuredLogger) Error(event, msg string, fields map[string]interface{}) {
	sl.log("error", event, msg, fields)
}

func slogInfo(event, msg string, fields map[string]interface{}) {
	if globalStructLogger != nil {
		globalStructLogger.Info(event, msg, fields)
	}
}

func slogWarn(event, msg string, fields map[string]interface{}) {
	if globalStructLogger != nil {
		globalStructLogger.Warn(event, msg, fields)
	}
}

func slogError(event, msg string, fields map[string]interface{}) {
	if globalStructLogger != nil {
		globalStructLogger.Error(event, msg, fields)
	}
}
