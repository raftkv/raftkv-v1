package main

import (
	"encoding/json"
	"fmt"
)

type Result struct {
	Status  string
	Module  string
	Message string
	Detail  string
}

func copyState(src map[string]interface{}) map[string]interface{} {
	dst := make(map[string]interface{})
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func main() {
	state := map[string]interface{}{
		"precision": 1.0,
		"step":      0,
		"crashed":   false,
		"retry":     false,
	}

	type Step struct {
		name string
		fn   func(map[string]interface{}) error
	}

	steps := []Step{
		{"Step1", func(s map[string]interface{}) error {
			s["step"] = 1
			s["precision"] = 0.98
			return nil
		}},
		{"Step2", func(s map[string]interface{}) error {
			s["step"] = 2
			s["precision"] = 0.95
			return nil
		}},
		{"Step3", func(s map[string]interface{}) error {
			s["step"] = 3
			if s["retry"] == true {
				s["precision"] = 0.91
				return nil
			}
			s["precision"] = 0.31
			s["crashed"] = true
			return fmt.Errorf("精度骤降触发Recovery")
		}},
		{"Step4", func(s map[string]interface{}) error {
			s["step"] = 4
			s["precision"] = 0.92
			return nil
		}},
		{"Step5", func(s map[string]interface{}) error {
			s["step"] = 5
			s["precision"] = 0.96
			return nil
		}},
	}

	recovered := false

	for i, step := range steps {
		snapshot := copyState(state)
		err := step.fn(state)
		if err != nil {
			fmt.Printf("[崩溃] %s -> %v\n", step.name, err)
			fmt.Println("[Recovery] 正在执行回滚...")
			state = snapshot
			state["crashed"] = false
			state["retry"] = true
			state["precision"] = 0.90
			retryErr := steps[i].fn(state)
			if retryErr != nil {
				res := Result{Status: "FAIL", Module: "五级回滚", Message: "Recovery回滚后重试仍然失败"}
				jsonData, _ := json.MarshalIndent(res, "", "  ")
				fmt.Println(string(jsonData))
				return
			}
			recovered = true
			fmt.Printf("[Recovery] 回滚成功，重试 %s 完成\n", step.name)
		} else {
			fmt.Printf("[正常] %s -> precision=%.2f\n", step.name, state["precision"])
		}
	}

	finalStep, _ := state["step"].(int)
	res := Result{Module: "五级回滚"}
	if finalStep == 5 && recovered {
		res.Status = "PASS"
		res.Message = "Recovery机制自愈成功"
		res.Detail = fmt.Sprintf("第3步触发崩溃并成功回滚，最终走到第%d步", finalStep)
	} else {
		res.Status = "FAIL"
		res.Message = "Recovery机制未能完成自愈"
		res.Detail = fmt.Sprintf("最终步骤: %d, 是否触发回滚: %v", finalStep, recovered)
	}
	jsonData, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(jsonData))
}
