package main

import (
	"encoding/json"
	"fmt"
	"math"
)

type Result struct {
	Status  string `json:"status"`
	Module  string `json:"module"`
	Message string `json:"message"`
	Detail  string `json:"detail,omitempty"`
}

func deterministicCompute(input float64) float64 {
	x := input
	for i := 0; i < 1000; i++ {
		x = math.Sin(x)*math.Cos(x+0.5) + math.Sqrt(math.Abs(x)+1)
		x = x*0.997 + 0.003*math.Log(math.Abs(x)+1)
	}
	return x
}

func main() {
	const iterations = 100
	const input = 3.14159265358979

	reference := deterministicCompute(input)
	results := make([]float64, iterations)
	allMatch := true
	maxDelta := 0.0

	for i := 0; i < iterations; i++ {
		results[i] = deterministicCompute(input)
		delta := math.Abs(results[i] - reference)
		if delta > maxDelta {
			maxDelta = delta
		}
		if delta > 1e-12 {
			allMatch = false
		}
	}

	res := &Result{Module: "双轨确定性"}

	if allMatch {
		res.Status = "PASS"
		res.Message = "确定性逻辑锁定成功"
		res.Detail = fmt.Sprintf("100次计算结果完全一致，最大偏差: %.2e", maxDelta)
	} else {
		res.Status = "FAIL"
		res.Message = "确定性逻辑未锁定，存在漂移"
		res.Detail = fmt.Sprintf("最大偏差: %.2e，超出容差阈值 1e-12", maxDelta)
	}

	jsonData, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(jsonData))
}
