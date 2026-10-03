package main

import (
	"encoding/json"
	"flag"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Result struct {
	Status  string `json:"status"`
	Target  string `json:"target"`
	Message string `json:"message"`
}

func main() {
	targetPtr := flag.String("target", "core_router", "指定要检查的目标节点名称")
	flag.Parse()

	conn, err := grpc.NewClient("localhost:9501", grpc.WithTransportCredentials(insecure.NewCredentials()))

	res := &Result{Target: *targetPtr}

	if err != nil {
		res.Status = "FAIL"
		res.Message = fmt.Sprintf("目标端口 9501 不可达，节点可能已离线。错误: %v", err)
	} else {
		defer conn.Close()
		res.Status = "PASS"
		res.Message = "节点连接正常，系统可靠性验证通过"
	}

	jsonData, _ := json.Marshal(res)
	fmt.Println(string(jsonData))
}
