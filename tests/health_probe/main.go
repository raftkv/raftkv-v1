
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: health-probe <addr>")
		os.Exit(2)
	}
	addr := os.Args[1]
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println("NOT_SERVING")
		os.Exit(1)
	}
	defer conn.Close()

	client := healthpb.NewHealthClient(conn)
	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		fmt.Println("NOT_SERVING")
		os.Exit(1)
	}

	if resp.Status == healthpb.HealthCheckResponse_SERVING {
		fmt.Println("SERVING")
		os.Exit(0)
	}
	fmt.Println("NOT_SERVING")
	os.Exit(1)
}