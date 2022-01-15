package main

import (
	"distributed-scheduler/core"
	"distributed-scheduler/proto/job"
	"distributed-scheduler/proto/node"
	"distributed-scheduler/service"
	"flag"
	"fmt"
	"log"

	"google.golang.org/grpc"
)

var (
	port   = flag.Int("port", 50051, "TCP port for this node")
	raftId = flag.String("raft_id", "", "Node id used by Raft")
)

func main() {
	flag.Parse()

	if *raftId == "" {
		log.Fatalf("flag --raft_id is required")
	}

	app := core.NewApp(core.WithAppNameOption("distributed-scheduler"), core.WithNacosAddrOption("10.11.12.78:8848"), core.WithNacosNamespaceOption("da30c79a-ad57-4ede-89dd-5eeb640d8655"), core.WithNacosServiceGroupOption("my-service"), core.WithPortOption(*port), core.WithNodeIdOption(*raftId), core.WithSchedulerThreadOption(10)).RegisterGrpc(func(ctx *core.ApplicationContext, server *grpc.Server) {
		node.RegisterNodeServiceServer(server, service.NewNodeService(ctx))
	}).RegisterGrpc(func(ctx *core.ApplicationContext, server *grpc.Server) {
		job.RegisterJobServiceServer(server, service.NewJobService(ctx))
	})

	err := app.Run()
	if err != nil {
		fmt.Println("got err:", err)
	}
}
