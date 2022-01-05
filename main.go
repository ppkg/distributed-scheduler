package main

import (
	"distributed-scheduler/core"
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

	app := core.NewApp(core.WithAppNameOption("distributed-scheduler"), core.WithNacosAddrOption("mse-e52dbdd6-p.nacos-ans.mse.aliyuncs.com:8848"), core.WithNacosNamespaceOption("27fdefc2-ae39-41fd-bac4-9256acbf97bc"), core.WithNacosGroupNameOption("test"), core.WithPortOption(*port), core.WithNodeIdOption(*raftId)).RegisterGrpc(func(ctx *core.ApplicationContext, server *grpc.Server) {
		node.RegisterNodeServiceServer(server, service.NewNodeService(ctx))
	})

	err := app.Run()
	if err != nil {
		fmt.Println("got err:", err)
	}
}
