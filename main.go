package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/ppkg/distributed-scheduler/core"
	"github.com/ppkg/distributed-scheduler/proto/job"
	"github.com/ppkg/distributed-scheduler/proto/node"
	"github.com/ppkg/distributed-scheduler/service"

	"google.golang.org/grpc"
)

var (
	port   = flag.Int("port", 5001, "TCP port for this node")
	nodeId = flag.String("nodeId", "", "Node id used by Raft")
)

func main() {
	flag.Parse()

	if *nodeId == "" {
		log.Fatalf("flag -nodeId is required")
	}

	app := core.NewApp(core.WithAppNameOption("distributed-scheduler"), core.WithNacosAddrOption("mse-e52dbdd6-p.nacos-ans.mse.aliyuncs.com:8848"), core.WithNacosNamespaceOption("27fdefc2-ae39-41fd-bac4-9256acbf97bc"), core.WithNacosServiceGroupOption("my-service"), core.WithPortOption(*port), core.WithNodeIdOption(*nodeId)).RegisterGrpc(func(ctx *core.ApplicationContext, server *grpc.Server) {
		node.RegisterNodeServiceServer(server, service.NewNodeService(ctx))
	}).RegisterGrpc(func(ctx *core.ApplicationContext, server *grpc.Server) {
		job.RegisterJobServiceServer(server, service.NewJobService(ctx))
	})

	app.Scheduler.BuildLimitRatePluginGroup("meituan", 5).AddLimitRatePlugin("meituan", "multi")

	err := app.Run()
	if err != nil {
		fmt.Println("start server but got err:", err)
	}
}
