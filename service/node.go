package service

import (
	"context"
	"fmt"

	"github.com/ppkg/distributed-scheduler/core"
	"github.com/ppkg/distributed-scheduler/proto/node"

	"github.com/golang/protobuf/ptypes/empty"
)

type nodeService struct {
	appCtx *core.ApplicationContext
}

// 获取主节点信息
func (s *nodeService) GetLeader(ctx context.Context, _ *empty.Empty) (*node.GetLeaderResponse, error) {
	nodeInfo := s.appCtx.GetLeaderNode()
	if nodeInfo.Endpoint == "" {
		return nil, fmt.Errorf("调度器主节点正在选举中...")
	}
	return &node.GetLeaderResponse{
		NodeInfo: &node.NodeInfo{
			Endpoint: nodeInfo.Endpoint,
			NodeId:   nodeInfo.Endpoint,
		},
	}, nil
}

func NewNodeService(ctx *core.ApplicationContext) node.NodeServiceServer {
	return &nodeService{
		appCtx: ctx,
	}
}
