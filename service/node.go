package service

import (
	"context"
	"distributed-scheduler/core"
	"distributed-scheduler/proto/node"

	"github.com/golang/protobuf/ptypes/empty"
)

type nodeService struct {
	appCtx *core.ApplicationContext
}

// 获取主节点信息
func (s *nodeService) GetLeader(ctx context.Context, _ *empty.Empty) (*node.GetLeaderResponse, error) {
	nodeInfo := s.appCtx.GetLeaderNode()
	return &node.GetLeaderResponse{
		NodeInfo: &node.NodeInfo{
			Endpoint: nodeInfo.Endpoint,
			NodeId:   nodeInfo.NodeId,
		},
	}, nil
}

func NewNodeService(ctx *core.ApplicationContext) node.NodeServiceServer {
	return &nodeService{
		appCtx: ctx,
	}
}
