package service

import (
	"context"
	"errors"

	"github.com/ppkg/distributed-scheduler/core"
	"github.com/ppkg/distributed-scheduler/proto/node"

	"github.com/golang/protobuf/ptypes/empty"
)

type nodeService struct {
	appCtx *core.ApplicationContext
}

// 获取主节点信息
func (s *nodeService) GetLeader(ctx context.Context, _ *empty.Empty) (*node.GetLeaderResponse, error) {
	endpoit := s.appCtx.GetLeaderNode()
	if endpoit == "" {
		return nil, errors.New("调度器leader节点正在选举中")
	}
	return &node.GetLeaderResponse{
		NodeInfo: &node.NodeInfo{
			Endpoint: endpoit,
		},
	}, nil
}

func NewNodeService(ctx *core.ApplicationContext) node.NodeServiceServer {
	return &nodeService{
		appCtx: ctx,
	}
}
