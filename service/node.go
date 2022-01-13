package service

import (
	"context"
	"distributed-scheduler/core"
	"distributed-scheduler/errCode"
	"distributed-scheduler/proto/node"

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/protobuf/types/known/emptypb"
)

type nodeService struct {
	appCtx *core.ApplicationContext
}

// 获取主节点信息
func (s *nodeService) GetMaster(ctx context.Context, _ *empty.Empty) (*node.GetMasterResponse, error) {
	nodeInfo := s.appCtx.GetMasterNode()
	return &node.GetMasterResponse{
		NodeInfo: &node.NodeInfo{
			Endpoint: nodeInfo.Endpoint,
			NodeId:   nodeInfo.NodeId,
		},
	}, nil
}

// 心跳检测
func (s *nodeService) HeartBeat(ctx context.Context, req *node.HeartBeatRequest) (*empty.Empty, error) {
	if !s.appCtx.IsMasterNode() {
		return nil, errCode.ToGrpcErr(errCode.ErrNonMasterNode)
	}

	err := s.appCtx.UpdateHeartbeat(core.WorkerNode{
		NodeId:    req.NodeInfo.NodeId,
		Endpoint:  req.NodeInfo.Endpoint,
		PluginSet: req.PluginSet,
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func NewNodeService(ctx *core.ApplicationContext) node.NodeServiceServer {
	return &nodeService{
		appCtx: ctx,
	}
}
