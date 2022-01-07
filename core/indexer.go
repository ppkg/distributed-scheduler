package core

import (
	"strings"
	"sync"
)

// worker缓存组件
type WorkerIndexer struct {
	lock      sync.RWMutex
	pipelines map[string]nodeIdSet
	workers   workerMap
}

func (s *WorkerIndexer) AddWorker(worker WorkerNode) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.workers[worker.NodeId] = worker

	for _, pipeline := range strings.Split(worker.PipelineSet, ",") {
		val, ok := s.pipelines[pipeline]
		if !ok {
			val = make(nodeIdSet)
		}
		val[worker.NodeId] = struct{}{}
		s.pipelines[pipeline] = val
	}
}

func (s *WorkerIndexer) RemoveWorker(worker WorkerNode) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.workers[worker.NodeId]; !ok {
		return
	}
	delete(s.workers, worker.NodeId)
	for _, pipeline := range strings.Split(worker.PipelineSet, ",") {
		if val, ok := s.pipelines[pipeline]; ok {
			delete(val, worker.NodeId)
		}
	}
}

func (s *WorkerIndexer) GetPipelineWorker(pipelineKey string) []WorkerNode {
	s.lock.RLock()
	defer s.lock.RUnlock()
	nodeIds, ok := s.pipelines[pipelineKey]
	if !ok {
		return nil
	}
	list := make([]WorkerNode, 0, len(nodeIds))
	for nodeId := range nodeIds {
		list = append(list, s.workers[nodeId])
	}
	return list
}

func (s *WorkerIndexer) GetWorker(nodeId string) (WorkerNode, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	node, ok := s.workers[nodeId]
	return node, ok
}

// 工作节点
type WorkerNode struct {
	NodeId string
	Endpoint    string
	// 支持管道集合,多个以","隔开
	PipelineSet string
}

type workerMap map[string]WorkerNode

type nodeIdSet map[string]struct{}

func NewWorkerIndexer() *WorkerIndexer {
	return &WorkerIndexer{
		pipelines: make(map[string]nodeIdSet),
		workers:   make(workerMap),
	}
}
