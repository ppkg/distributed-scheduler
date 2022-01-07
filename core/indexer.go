package core

import (
	"sync"
)

// worker缓存组件
type WorkerIndexer struct {
	lock    sync.RWMutex
	plugins map[string]nodeIdSet
	workers workerMap
}

func (s *WorkerIndexer) AddWorker(worker WorkerNode) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.workers[worker.NodeId] = worker

	for _, plugin := range worker.PluginSet {
		val, ok := s.plugins[plugin]
		if !ok {
			val = make(nodeIdSet)
		}
		val[worker.NodeId] = struct{}{}
		s.plugins[plugin] = val
	}
}

func (s *WorkerIndexer) RemoveWorker(worker WorkerNode) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.workers[worker.NodeId]; !ok {
		return
	}
	delete(s.workers, worker.NodeId)
	for _, plugin := range worker.PluginSet {
		if val, ok := s.plugins[plugin]; ok {
			delete(val, worker.NodeId)
		}
	}
}

func (s *WorkerIndexer) GetPluginWorker(pluginKey string) []WorkerNode {
	s.lock.RLock()
	defer s.lock.RUnlock()
	nodeIds, ok := s.plugins[pluginKey]
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
	NodeId   string
	Endpoint string
	// 工作节点支持插件集合,多个以","隔开
	PluginSet []string
}

type workerMap map[string]WorkerNode

type nodeIdSet map[string]struct{}

func NewWorkerIndexer() *WorkerIndexer {
	return &WorkerIndexer{
		plugins: make(map[string]nodeIdSet),
		workers: make(workerMap),
	}
}