package core

import (
	"sync"
)

// worker缓存组件
type workerIndexer struct {
	lock    sync.RWMutex
	data    map[string]hashSet
	workers workerMap
}

func (s *workerIndexer) AddWorker(worker WorkerNode, keySet []string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.workers[worker.NodeId] = worker

	for _, key := range keySet {
		val, ok := s.data[key]
		if !ok {
			val = make(hashSet)
		}
		val[worker.NodeId] = struct{}{}
		s.data[key] = val
	}
}

func (s *workerIndexer) RemoveWorker(nodeId string, keySet []string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.workers[nodeId]; !ok {
		return
	}
	delete(s.workers, nodeId)
	for _, key := range keySet {
		if val, ok := s.data[key]; ok {
			delete(val, nodeId)
		}
	}
}

func (s *workerIndexer) UpdateWorker(worker WorkerNode, newKeySet, delKeySet []string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.workers[worker.NodeId] = worker
	for _, key := range delKeySet {
		if val, ok := s.data[key]; ok {
			delete(val, worker.NodeId)
		}
	}
	for _, key := range newKeySet {
		val, ok := s.data[key]
		if !ok {
			val = make(hashSet)
		}
		val[worker.NodeId] = struct{}{}
		s.data[key] = val
	}
}

func (s *workerIndexer) ListWorker(key string) []WorkerNode {
	s.lock.RLock()
	defer s.lock.RUnlock()
	nodeIds, ok := s.data[key]
	if !ok {
		return nil
	}
	list := make([]WorkerNode, 0, len(nodeIds))
	for nodeId := range nodeIds {
		list = append(list, s.workers[nodeId])
	}
	return list
}

func (s *workerIndexer) GetWorker(nodeId string) (WorkerNode, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	node, ok := s.workers[nodeId]
	return node, ok
}

func (s *workerIndexer) GetAllWorker() []WorkerNode {
	s.lock.RLock()
	defer s.lock.RUnlock()
	list := make([]WorkerNode, 0, len(s.workers))
	for _, item := range s.workers {
		list = append(list, item)
	}
	return list
}

// 工作节点
type WorkerNode struct {
	NodeId   string
	Endpoint string
	// 工作节点支持插件集合,多个以","隔开
	PluginSet []string
	// 工作节点支持job回调通知，多个以","隔开
	JobNotifySet []string
}

type workerMap map[string]WorkerNode

type hashSet map[string]struct{}

func NewWorkerIndexer() *workerIndexer {
	return &workerIndexer{
		data:    make(map[string]hashSet),
		workers: make(workerMap),
	}
}
