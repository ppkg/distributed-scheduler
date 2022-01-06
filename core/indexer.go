package core

import (
	"strings"
	"sync"
)

// worker缓存组件
type WorkerIndexer struct {
	lock      sync.RWMutex
	pipelines map[string]workerSet
	workers   workerSet
}

func (s *WorkerIndexer) AddWorker(worker WorkerNode) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.workers[worker] = struct{}{}

	for _, pipeline := range strings.Split(worker.PipelineSet, ",") {
		val, ok := s.pipelines[pipeline]
		if !ok {
			val = make(workerSet)
		}
		val[worker] = struct{}{}
		s.pipelines[pipeline] = val
	}
}

func (s *WorkerIndexer) RemoveWorker(worker WorkerNode) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.workers[worker]; !ok {
		return
	}
	delete(s.workers, worker)
	for _, pipeline := range strings.Split(worker.PipelineSet, ",") {
		if val, ok := s.pipelines[pipeline]; ok {
			delete(val, worker)
		}
	}
}

func (s *WorkerIndexer) GetPipelineWorker(key string) []WorkerNode {
	s.lock.RLocker().Lock()
	defer s.lock.RLocker().Unlock()
	workerSet, ok := s.pipelines[key]
	if !ok {
		return nil
	}
	list := make([]WorkerNode, 0, len(workerSet))
	for worker := range workerSet {
		list = append(list, worker)
	}
	return list
}

// 工作节点
type WorkerNode struct {
	NodeId string
	Url    string
	// 支持管道集合,多个以","隔开
	PipelineSet string
}

type workerSet map[WorkerNode]struct{}

func NewWorkerIndexer() *WorkerIndexer {
	return &WorkerIndexer{
		pipelines: make(map[string]workerSet),
		workers:   make(workerSet),
	}
}
