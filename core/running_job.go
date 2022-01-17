package core

import (
	"context"
	"sync"

	"github.com/ppkg/distributed-scheduler/dto"
)

type runningJobContainer struct {
	data map[int64]*RunningJobItem
	lock sync.RWMutex
}

type RunningJobItem struct {
	Ctx context.Context
	Job *dto.JobInfo
}

func NewJobContainer() *runningJobContainer {
	return &runningJobContainer{
		data: make(map[int64]*RunningJobItem),
	}
}

func (s *runningJobContainer) Put(id int64, job *RunningJobItem) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.data[id] = job
}

func (s *runningJobContainer) Get(id int64) (*RunningJobItem, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	val, ok := s.data[id]
	return val, ok
}

func (s *runningJobContainer) GetAll() []*RunningJobItem {
	s.lock.RLock()
	defer s.lock.RUnlock()
	list := make([]*RunningJobItem, 0, len(s.data))
	for _, v := range s.data {
		list = append(list, v)
	}
	return list
}

func (s *runningJobContainer) Remove(id int64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.data, id)
}

func (s *runningJobContainer) RemoveAll() {
	s.lock.Lock()
	defer s.lock.Unlock()
	for k := range s.data {
		delete(s.data, k)
	}
}
