package core

import (
	"distributed-scheduler/dto"
	"sync"
)

type workerNotifyChannel struct {
	data map[string]chan *dto.JobInfo
	lock sync.RWMutex
}

func NewWorkerNotifyChannel() *workerNotifyChannel {
	return &workerNotifyChannel{
		data: make(map[string]chan *dto.JobInfo),
	}
}

func (s *workerNotifyChannel) RegisterChannel(nodeId string, channel chan *dto.JobInfo) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.data[nodeId] = channel
}

func (s *workerNotifyChannel) GetChannel(nodeId string) (chan *dto.JobInfo, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	val, ok := s.data[nodeId]
	return val, ok
}

func (s *workerNotifyChannel) RemoveAndCloseChannel(nodeId string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	channel, ok := s.data[nodeId]
	if !ok {
		return
	}
	delete(s.data, nodeId)
	close(channel)
}

func (s *workerNotifyChannel) GetAll() []chan *dto.JobInfo {
	s.lock.RLock()
	defer s.lock.RUnlock()
	list := make([]chan *dto.JobInfo, 0, len(s.data))
	for _, item := range s.data {
		list = append(list, item)
	}
	return list
}