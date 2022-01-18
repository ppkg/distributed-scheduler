package dto

import "sync/atomic"

// 并发安全计数器，支持可关闭
type CountDownLatch struct {
	count   int
	ch      chan struct{}
	isClose int32
}

func (s *CountDownLatch) Wait() {
	for range s.ch {
		s.count--
		if s.count <= 0 {
			break
		}
	}
}

func (s *CountDownLatch) Done() {
	if s.isClose == 1 {
		return
	}
	s.ch <- struct{}{}
}

func (s *CountDownLatch) Close() {
	if atomic.CompareAndSwapInt32(&s.isClose, 0, 1) {
		close(s.ch)
	}
}

func NewCountDownLatch(count int) *CountDownLatch {
	instance := &CountDownLatch{
		count: count,
		ch:    make(chan struct{}, count),
	}
	if count <= 0 {
		instance.isClose = 1
		close(instance.ch)
	}
	return instance
}
