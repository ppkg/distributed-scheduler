package core

type tokenBucket struct {
	bucket chan *LimitRateToken
	data   map[*LimitRateToken]struct{}
}

func (s *tokenBucket) Take() *LimitRateToken {
	return <-s.bucket
}

func (s *tokenBucket) Put(token *LimitRateToken) bool {
	if token == nil {
		return false
	}
	if _, ok := s.data[token]; !ok {
		return false
	}
	s.bucket <- token
	return true
}

func NewTokenBucket(size int) *tokenBucket {
	instance := &tokenBucket{
		bucket: make(chan *LimitRateToken, size),
		data:   make(map[*LimitRateToken]struct{}),
	}
	for i := 0; i < size; i++ {
		item := &LimitRateToken{}
		instance.bucket <- item
		instance.data[item] = struct{}{}
	}
	return instance
}

type LimitRateToken struct {
}
