package hyliocache

import "sync/atomic"

type statsAccessor interface {
	HitCount() uint64
	MissCount() uint64
	LookupCount() uint64
	HitRate() float64
}

/*
cache的统计数据
包括命中次数和miss次数
*/

type stats struct {
	hitCount  uint64
	missCount uint64
}

// IncrHitCount increment hit count
func (s *stats) IncrHitCount() uint64 {
	return atomic.AddUint64(&s.hitCount, 1)
}

// IncrMissCount increment hit count
func (s *stats) IncrMissCount() uint64 {
	return atomic.AddUint64(&s.missCount, 1)
}

// HitCount returns hit count
func (s *stats) HitCount() uint64 {
	return atomic.LoadUint64(&s.hitCount)
}

// MissCount returns miss count
func (s *stats) MissCount() uint64 {
	return atomic.LoadUint64(&s.missCount)
}

// LookupCount returns lookup count
func (s *stats) LookupCount() uint64 {
	return s.HitCount() + s.MissCount()
}

func (s *stats) HitRate() float64 {
	hc, mc := s.HitCount(), s.MissCount()
	total := hc + mc
	if total == 0 {
		return 0.0
	}
	return float64(hc) / float64(total)
}
