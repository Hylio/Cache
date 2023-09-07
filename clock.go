package hyliocache

import (
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
}

type RealClock struct {
}

func NewRealClock() Clock {
	return RealClock{}
}

func (r RealClock) Now() time.Time {
	return time.Now()
}

type FakeClock interface {
	Clock
	Advance(d time.Duration)
}

func NewFakeClock() FakeClock {
	return &fakeclock{}
}

type fakeclock struct {
	now   time.Time
	mutex sync.RWMutex
}

func (f *fakeclock) Now() time.Time {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.now
}

func (f *fakeclock) Advance(d time.Duration) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.now = f.now.Add(d)
}
