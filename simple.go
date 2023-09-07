package hyliocache

import (
	"time"
)

type SimpleCache struct {
	baseCache
	items map[interface{}]*simpleItem
}

func newSimpleCache(cb *CacheBuilder) *SimpleCache {
	c := &SimpleCache{}
	buildCache(&c.baseCache, cb)
	c.init()
	c.group.cache = c
	return c
}

func (sc *SimpleCache) init() {
	if sc.size <= 0 {
		sc.items = make(map[interface{}]*simpleItem)
	} else {
		sc.items = make(map[interface{}]*simpleItem, sc.size)
	}
}

func (sc *SimpleCache) Set(key, value interface{}) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	_, err := sc.set(key, value)
	return err
}

func (sc *SimpleCache) SetWithExpire(key, value interface{}, expiration time.Duration) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	item, err := sc.set(key, value)
	if err != nil {
		return err
	}
	t := sc.clock.Now().Add(expiration)
	item.(*simpleItem).expiration = &t
	return nil
}

func (sc *SimpleCache) set(key, value interface{}) (interface{}, error) {
	item, ok := sc.items[key]
	if ok {
		item.value = value
	} else {
		if len(sc.items) >= sc.size && sc.size > 0 {
			sc.evict(1)
		}
		item = &simpleItem{
			clock: sc.clock,
			value: value,
		}
		sc.items[key] = item
	}

	if sc.expiration != nil {
		t := sc.clock.Now().Add(*sc.expiration)
		item.expiration = &t
	}
	if sc.addedFunc != nil {
		sc.addedFunc(key, value)
	}
	return item, nil
}

// 进行内存淘汰
func (sc *SimpleCache) evict(count int) {
	now := sc.clock.Now()
	for key, item := range sc.items {
		if count <= 0 {
			return
		}
		if item.expiration == nil || now.After(*item.expiration) {
			defer sc.remove(key)
			count--
		}
	}
}

func (sc *SimpleCache) remove(key interface{}) bool {
	item, ok := sc.items[key]
	if ok {
		delete(sc.items, key)
		if sc.evictedFunc != nil {
			sc.evictedFunc(key, item.value)
		}
		return true
	}
	return false
}

func (sc *SimpleCache) Get(key interface{}) (interface{}, error) {
	v, err := sc.get(key, false)
	if err == KeyNotFoundError {
		return sc.getWithLoader(key, true)
	}
	return v, err
}

func (sc *SimpleCache) GetIfPresent(key interface{}) (interface{}, error) {
	v, err := sc.get(key, false)
	if err == KeyNotFoundError {
		return sc.getWithLoader(key, false)
	}
	return v, nil
}

func (sc *SimpleCache) get(key interface{}, onLoad bool) (interface{}, error) {
	v, err := sc.getValue(key, onLoad)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (sc *SimpleCache) getValue(key interface{}, onload bool) (interface{}, error) {
	sc.mu.Lock()
	item, ok := sc.items[key]
	if ok {
		if !item.IsExpired(nil) {
			v := item.value
			sc.mu.Unlock()
			if !onload {
				sc.stats.IncrHitCount()
			}
			return v, nil
		}
		sc.remove(key)
	}
	sc.mu.Unlock()
	if !onload {
		sc.stats.IncrMissCount()
	}
	return nil, KeyNotFoundError
}

func (sc *SimpleCache) getWithLoader(key interface{}, isWait bool) (interface{}, error) {
	if sc.loaderExpireFunc == nil {
		return nil, KeyNotFoundError
	}
	value, _, err := sc.load(key, func(v interface{}, expiration *time.Duration, e error) (interface{}, error) {
		if e != nil {
			return nil, e
		}
		sc.mu.Lock()
		defer sc.mu.Unlock()
		item, err := sc.set(key, v)
		if err != nil {
			return nil, err
		}
		if expiration != nil {
			t := sc.clock.Now().Add(*expiration)
			item.(*simpleItem).expiration = &t
		}
		return v, nil
	}, isWait)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (sc *SimpleCache) GetALL(checkExpired bool) map[interface{}]interface{} {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	items := make(map[interface{}]interface{}, len(sc.items))
	now := time.Now()
	for k, item := range sc.items {
		if !checkExpired || sc.has(k, &now) {
			items[k] = item.value
		}
	}
	return items
}

func (sc *SimpleCache) Remove(key interface{}) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.remove(key)
}

func (sc *SimpleCache) Keys(checkExpired bool) []interface{} {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	keys := make([]interface{}, 0, len(sc.items))
	now := time.Now()
	for k := range sc.items {
		if !checkExpired || sc.has(k, &now) {
			keys = append(keys, k)
		}
	}
	return keys
}

func (sc *SimpleCache) Len(checkExpired bool) int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	if !checkExpired {
		return len(sc.items)
	}
	var length int
	now := time.Now()
	for k := range sc.items {
		if sc.has(k, &now) {
			length++
		}
	}
	return length
}

func (sc *SimpleCache) Has(key interface{}) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	now := time.Now()
	return sc.has(key, &now)
}

func (sc *SimpleCache) has(key interface{}, now *time.Time) bool {
	item, ok := sc.items[key]
	if !ok {
		return false
	}
	return !item.IsExpired(now)
}

type simpleItem struct {
	clock      Clock
	value      interface{}
	expiration *time.Time
}

func (s *simpleItem) IsExpired(now *time.Time) bool {
	if s.expiration == nil {
		return false
	}
	if now == nil {
		t := s.clock.Now()
		now = &t
	}
	return s.expiration.Before(*now)
}
