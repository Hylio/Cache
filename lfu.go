package hyliocache

import (
	"container/list"
	"time"
)

type LFUCache struct {
	baseCache
	items    map[interface{}]*lfuItem
	freqList *list.List
}

func newLFUCache(cb *CacheBuilder) *LFUCache {
	c := &LFUCache{}
	buildCache(&c.baseCache, cb)
	c.init()
	c.group.cache = c
	return c
}

func (L *LFUCache) init() {
	L.freqList = list.New()
	L.items = make(map[interface{}]*lfuItem)
	L.freqList.PushFront(&freqEntry{
		freq:  0,
		items: make(map[*lfuItem]struct{}),
	})
}

func (L *LFUCache) Set(key, value interface{}) error {
	L.mu.Lock()
	defer L.mu.Unlock()
	_, err := L.set(key, value)
	return err
}

func (L *LFUCache) SetWithExpire(key, value interface{}, expiration time.Duration) error {
	L.mu.Lock()
	defer L.mu.Unlock()
	item, err := L.set(key, value)
	if err != nil {
		return err
	}
	t := L.clock.Now().Add(expiration)
	item.(*lfuItem).expiration = &t
	return nil
}

func (L *LFUCache) set(key, value interface{}) (interface{}, error) {
	item, ok := L.items[key]
	if ok {
		item.value = value
	} else {
		if len(L.items) >= L.size {
			L.evict(1)
		}
		item = &lfuItem{
			clock:       L.clock,
			key:         key,
			value:       value,
			freqElement: nil,
		}
		// 直接加到了freqList的最前面
		// todo: WHY
		head := L.freqList.Front()
		entry := head.Value.(*freqEntry)
		entry.items[item] = struct{}{}
		item.freqElement = head
		L.items[key] = item
	}
	if L.expiration != nil {
		t := L.clock.Now().Add(*L.expiration)
		item.expiration = &t
	}
	if L.addedFunc != nil {
		L.addedFunc(key, value)
	}
	return item, nil
}

func (L *LFUCache) Get(key interface{}) (interface{}, error) {
	v, err := L.get(key, false)
	if err == KeyNotFoundError {
		return L.getWithLoader(key, true)
	}
	return v, err
}

func (L *LFUCache) GetIfPresent(key interface{}) (interface{}, error) {
	v, err := L.get(key, false)
	if err == KeyNotFoundError {
		return L.getWithLoader(key, false)
	}
	return v, err
}

func (L *LFUCache) get(key interface{}, onLoad bool) (interface{}, error) {
	v, err := L.getValue(key, onLoad)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (L *LFUCache) getWithLoader(key interface{}, isWait bool) (interface{}, error) {
	if L.loaderExpireFunc == nil {
		return nil, KeyNotFoundError
	}
	value, _, err := L.load(key, func(v interface{}, expiration *time.Duration, e error) (interface{}, error) {
		if e != nil {
			return nil, e
		}
		L.mu.Lock()
		defer L.mu.Unlock()
		item, err := L.set(key, v)
		if err != nil {
			return nil, err
		}
		if expiration != nil {
			t := L.clock.Now().Add(*expiration)
			item.(*lfuItem).expiration = &t
		}
		return v, nil
	}, isWait)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (L *LFUCache) getValue(key interface{}, onLoad bool) (interface{}, error) {
	L.mu.Lock()
	item, ok := L.items[key]
	if ok {
		if !item.IsExpired(nil) {
			L.increment(item)
			v := item.value
			L.mu.Unlock()
			if !onLoad {
				L.stats.IncrHitCount()
			}
			return v, nil
		}
		L.removeItem(item)
	}
	L.mu.Unlock()
	if !onLoad {
		L.stats.IncrMissCount()
	}
	return nil, KeyNotFoundError
}

// increment 增加item的freq
func (L *LFUCache) increment(item *lfuItem) {
	currentFreqElement := item.freqElement
	currentFreqEntry := currentFreqElement.Value.(*freqEntry)
	nextFreq := currentFreqEntry.freq + 1
	delete(currentFreqEntry.items, item)

	removable := isRemovableFreqEntry(currentFreqEntry)
	nextFreqElement := currentFreqElement.Next()
	switch {
	case nextFreqElement == nil || nextFreqElement.Value.(*freqEntry).freq > nextFreq:
		// 如果链表中没有freq对应的元素
		// 就把当前entry直接作为下一个freq对应的结点
		// 或者新建一个结点
		if removable {
			currentFreqEntry.freq = nextFreq
			nextFreqElement = currentFreqElement
		} else {
			nextFreqElement = L.freqList.InsertAfter(&freqEntry{
				freq:  nextFreq,
				items: make(map[*lfuItem]struct{}),
			}, currentFreqElement)
		}
	case nextFreqElement.Value.(*freqEntry).freq == nextFreq:
		// 如果有就直接加过去
		// 把当前结点删除
		if removable {
			L.freqList.Remove(currentFreqElement)
		}
	default:
		panic("LFU freq element unreachable")
	}
	nextFreqElement.Value.(*freqEntry).items[item] = struct{}{}
	item.freqElement = nextFreqElement
}

func (L *LFUCache) Keys(checkExpired bool) []interface{} {
	L.mu.RLock()
	defer L.mu.RUnlock()
	keys := make([]interface{}, 0, len(L.items))
	now := time.Now()
	for key := range L.items {
		if !checkExpired || L.has(key, &now) {
			keys = append(keys, key)
		}
	}
	return keys
}

func (L *LFUCache) Remove(key interface{}) bool {
	L.mu.Lock()
	defer L.mu.Unlock()
	return L.remove(key)
}

func (L *LFUCache) remove(key interface{}) bool {
	if item, ok := L.items[key]; ok {
		L.removeItem(item)
		return true
	}
	return false
}

func (L *LFUCache) GetALL(checkExpired bool) map[interface{}]interface{} {
	L.mu.RLock()
	defer L.mu.RUnlock()
	items := make(map[interface{}]interface{}, len(L.items))
	now := time.Now()
	for k, item := range L.items {
		if !checkExpired || L.has(k, &now) {
			items[k] = item.value
		}
	}
	return items
}

func (L *LFUCache) Len(checkExpired bool) int {
	L.mu.RLock()
	defer L.mu.RUnlock()
	if !checkExpired {
		return len(L.items)
	}
	length := 0
	now := time.Now()
	for _, item := range L.items {
		if !item.IsExpired(&now) {
			length++
		}
	}
	return length
}

func (L *LFUCache) Has(key interface{}) bool {
	L.mu.Lock()
	defer L.mu.Unlock()
	now := time.Now()
	return L.has(key, &now)
}

func (L *LFUCache) has(key interface{}, now *time.Time) bool {
	item, ok := L.items[key]
	if !ok {
		return false
	}
	return !item.IsExpired(now)
}

func (L *LFUCache) evict(count int) {
	entry := L.freqList.Front()
	for i := 0; i < count; {
		if entry == nil {
			return
		} else {
			for item := range entry.Value.(*freqEntry).items {
				// 同一个优先级随机淘汰
				if i >= count {
					return
				}
				L.removeItem(item)
				i++
			}
			entry = entry.Next()
		}
	}
}

func (L *LFUCache) removeItem(item *lfuItem) {
	entry := item.freqElement.Value.(*freqEntry)
	delete(L.items, item.key)
	delete(entry.items, item)
	if isRemovableFreqEntry(entry) {
		L.freqList.Remove(item.freqElement)
	}
	if L.evictedFunc != nil {
		L.evictedFunc(item.key, item.value)
	}
}

type lfuItem struct {
	clock       Clock
	key         interface{} // ?
	value       interface{}
	freqElement *list.Element
	expiration  *time.Time
}

func (it *lfuItem) IsExpired(now *time.Time) bool {
	if it.expiration == nil {
		return false
	}
	if now == nil {
		t := it.clock.Now()
		now = &t
	}
	return it.expiration.Before(*now)
}

type freqEntry struct {
	freq  uint
	items map[*lfuItem]struct{}
}

// isRemovableFreqEntry 判断一个entry是否已经没用了
func isRemovableFreqEntry(entry *freqEntry) bool {
	return entry.freq != 0 && len(entry.items) == 0
}
