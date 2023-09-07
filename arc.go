package hyliocache

import (
	"container/list"
	"time"
)

type ARCCache struct {
	baseCache
	items map[interface{}]*arcItem
	part  int
	t1    *arcList
	t2    *arcList
	b1    *arcList
	b2    *arcList
}

func newARCCache(cb *CacheBuilder) *ARCCache {
	c := &ARCCache{}
	buildCache(&c.baseCache, cb)
	c.init()
	c.group.cache = c
	return c
}

func (c *ARCCache) init() {
	c.items = make(map[interface{}]*arcItem)
	c.t1 = newArcList()
	c.t2 = newArcList()
	c.b1 = newArcList()
	c.b2 = newArcList()
}

func (c *ARCCache) Set(key, value interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.set(key, value)
	return err
}

func (c *ARCCache) SetWithExpire(key, value interface{}, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.set(key, value)
	if err != nil {
		return err
	}
	t := c.clock.Now().Add(expiration)
	item.(*arcItem).expiration = &t
	return nil
}

func (c *ARCCache) set(key, value interface{}) (interface{}, error) {
	item, ok := c.items[key]
	if ok {
		item.value = value
	} else {
		item = &arcItem{
			clock: c.clock,
			key:   key,
			value: value,
		}
		c.items[key] = item
	}
	if c.expiration != nil {
		t := c.clock.Now().Add(*c.expiration)
		item.expiration = &t
	}

	if c.t1.Has(key) && c.t2.Has(key) {
		return item, nil
	}

	if ele := c.b1.Get(key); ele != nil {
		c.setPart(min(c.size, c.part+max(c.b2.Len()/c.b1.Len(), 1)))
		c.replace(key)
		c.b1.Remove(key, ele)
		c.t2.PushFront(key)
		return item, nil
	}

	if ele := c.b2.Get(key); ele != nil {
		c.setPart(max(0, c.part-max(c.b1.Len()/c.b2.Len(), 1)))
		c.replace(key)
		c.b2.Remove(key, ele)
		c.t2.PushFront(key)
		return item, nil
	}

	if c.isCacheFull() && c.t1.Len()+c.b1.Len() == c.size {
		if c.t1.Len() < c.size {
			c.b1.RemoveTail()
			c.replace(key)
		} else {
			pop := c.t1.RemoveTail()
			item, ok := c.items[pop]
			if ok {
				delete(c.items, pop)
				if c.evictedFunc != nil {
					c.evictedFunc(item.key, item.value)
				}
			}
		}
	} else {
		total := c.t1.Len() + c.t2.Len() + c.b1.Len() + c.b2.Len()
		if total >= c.size {
			if total == (2 * c.size) {
				if c.b2.Len() > 0 {
					c.b2.RemoveTail()
				} else {
					c.b1.RemoveTail()
				}
			}
			c.replace(key)
		}
	}

	if c.addedFunc != nil {
		c.addedFunc(key, value)
	}
	c.t1.PushFront(key)
	return item, nil
}

func (c *ARCCache) Get(key interface{}) (interface{}, error) {
	item, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, true)
	}
	return item, nil
}

func (c *ARCCache) GetALL(checkExpired bool) map[interface{}]interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	items := make(map[interface{}]interface{}, len(c.items))
	now := time.Now()
	for k, item := range c.items {
		if !checkExpired || c.has(k, &now) {
			items[k] = item.value
		}
	}
	return items
}

func (c *ARCCache) GetIfPresent(key interface{}) (interface{}, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, false)
	}
	return v, nil
}

func (c *ARCCache) get(key interface{}, onLoad bool) (interface{}, error) {
	v, err := c.getValue(key, onLoad)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (c *ARCCache) getValue(key interface{}, onLoad bool) (interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ele := c.t1.Get(key); ele != nil {
		c.t1.Remove(key, ele)
		item := c.items[key]
		if !item.IsExpired(nil) {
			c.t2.PushFront(key)
			if !onLoad {
				c.stats.IncrHitCount()
			}
			return item.value, nil
		} else {
			delete(c.items, key)
			c.b1.PushFront(key)
			if c.evictedFunc != nil {
				c.evictedFunc(item.key, item.value)
			}
		}
	}
	if ele := c.t2.Get(key); ele != nil {
		item := c.items[key]
		if !item.IsExpired(nil) {
			c.t2.MoveToFront(ele)
			if !onLoad {
				c.stats.IncrHitCount()
			}
			return item.value, nil
		} else {
			delete(c.items, key)
			c.t2.Remove(key, ele)
			c.b2.PushFront(key)
			if c.evictedFunc != nil {
				c.evictedFunc(item.key, item.value)
			}
		}
	}
	if !onLoad {
		c.stats.IncrMissCount()
	}
	return nil, KeyNotFoundError
}

func (c *ARCCache) getWithLoader(key interface{}, isWait bool) (interface{}, error) {
	if c.loaderExpireFunc == nil {
		return nil, KeyNotFoundError
	}
	value, _, err := c.load(key, func(v interface{}, expiration *time.Duration, e error) (interface{}, error) {
		if e != nil {
			return nil, e
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		item, err := c.set(key, v)
		if err != nil {
			return nil, err
		}
		if expiration != nil {
			t := c.clock.Now().Add(*expiration)
			item.(*arcItem).expiration = &t
		}
		return v, nil
	}, isWait)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (c *ARCCache) Keys(checkExpired bool) []interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]interface{}, 0, len(c.items))
	now := time.Now()
	for k := range c.items {
		if !checkExpired || c.has(k, &now) {
			keys = append(keys, k)
		}
	}
	return keys
}

func (c *ARCCache) Len(checkExpired bool) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !checkExpired {
		return len(c.items)
	}
	length := 0
	now := time.Now()
	for k := range c.items {
		if c.has(k, &now) {
			length++
		}
	}
	return length
}

func (c *ARCCache) Has(key interface{}) bool {
	c.mu.RLock()
	defer c.mu.Unlock()
	now := time.Now()
	return c.has(key, &now)
}

func (c *ARCCache) has(key interface{}, now *time.Time) bool {
	it, ok := c.items[key]
	if !ok {
		return false
	}
	return !it.IsExpired(now)
}

func (c *ARCCache) Remove(key interface{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.remove(key)
}

func (c *ARCCache) remove(key interface{}) bool {
	if elt := c.t1.Get(key); elt != nil {
		c.t1.Remove(key, elt)
		item := c.items[key]
		delete(c.items, key)
		c.b1.PushFront(key)
		if c.evictedFunc != nil {
			c.evictedFunc(key, item.value)
		}
		return true
	}

	if elt := c.t2.Get(key); elt != nil {
		c.t2.Remove(key, elt)
		item := c.items[key]
		delete(c.items, key)
		c.b2.PushFront(key)
		if c.evictedFunc != nil {
			c.evictedFunc(key, item.value)
		}
		return true
	}

	return false
}

func (c *ARCCache) setPart(p int) {
	if c.isCacheFull() {
		c.part = p
	}
}

func (c *ARCCache) isCacheFull() bool {
	return (c.t1.Len() + c.t2.Len()) == c.size
}

func (c *ARCCache) replace(key interface{}) {
	if !c.isCacheFull() {
		return
	}
	var old interface{}
	if c.t1.Len() > 0 && ((c.b2.Has(key) && c.t1.Len() == c.part) || (c.t1.Len() > c.part)) {
		old = c.t1.RemoveTail()
		c.b1.PushFront(old)
	} else if c.t2.Len() > 0 {
		old = c.t2.RemoveTail()
		c.b2.PushFront(old)
	} else {
		old = c.t1.RemoveTail()
		c.b1.PushFront(old)
	}
	item, ok := c.items[old]
	if ok {
		delete(c.items, old)
		if c.evictedFunc != nil {
			c.evictedFunc(item.key, item.value)
		}
	}
}

// arc链表的定义 链表的list的element中只存了ARCCache.key
type arcList struct {
	l    *list.List
	keys map[interface{}]*list.Element
}

func newArcList() *arcList {
	return &arcList{
		l:    list.New(),
		keys: make(map[interface{}]*list.Element),
	}
}

func (a *arcList) Has(key interface{}) bool {
	_, ok := a.keys[key]
	return ok
}

func (a *arcList) Get(key interface{}) *list.Element {
	ele := a.keys[key]
	return ele
}

func (a *arcList) MoveToFront(ele *list.Element) {
	a.l.MoveToFront(ele)
}

func (a *arcList) PushFront(key interface{}) {
	if ele, ok := a.keys[key]; ok {
		a.l.MoveToFront(ele)
		return
	}
	ele := a.l.PushFront(key)
	a.keys[key] = ele
}

func (a *arcList) Remove(key interface{}, ele *list.Element) {
	delete(a.keys, key)
	a.l.Remove(ele)
}

func (a *arcList) RemoveTail() interface{} {
	ele := a.l.Back()
	a.l.Remove(ele)
	key := ele.Value
	delete(a.keys, key)
	return key
}

func (a *arcList) Len() int {
	return a.l.Len()
}

type arcItem struct {
	clock      Clock
	key        interface{}
	value      interface{}
	expiration *time.Time
}

func (it *arcItem) IsExpired(now *time.Time) bool {
	if it.expiration == nil {
		return false
	}
	if now == nil {
		t := it.clock.Now()
		now = &t
	}
	return it.expiration.Before(*now)
}
