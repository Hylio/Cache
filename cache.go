package hyliocache

/*
cache 模块负责提供缓存的并发控制
*/

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	TypeSimple = "simple"
	TypeLru    = "lru"
	TypeLfu    = "lfu"
	TypeArc    = "arc"
)

var KeyNotFoundError = errors.New("key not found")

type Cache interface {
	Set(key, value interface{}) error
	SetWithExpire(key, value interface{}, expiration time.Duration) error
	Get(key interface{}) (interface{}, error)
	GetALL(checkExpired bool) map[interface{}]interface{}
	GetIfPresent(key interface{}) (interface{}, error)
	get(key interface{}, onLoad bool) (interface{}, error)
	Keys(checkExpired bool) []interface{}
	Len(checkExpired bool) int
	Has(key interface{}) bool
	Remove(key interface{}) bool
	statsAccessor
}

type (
	LoaderFunc       func(interface{}) (interface{}, error)
	LoaderExpireFunc func(interface{}) (interface{}, *time.Duration, error)
	EvictedFunc      func(interface{}, interface{})
	AddedFunc        func(interface{}, interface{})
)

type baseCache struct {
	clock            Clock            // 时间接口
	size             int              // 缓存容量
	loaderExpireFunc LoaderExpireFunc // 带有过期时间的加载器函数
	evictedFunc      EvictedFunc      // 元素被清理时触发的回调函数
	addedFunc        AddedFunc        // 元素被添加时触发的回调函数
	expiration       *time.Duration   // 过期时间
	mu               sync.RWMutex     // 读写锁
	group            Group            // singleFlight
	*stats
}

func (c *baseCache) load(key interface{}, cb func(interface{}, *time.Duration, error) (interface{}, error), isWait bool) (interface{}, bool, error) {
	v, called, err := c.group.Do(key, func() (v interface{}, e error) {
		defer func() {
			if r := recover(); r != nil {
				e = fmt.Errorf("loader panics: %v", r)
			}
		}()
		return cb(c.loaderExpireFunc(key))
	}, isWait)
	if err != nil {
		return nil, called, err
	}
	return v, called, nil
}

// 使用建造者模式

type CacheBuilder struct {
	clock            Clock
	tp               string
	size             int
	loaderExpireFunc LoaderExpireFunc
	evictedFunc      EvictedFunc
	addedFunc        AddedFunc
	expiration       *time.Duration
}

func New(size int) *CacheBuilder {
	return &CacheBuilder{
		clock: NewRealClock(),
		tp:    TypeSimple,
		size:  size,
	}
}

func (c *CacheBuilder) Clock(clock Clock) *CacheBuilder {
	c.clock = clock
	return c
}

func (c *CacheBuilder) EvictType(tp string) *CacheBuilder {
	c.tp = tp
	return c
}

func (c *CacheBuilder) Simple() *CacheBuilder {
	return c.EvictType(TypeSimple)
}

func (c *CacheBuilder) LRU() *CacheBuilder {
	return c.EvictType(TypeLru)
}

func (c *CacheBuilder) LFU() *CacheBuilder {
	return c.EvictType(TypeLfu)
}

func (c *CacheBuilder) ARC() *CacheBuilder {
	return c.EvictType(TypeArc)
}

// LoaderFunc 当一个元素把另一个元素挤出缓存的时候 调用该函数
func (c *CacheBuilder) LoaderFunc(loaderFunc LoaderFunc) *CacheBuilder {
	c.loaderExpireFunc = func(k interface{}) (interface{}, *time.Duration, error) {
		v, err := loaderFunc(k)
		return v, nil, err
	}
	return c
}

func (c *CacheBuilder) LoaderExpireFunc(expireFunc LoaderExpireFunc) *CacheBuilder {
	c.loaderExpireFunc = expireFunc
	return c
}

func (c *CacheBuilder) EvictedFunc(evictedFunc EvictedFunc) *CacheBuilder {
	c.evictedFunc = evictedFunc
	return c
}

func (c *CacheBuilder) AddedFunc(addedFunc AddedFunc) *CacheBuilder {
	c.addedFunc = addedFunc
	return c
}

func (c *CacheBuilder) Expiration(expiration time.Duration) *CacheBuilder {
	c.expiration = &expiration
	return c
}

func (c *CacheBuilder) Build() Cache {
	if c.size <= 0 && c.tp != TypeSimple {
		panic("cache size <= 0")
	}
	return c.build()
}

func (c *CacheBuilder) build() Cache {
	switch c.tp {
	case TypeSimple:
		return newSimpleCache(c)
	case TypeLfu:
		return newLFUCache(c)
	case TypeLru:
		return newLRUCache(c)
	case TypeArc:
		return newARCCache(c)
	default:
		panic("Unknown type")
	}
}

func buildCache(c *baseCache, cb *CacheBuilder) {
	c.clock = cb.clock
	c.size = cb.size
	c.loaderExpireFunc = cb.loaderExpireFunc
	c.expiration = cb.expiration
	c.evictedFunc = cb.evictedFunc
	c.addedFunc = cb.addedFunc
	c.stats = &stats{}
}
