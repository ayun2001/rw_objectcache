package rw_objectcache

import (
	"errors"
	"runtime"
	"sync"
	"time"
)

const (
	DefaultExpireSeconds          = 30
	DefaultAutoCleanupSeconds     = DefaultExpireSeconds * 2
	ErrIntReturnNum           int = -2147483600
	NeverExpireTime           int = -1
	DefaultExpireTime         int = 0
)

const (
	TypeExpiredElement = iota + 1
	TypeNeverExpiredElement
	TypeNoValidElement
	TypeValidElement
)

var (
	ErrElementAlreadyExist = errors.New("Element already exists.")
	ErrElementDoNotExist   = errors.New("Element do not exists.")
)

///=================================================================================================
///
///	对象缓存实现
///

type cacheElement struct {
	object   interface{}
	expireAt uint32
}

func (el cacheElement) IsExpired() bool {
	if el.expireAt == 0 {
		return false
	}
	return uint32(time.Now().Unix()) >= el.expireAt
}

type janitor struct {
	intervalTime time.Duration
	stop         chan bool
}

type evictedElementKeyAndObject struct {
	key   string
	value interface{}
}

type ObjectCache struct {
	elements              map[string]cacheElement
	lock                  *sync.RWMutex
	onEvicted             func(string, interface{})
	janitorPtr            *janitor
	enableEvictedCallback bool
	defaultExpireSeconds  uint32
}

//只会获取有效的数据，不包含过期但未删除的数据 (严格模式)
func (oc *ObjectCache) no_lock_strict_get(key string) (interface{}, int, bool) {
	element, found := oc.elements[key]
	if !found {
		return nil, ErrIntReturnNum, false
	} else {
		if element.expireAt == 0 {
			//处理永不超时的数据
			return element.object, NeverExpireTime, true
		}
		now := uint32(time.Now().Unix())
		if now >= element.expireAt && element.expireAt > 0 {
			return nil, ErrIntReturnNum, false
		}
		return element.object, element.expireAt - now, true
	}
}

//获得所有数据，可能包含过期但未删除的数据（宽松模式）
func (oc *ObjectCache) no_lock_get(key string, enable bool) (interface{}, uint8, bool) {
	element, found := oc.elements[key]
	if !found {
		return nil, TypeNoValidElement, false
	} else {
		if enable && element.expireAt >= 0 {
			if element.expireAt == 0 {
				return element.object, TypeNeverExpiredElement, true
			} else {
				now := uint32(time.Now().Unix())
				if now >= element.expireAt {
					return element.object, TypeExpiredElement, true
				} else {
					return element.object, TypeValidElement, true
				}
			}
		} else {
			return element.object, TypeValidElement, true
		}
	}
}

func (oc *ObjectCache) no_lock_set(key string, value interface{}, expireSeconds int) {
	var expireAt uint32 = 0
	if expireSeconds == DefaultExpireTime {
		expireAt = uint32(time.Now().Add(oc.defaultExpireSeconds).Unix())
	}
	//如果超时间大于0，按照设定的情况超时；如果小于0，所有cache内的元素，超时间都是0，表示永不超时
	if expireSeconds > 0 {
		expireAt = uint32(time.Now().Add(expireSeconds).Unix())
	}
	oc.elements[key] = cacheElement{object: value, expireAt: expireAt}
}

func (oc *ObjectCache) no_lock_delete(key string) (interface{}, bool) {
	if element, found := oc.elements[key]; found {
		delete(oc.elements, key)
		return element.object, true
	} else {
		delete(oc.elements, key)
		return nil, false
	}
}

func (oc *ObjectCache) Set(key string, value interface{}, expireSeconds int) {
	oc.lock.Lock()
	oc.no_lock_set(key, value, expireSeconds)
	oc.lock.Unlock()
}

func (oc *ObjectCache) SetWithDefaultExpireSeconds(key string, value interface{}) {
	oc.Set(key, value, DefaultExpireTime)
}

func (oc *ObjectCache) SetWithNeverExpired(key string, value interface{}) {
	oc.Set(key, value, NeverExpireTime)
}

func (oc *ObjectCache) TrySet(key string, value interface{}, expireSeconds int) error {
	oc.lock.Lock()
	if _, _, found := oc.no_lock_strict_get(key); found {
		oc.lock.Unlock()
		return ErrElementAlreadyExist
	} else {
		oc.no_lock_set(key, value, expireSeconds)
		oc.lock.Unlock()
		return nil
	}
}

func (oc *ObjectCache) TrySetWithDefaultExpireSeconds(key string, value interface{}) error {
	return oc.TrySet(key, value, DefaultExpireTime)
}

func (oc *ObjectCache) TrySetWithNeverExpired(key string, value interface{}) error {
	return oc.TrySet(key, value, NeverExpireTime)
}

func (oc *ObjectCache) Replace(key string, value interface{}, expireSeconds int) {
	oc.lock.Lock()
	oc.no_lock_set(key, value, expireSeconds)
	oc.lock.Unlock()
}

func (oc *ObjectCache) TryReplace(key string, value interface{}, expireSeconds int) error {
	oc.lock.Lock()
	if _, _, found := oc.no_lock_strict_get(key); !found {
		oc.lock.Unlock()
		return ErrElementDoNotExist
	} else {
		oc.no_lock_set(key, value, expireSeconds)
		oc.lock.Unlock()
		return nil
	}
}

func (oc *ObjectCache) UpdateValue(key string, value interface{}) error {
	oc.lock.Lock()
	if _, expireSeconds, found := oc.no_lock_strict_get(key); !found {
		oc.lock.Unlock()
		return ErrElementDoNotExist
	} else {
		oc.no_lock_set(key, value, expireSeconds)
		oc.lock.Unlock()
		return nil
	}
}

func (oc *ObjectCache) UpdateExpireSeconds(key string, expireSeconds int) error {
	oc.lock.Lock()
	if value, _, found := oc.no_lock_strict_get(key); !found {
		oc.lock.Unlock()
		return ErrElementDoNotExist
	} else {
		oc.no_lock_set(key, value, expireSeconds)
		oc.lock.Unlock()
		return nil
	}
}

func (oc *ObjectCache) Get(key string) (interface{}, error) {
	oc.lock.RLock()
	value, _, found := oc.no_lock_strict_get(key)
	oc.lock.RUnlock()
	if !found {
		return nil, ErrElementDoNotExist
	} else {
		return value, nil
	}
}

func (oc *ObjectCache) GetWithExpiration(key string) (interface{}, int, error) {
	oc.lock.RLock()
	value, expireSeconds, found := oc.no_lock_strict_get(key)
	oc.lock.RUnlock()
	if !found {
		return nil, ErrIntReturnNum, ErrElementDoNotExist
	} else {
		return value, expireSeconds, nil
	}
}

func (oc *ObjectCache) GetPossiblyExpired(key string) (interface{}, error) {
	oc.lock.RLock()
	value, _, found := oc.no_lock_get(key, false)
	oc.lock.RUnlock()
	if !found {
		return nil, ErrElementDoNotExist
	} else {
		return value, nil
	}
}

func (oc *ObjectCache) GetPossiblyExpiredWitchType(key string) (interface{}, uint8, error) {
	oc.lock.RLock()
	value, dataType, found := oc.no_lock_get(key, true)
	oc.lock.RUnlock()
	if !found {
		return nil, dataType, ErrElementDoNotExist
	} else {
		return value, dataType, nil
	}
}

func (oc *ObjectCache) Remove(key string) interface{} {
	oc.lock.Lock()
	object, _ := oc.no_lock_delete(key)
	oc.lock.Unlock()
	return object
}

func (oc *ObjectCache) RemoveWitchCallback(key string) interface{} {
	oc.lock.Lock()
	object, evicted := oc.no_lock_delete(key)
	oc.lock.Unlock()
	if evicted && oc.onEvicted != nil && oc.enableEvictedCallback {
		oc.onEvicted(key, object)
	}
	return object
}

func (oc *ObjectCache) Delete(key string) {
	oc.lock.Lock()
	oc.no_lock_delete(key)
	oc.lock.Unlock()
}

func (oc *ObjectCache) DeleteWitchCallback(key string) {
	oc.lock.Lock()
	element, evicted := oc.no_lock_delete(key)
	oc.lock.Unlock()
	if evicted && oc.onEvicted != nil && oc.enableEvictedCallback {
		oc.onEvicted(key, element)
	}
}

func (oc *ObjectCache) DeleteAllExpired() {
	now := uint32(time.Now().Unix())
	oc.lock.Lock()
	for key, element := range oc.elements {
		if element.expireAt > 0 && now >= element.expireAt {
			oc.no_lock_delete(key)
		}
	}
	oc.lock.Unlock()
}

func (oc *ObjectCache) DeleteAllExpiredWitchCallback() {
	var evictedElements []evictedElementKeyAndObject
	now := uint32(time.Now().Unix())
	oc.lock.Lock()
	for key, element := range oc.elements {
		// "Inlining" of expired
		if element.expireAt > 0 && now >= element.expireAt {
			object, evicted := oc.no_lock_delete(key)
			if evicted && oc.onEvicted != nil && oc.enableEvictedCallback {
				evictedElements = append(evictedElements, evictedElementKeyAndObject{key, object})
			}
		}
	}
	oc.lock.Unlock()
	if oc.onEvicted != nil && oc.enableEvictedCallback {
		for _, data := range evictedElements {
			oc.onEvicted(data.key, data.value)
		}
	}
}

func (oc *ObjectCache) OnEvicted(fn func(string, interface{})) {
	oc.lock.Lock()
	oc.onEvicted = fn
	oc.lock.Unlock()
}

func (oc *ObjectCache) EnableEvictedCallback(enable bool) {
	oc.enableEvictedCallback = enable
}

func (oc *ObjectCache) Count() int {
	oc.lock.Lock()
	count := len(oc.elements)
	oc.lock.Unlock()
	return count
}

func (oc *ObjectCache) Clean() {
	oc.lock.Lock()
	oc.elements = map[string]cacheElement{}
	oc.lock.Unlock()
}

func (oc *ObjectCache) IsEmpty() bool {
	if oc.Count() == 0 {
		return true
	} else {
		return false
	}
}

///=================================================================================================
///
///	后台自动清理过期对象的看门人
///

func (j *janitor) Run(oc *ObjectCache) {
	ticker := time.NewTicker(j.intervalTime)
	for {
		select {
		case <-ticker.C:
			oc.DeleteAllExpiredWitchCallback()
		case <-j.stop:
			ticker.Stop()
			return
		}
	}
}

func stopJanitor(objectCache *ObjectCache) {
	objectCache.janitorPtr.stop <- true
}

func runJanitor(objectCache *ObjectCache, interval time.Duration) {
	objectCache.janitorPtr = &janitor{
		intervalTime: interval,
		stop:         make(chan bool),
	}
	go objectCache.janitorPtr.Run(objectCache)
}

///=================================================================================================
///
///	创建动态缓存方法
///

func newCacheWithJanitor(cacheDefaultExpireSeconds uint32, autoCleanupSeconds uint32, cacheElements map[string]cacheElement) *ObjectCache {
	var objectCache *ObjectCache
	if cacheDefaultExpireSeconds == 0 {
		objectCache = &ObjectCache{
			defaultExpireSeconds: DefaultExpireSeconds,
			elements:             cacheElements,
			lock:                 new(sync.RWMutex),
		}
	} else {
		objectCache = &ObjectCache{
			defaultExpireSeconds: cacheDefaultExpireSeconds,
			elements:             cacheElements,
			lock:                 new(sync.RWMutex),
		}
	}
	if autoCleanupSeconds >= 0 {
		if autoCleanupSeconds == 0 {
			runJanitor(objectCache, time.Second*DefaultAutoCleanupSeconds)
		} else {
			runJanitor(objectCache, time.Second*autoCleanupSeconds)
		}
		runtime.SetFinalizer(objectCache, stopJanitor)
	}
	return objectCache
}

func New(cacheDefaultExpireSeconds uint32, autoCleanupSeconds uint32) *ObjectCache {
	return newCacheWithJanitor(cacheDefaultExpireSeconds, autoCleanupSeconds, make(map[string]cacheElement))
}

func NewWithDefaultSeconds() *ObjectCache {
	return New(0, 0)
}
