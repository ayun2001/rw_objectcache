package rw_objectcache

import (
	"errors"
	"math"
	"reflect"
	"runtime"
	"sync"
	"time"
)

const (
	DefaultExpireSeconds            = 30
	DefaultAutoCleanupSeconds       = DefaultExpireSeconds * 2
	ErrIntReturnNum           int64 = -9223372036854775000
	NeverExpireTime           int   = -1
	DefaultExpireTime         int   = 0
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
	ErrElementIsNotNumeric = errors.New("Element is not Int64.")
)

///=================================================================================================
///
///	对象缓存实现
///

type cacheElement struct {
	object   interface{}
	expireAt uint32
}

func (el cacheElement) IsInt64() bool {
	return reflect.TypeOf(el.object).Kind() == reflect.Int64
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
func (oc *ObjectCache) no_lock_strict_get(key string) (interface{}, int64, bool) {
	element, found := oc.elements[key]
	if !found {
		return nil, ErrIntReturnNum, false //没有能找到元素
	} else {
		if element.expireAt == 0 {
			//处理永不超时的数据
			return element.object, NeverExpireTime, true //永不超时数据
		}
		now := uint32(time.Now().Unix())
		if now >= element.expireAt && element.expireAt > 0 {
			return element.object, ErrIntReturnNum, false //超时数据
		}
		return element.object, int64(element.expireAt) - int64(now), true //正常数据
	}
}

//获得所有数据，可能包含过期但未删除的数据（宽松模式）
func (oc *ObjectCache) no_lock_get(key string, enable bool) (interface{}, uint8, bool) {
	element, found := oc.elements[key]
	if !found {
		return nil, TypeNoValidElement, false //没有能找到元素
	} else {
		if enable && element.expireAt >= 0 {
			if element.expireAt == 0 {
				return element.object, TypeNeverExpiredElement, true //永不超时数据
			} else {
				now := uint32(time.Now().Unix())
				if now >= element.expireAt {
					return element.object, TypeExpiredElement, true //超时但是没有删除的数据
				} else {
					return element.object, TypeValidElement, true //正常数据
				}
			}
		} else {
			return element.object, TypeValidElement, true //正常数据
		}
	}
}

func (oc *ObjectCache) no_lock_set(key string, value interface{}, expireSeconds int64) {
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

func (oc *ObjectCache) no_lock_incr(key string, step int64, expireSeconds int64) (int64, error) {
	if element, found := oc.elements[key]; found {
		if element.IsInt64() && (element.object.(int64) < (math.MaxInt64 - step)) {
			element.object.(int64) += step
			oc.no_lock_set(key, element.object, expireSeconds)
			return element.object.(int64), nil
		} else {
			return 0, ErrElementIsNotNumeric
		}
	} else {
		oc.no_lock_set(key, step, expireSeconds)
		return step, nil
	}
}

func (oc *ObjectCache) no_lock_decr(key string, step int64, expireSeconds int64) (int64, error) {
	if element, found := oc.elements[key]; found {
		if element.IsInt64() && (element.object.(int64) > (math.MinInt64 + step)) {
			element.object.(int64) -= step
			oc.no_lock_set(key, element.object, expireSeconds)
			return element.object.(int64), nil
		} else {
			return 0, ErrElementIsNotNumeric
		}
	} else {
		value := 0 - step
		oc.no_lock_set(key, value, expireSeconds)
		return value, nil
	}
}

func (oc *ObjectCache) no_lock_delete(key string) (interface{}, bool) {
	if element, found := oc.elements[key]; found {
		delete(oc.elements, key)
		return element.object, true
	} else {
		return nil, false
	}
}

func (oc *ObjectCache) no_lock_keys(expired bool) []string {
	dataSet := make([]string, len(oc.elements))
	count := 0
	now := uint32(time.Now().Unix())
	for key, element := range oc.elements {
		if expired {
			if element.expireAt <= now {
				dataSet[count] = key
				count++
			}
		} else {
			if element.expireAt == 0 || element.expireAt > now {
				dataSet[count] = key
				count++
			}
		}
	}
	if count > 0 {
		keys := make([]string, count)
		copy(keys, dataSet[:count])
		return keys
	} else {
		return []string{}
	}
}

func (oc *ObjectCache) Set(key string, value interface{}, expireSeconds int64) {
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

func (oc *ObjectCache) TrySet(key string, value interface{}, expireSeconds int64) error {
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

func (oc *ObjectCache) Replace(key string, value interface{}, expireSeconds int64) {
	oc.lock.Lock()
	oc.no_lock_set(key, value, expireSeconds)
	oc.lock.Unlock()
}

func (oc *ObjectCache) TryReplace(key string, value interface{}, expireSeconds int64) error {
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

func (oc *ObjectCache) UpdateExpireSeconds(key string, expireSeconds int64) error {
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
		if value != nil {
			oc.Delete(key) //惰性删除，这样比较节省CPU
		}
		return nil, ErrElementDoNotExist
	} else {
		return value, nil
	}
}

func (oc *ObjectCache) GetWithExpiration(key string) (interface{}, int64, error) {
	oc.lock.RLock()
	value, expireSeconds, found := oc.no_lock_strict_get(key)
	oc.lock.RUnlock()
	if !found {
		if value != nil {
			oc.Delete(key) //惰性删除，这样比较节省CPU
		}
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

func (oc *ObjectCache) IncrementBy(key string, step int64, expireSeconds int64) (value int64, err error) {
	oc.lock.Lock()
	value, err = oc.no_lock_incr(key, step, expireSeconds)
	oc.lock.Unlock()
	return
}

func (oc *ObjectCache) Increment(key string) (value int64, err error) {
	value, err = oc.IncrementBy(key, 1, oc.defaultExpireSeconds)
	return
}

func (oc *ObjectCache) IncrementWithDefaultStep(key string, expireSeconds int64) (value int64, err error) {
	value, err = oc.IncrementBy(key, 1, expireSeconds)
	return
}

func (oc *ObjectCache) IncrementWithDefaultExpireSeconds(key string, step int64) (value int64, err error) {
	value, err = oc.IncrementBy(key, step, oc.defaultExpireSeconds)
	return
}

func (oc *ObjectCache) DecrementBy(key string, step int64, expireSeconds int64) (value int64, err error) {
	oc.lock.Lock()
	value, err = oc.no_lock_decr(key, step, expireSeconds)
	oc.lock.Unlock()
	return
}

func (oc *ObjectCache) Decrement(key string) (value int64, err error) {
	value, err = oc.DecrementBy(key, 1, oc.defaultExpireSeconds)
	return
}

func (oc *ObjectCache) DecrementWithDefaultStep(key string, expireSeconds int64) (value int64, err error) {
	value, err = oc.DecrementBy(key, 1, expireSeconds)
	return
}

func (oc *ObjectCache) DecrementWithDefaultExpireSeconds(key string, step int64) (value int64, err error) {
	value, err = oc.DecrementBy(key, step, oc.defaultExpireSeconds)
	return
}

func (oc *ObjectCache) Count() (count int) {
	oc.lock.Lock()
	count = len(oc.elements)
	oc.lock.Unlock()
	return
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

func (oc *ObjectCache) Keys() (keys []string) {
	oc.lock.Lock()
	keys = oc.no_lock_keys(false)
	oc.lock.Unlock()
	return
}

func (oc *ObjectCache) ExpiredKeys() (keys []string) {
	oc.lock.Lock()
	keys = oc.no_lock_keys(true)
	oc.lock.Unlock()
	return
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

func NewExpress(cacheDefaultExpireSeconds uint32) *ObjectCache {
	return New(cacheDefaultExpireSeconds, cacheDefaultExpireSeconds*2)
}

func NewWithDefaultSeconds() *ObjectCache {
	return New(0, 0)
}
