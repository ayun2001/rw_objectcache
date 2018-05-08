package rw_objectcache

import (
	"time"
	"sync"
	"errors"
)

const (
	ErrIntReturnNum int = -66535
	//永不过期
	NeverExpireTime int = -1
	//使用默认过期 （5分钟）
	DefaultExpireTime int = 0
)

var (
	ErrElementIsExist = errors.New("Element is already exists.")
	ErrElementIsNotExist = errors.New("Element do not exists.")
)

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

type ObjectCache struct {
	defaultExpireSeconds uint32
	elements             map[string]cacheElement
	lock                 *sync.RWMutex
	onEvicted            func(string, interface{})
	janitorPtr           *janitor
}

func (oc *ObjectCache) no_lock_get(key string) (interface{}, int, bool) {
	element, found := oc.elements[key]
	//如果元素没有找到 或者 缓存元素已经超时
	currentTimestamp := uint32(time.Now().Unix())
	if !found || (currentTimestamp >= element.expireAt && element.expireAt > 0) {
		return nil, ErrIntReturnNum, false
	}
	if element.expireAt == 0 {
		//处理永不超时的数据
		return element.object, NeverExpireTime, true
	}
	return element.object, element.expireAt - currentTimestamp, true
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
	oc.elements[key] = cacheElement{object:value, expireAt:expireAt}
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
	if _, _, found := oc.no_lock_get(key); found {
		oc.lock.Unlock()
		return ErrElementIsExist
	}
	oc.no_lock_set(key, value, expireSeconds)
	oc.lock.Unlock()
	return nil
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
	if _, _, found := oc.no_lock_get(key); !found {
		oc.lock.Unlock()
		return ErrElementIsNotExist
	}
	oc.no_lock_set(key, value, expireSeconds)
	oc.lock.Unlock()
	return nil
}

func (oc *ObjectCache) Get(key string) (interface{}, error) {
	oc.lock.RLock()
	value, _, found := oc.no_lock_get(key)
	oc.lock.RUnlock()
	if !found {
		return nil, ErrElementIsNotExist
	} else {
		return value, nil
	}
}

func (oc *ObjectCache) GetWithExpiration(key string) (interface{}, int, error) {
	oc.lock.RLock()
	value, expireSeconds, found := oc.no_lock_get(key)
	oc.lock.RUnlock()
	if !found {
		return nil, ErrIntReturnNum, ErrElementIsNotExist
	} else {
		return value, expireSeconds, nil
	}
}

func (oc *ObjectCache) no_lock_delete(key string, enable bool) (interface{}, bool) {
	if oc.onEvicted != nil  && enable {
		if element, found := oc.elements[key]; found {
			delete(oc.elements, key)
			return element.object, true
		}
	}
	delete(oc.elements, key)
	return nil, false
}

func (oc *ObjectCache) Delete(key string) {
	oc.lock.Lock()
	oc.no_lock_delete(key, false)
	oc.lock.Unlock()
}

func (oc *ObjectCache) DeleteWitchEvictedCallback(key string) {
	oc.lock.Lock()
	element, evicted := oc.no_lock_delete(key, true)
	oc.lock.Unlock()
	if evicted {
		oc.onEvicted(key, element)
	}
}
