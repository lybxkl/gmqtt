package service

import "sync"

type SafeMap struct {
	v  map[uint16]interface{}
	wg *sync.RWMutex
}

func NewSafeMap() *SafeMap {
	return &SafeMap{
		v:  make(map[uint16]interface{}),
		wg: &sync.RWMutex{},
	}
}

func (sm *SafeMap) Get(k uint16) (v interface{}, exist bool) {
	sm.wg.RLock()
	defer sm.wg.RUnlock()
	v, exist = sm.v[k]
	return
}

// 之前存在返回false，不会替换
// 存在返回旧数据，不存在存入，并返回新数据
func (sm *SafeMap) Set(k uint16, v interface{}) (interface{}, bool) {
	sm.wg.Lock()
	defer sm.wg.Unlock()
	if old, ok := sm.v[k]; ok {
		return old, false
	}
	sm.v[k] = v
	return v, true
}
