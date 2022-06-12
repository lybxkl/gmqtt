package collection

import "sync"

const (
	copyThreshold = 1000
	maxDeletion   = 10000
)

// SafeMap provides a map alternative to avoid memory leak.
// This implementation is not needed until issue below fixed.
// https://github.com/golang/go/issues/20135
// https://github.com/zeromicro/go-zero/blob/master/core/collection/safemap.go
type SafeMap struct {
	lock        sync.RWMutex
	deletionOld int
	deletionNew int
	dirtyOld    map[interface{}]interface{}
	dirtyNew    map[interface{}]interface{}
}

// NewSafeMap returns a SafeMap.
func NewSafeMap() *SafeMap {
	return &SafeMap{
		dirtyOld: make(map[interface{}]interface{}),
		dirtyNew: make(map[interface{}]interface{}),
	}
}

// Del deletes the value with the given key from m.
func (m *SafeMap) Del(key interface{}) {
	m.lock.Lock()
	m.del(key)
	m.lock.Unlock()
}

func (m *SafeMap) Dels(keys ...interface{}) {
	m.lock.Lock()
	for _, key := range keys {
		m.del(key)
	}
	m.lock.Unlock()
}

func (m *SafeMap) del(key interface{}) (interface{}, bool) {
	var (
		old interface{}
		ok  bool
	)
	if old, ok = m.dirtyOld[key]; ok {
		delete(m.dirtyOld, key)
		m.deletionOld++
	} else if old, ok = m.dirtyNew[key]; ok {
		delete(m.dirtyNew, key)
		m.deletionNew++
	}
	if m.deletionOld >= maxDeletion && len(m.dirtyOld) < copyThreshold {
		for k, v := range m.dirtyOld {
			m.dirtyNew[k] = v
		}
		m.dirtyOld = m.dirtyNew
		m.deletionOld = m.deletionNew
		m.dirtyNew = make(map[interface{}]interface{})
		m.deletionNew = 0
	}
	if m.deletionNew >= maxDeletion && len(m.dirtyNew) < copyThreshold {
		for k, v := range m.dirtyNew {
			m.dirtyOld[k] = v
		}
		m.dirtyNew = make(map[interface{}]interface{})
		m.deletionNew = 0
	}
	return old, ok
}

// Get gets the value with the given key from m.
func (m *SafeMap) Get(key interface{}) (interface{}, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if val, ok := m.dirtyOld[key]; ok {
		return val, true
	}

	val, ok := m.dirtyNew[key]
	return val, ok
}

func (m *SafeMap) ContainsKey(key interface{}) bool {
	_, ok := m.Get(key)
	return ok
}

// Set sets the value into m with the given key.
func (m *SafeMap) Set(key, value interface{}) {
	m.lock.Lock()
	if m.deletionOld <= maxDeletion {
		if _, ok := m.dirtyNew[key]; ok {
			delete(m.dirtyNew, key)
			m.deletionNew++
		}
		m.dirtyOld[key] = value
	} else {
		if _, ok := m.dirtyOld[key]; ok {
			delete(m.dirtyOld, key)
			m.deletionOld++
		}
		m.dirtyNew[key] = value
	}
	m.lock.Unlock()
}

// GetOrSet ...获取，不存在即设置，返回最新或者旧的值，是否原来存在标识
func (m *SafeMap) GetOrSet(key, value interface{}) (interface{}, bool) {
	val, ok := m.Get(key)
	if ok {
		return val, true
	}
	m.lock.Lock()
	if val, ok = m.dirtyOld[key]; ok {
		return val, true
	}

	if val, ok = m.dirtyNew[key]; ok {
		return val, true
	}

	if m.deletionOld <= maxDeletion {
		if _, ok := m.dirtyNew[key]; ok {
			delete(m.dirtyNew, key)
			m.deletionNew++
		}
		m.dirtyOld[key] = value
	} else {
		if _, ok := m.dirtyOld[key]; ok {
			delete(m.dirtyOld, key)
			m.deletionOld++
		}
		m.dirtyNew[key] = value
	}
	m.lock.Unlock()
	return value, false
}

// GetDel 获取并删除， 返回删除对象，是否存在
func (m *SafeMap) GetDel(key interface{}) (interface{}, bool) {
	m.lock.Lock()
	old, ok := m.del(key)
	m.lock.Unlock()
	return old, ok
}

// Size returns the size of m.
func (m *SafeMap) Size() int {
	m.lock.RLock()
	size := len(m.dirtyOld) + len(m.dirtyNew)
	m.lock.RUnlock()
	return size
}

func (m *SafeMap) Range(fn func(k, v interface{}) error) (err error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for k, v := range m.dirtyOld {
		k1, v1 := k, v
		err = fn(k1, v1)
		if err != nil {
			return
		}
	}
	for k, v := range m.dirtyNew {
		k1, v1 := k, v
		err = fn(k1, v1)
		if err != nil {
			return
		}
	}
	return
}
