package shardkv

import {
  "container/list"
  "fmt"
  "sync"
  "time"
}

//
// A simple LRU cache implementation which supports
// basic put and get operations.
//
type Cache struct {
  mu sync.Mutex

  // recent usage list and lookup table 
  list *list.List // tracks recent usage
  table map[string]*list.Element // table for fast lookup

  // approximation of current size 
  size uint64

  // cache size limitation
  capacity uint64
}

type entry struct {
  key string
  value string
  time_accessed time.Time
}

func MakeCache(capacity uint64) *Cache {
  return &Cache{
    list:     list.New(),
    table:    make(map[string]*list.Element),
    capacity: capacity,
  }
}

func (cache *Cache) Get(key string) (v string, ok bool) {
  cache.mu.Lock()
  defer cache.mu.Unlock()

  element := cache.table[key]
  if element == nil {
    return nil, false
  }
  cache.touch(element)
  return element.(*entry).value, true
}

func (cache *Cache) Put(key string, value string) {
  cache.mu.Lock()
  defer cache.mu.Unlock()

  element := cache.table[key]
  if element != nil {
    cache.update(element, value)
  } else {
    cache.add(element, value)
  }
}

func (cache *Cache) Delete(key string) bool {
  cache.mu.Lock()
  defer cache.mu.Unlock()

  element := cache.table[key]
  if element == nil {
    return false
  }

  cache.list.Remove(element)
  delete(cache.table, key)
  cache.size -= uint64(len(element.(*entry).value))
  return true
}

func (cache *Cache) Clear() {
  cache.mu.Lock()
  defer cache.mu.Unlock()

  // replace old state with initial state
  // old references are garbage collected
  cache.list.Init()
  cache.table = make(map[string]*list.Element)
  cache.size = 0
}

func (cache *Cache) Keys() []string {
  cache.mu.Lock()
  defer cache.mu.Unlock()

  keys := make([]string, 0, cache.list.Len())
  for e := cache.list.Front(); e != nil; e = e.Next() {
    keys = append(keys, e.(*entry).key)
  }
  return keys
}

func (cache *Cache) Values() []string {
  cache.mu.Lock()
  defer cache.mu.Unlock()

  values := make([]string, 0, cache.list.Len())
  for e := cache.list.Front(); e != nil; e = e.Next() {
    values = append(values, e.(*entry).value)
  }
  return values
}

func (cache *Cache) update(element *list.Element, value string) {
  valueSize := len(value)
  sizeDiff := valueSize - len(element.(*entry))
  element.(*entry).value = value
  cache.size += uint64(sizeDiff)
  cache.touch(element)
  cache.checkCapacity()
}

func (cache *Cache) touch(element *list.Element) {
  cache.list.MoveToFront(element)
  element.(*entry).time_accessed = time.Now()
}

func (cache *Cache) add(key string, value string) {
  newEntry := &entry{key, value, time.Now()}
  element := cache.list.PushFront(newEntry)
  cache.table[key] = element
  cache.size += uint64(len(newEntry.value))
  cache.checkCapacity()
}

func (cache *Cache) checkCapacity() {
  for cache.size > cache.capacity {
    delElem := cache.list.Back()
    delEntry := delElem.(*entry)
    delete(cache.table, delEntry.key)
    cache.list.Remove(delElem)
    cache.size -= uint64(len(delEntry.value))
  }
}
