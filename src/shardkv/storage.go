package shardkv

import (
  "container/list"
  "sync"
  "time"
  "shardmaster"
  "strconv"
  "labix.org/v2/mgo"
  "labix.org/v2/mgo/bson"
  "fmt"
)

//
// A simple LRU cache implementation which supports
// basic put and get operations.
//
type ShardCache struct {
  mu sync.Mutex

  // recent usage list and lookup table 
  list *list.List // tracks recent usage
  table map[int]map[string]*list.Element // table for fast lookup

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

func MakeCache(capacity uint64) *ShardCache {
  cache := &ShardCache{
    list:     list.New(),
    table:    make(map[int](map[string]*list.Element)),
    capacity: capacity,
  }
  for i := 0; i < shardmaster.NShards; i++ {
    cache.table[i] = make(map[string]*list.Element)
  }
  return cache
}

func (cache *ShardCache) Size() uint64 {
  return cache.size
}

func (cache *ShardCache) Get(key string) (v string, ok bool) {
  cache.mu.Lock()
  defer cache.mu.Unlock()

  shard := key2shard(key)
  element := cache.table[shard][key]
  if element == nil {
    return "", false
  }
  cache.touch(element)
  return element.Value.(*entry).value, true
}

func (cache *ShardCache) Put(key string, value string) {
  cache.mu.Lock()
  defer cache.mu.Unlock()

  shard := key2shard(key)
  element := cache.table[shard][key]
  if element != nil {
    cache.update(element, value)
  } else {
    cache.add(key, value)
  }
}

func (cache *ShardCache) Delete(key string) bool {
  cache.mu.Lock()
  defer cache.mu.Unlock()

  shard := key2shard(key)
  element := cache.table[shard][key]
  if element == nil {
    return false
  }

  cache.list.Remove(element)
  delete(cache.table[shard], key)
  cache.size -= uint64(len(element.Value.(*entry).value) + len(element.Value.(*entry).key))
  return true
}

func (cache *ShardCache) Clear() {
  cache.mu.Lock()
  defer cache.mu.Unlock()

  // replace old state with initial state
  // old references are garbage collected
  cache.list.Init()
  cache.table = make(map[int]map[string]*list.Element)
  cache.size = 0

  for i := 0; i < shardmaster.NShards; i++ {
    cache.table[i] = make(map[string]*list.Element)
  }
}

func (cache *ShardCache) ClearShard(shard int) {
  cache.mu.Lock()
  defer cache.mu.Unlock()

  cache.table[shard] = make(map[string]*list.Element)
}

func (cache *ShardCache) Keys() []string {
  cache.mu.Lock()
  defer cache.mu.Unlock()

  keys := make([]string, 0, cache.list.Len())
  for e := cache.list.Front(); e != nil; e = e.Next() {
    keys = append(keys, e.Value.(*entry).key)
  }
  return keys
}

func (cache *ShardCache) Values() []string {
  cache.mu.Lock()
  defer cache.mu.Unlock()

  values := make([]string, 0, cache.list.Len())
  for e := cache.list.Front(); e != nil; e = e.Next() {
    values = append(values, e.Value.(*entry).value)
  }
  return values
}

func (cache *ShardCache) KVPairs() []KVPair {
  cache.mu.Lock()
  defer cache.mu.Unlock()

  kvpairs := make([]KVPair, 0, cache.list.Len())
  for e := cache.list.Front(); e != nil; e = e.Next() {
    kvpairs = append(kvpairs, KVPair{key2shard(e.Value.(*entry).key), e.Value.(*entry).key, e.Value.(*entry).value})
  }
  return kvpairs
}

func (cache *ShardCache) update(element *list.Element, value string) {
  valueSize := len(value)
  sizeDiff := valueSize - len(element.Value.(*entry).value)
  element.Value.(*entry).value = value
  cache.size += uint64(sizeDiff)
  cache.touch(element)
  cache.maintainSize()
}

func (cache *ShardCache) touch(element *list.Element) {
  cache.list.MoveToFront(element)
  element.Value.(*entry).time_accessed = time.Now()
}

func (cache *ShardCache) add(key string, value string) {
  shard := key2shard(key)
  newEntry := &entry{key, value, time.Now()}
  element := cache.list.PushFront(newEntry)
  cache.table[shard][key] = element
  cache.size += uint64(len(newEntry.value) + len(newEntry.key))
  cache.maintainSize()
}

func (cache *ShardCache) maintainSize() {
  //deleted := list.New()
  for cache.size > cache.capacity {
    delElem := cache.list.Back()
    delEntry := delElem.Value.(*entry)
    shard := key2shard(delEntry.key)
    delete(cache.table[shard], delEntry.key)
    cache.list.Remove(delElem)
    cache.size -= uint64(len(delEntry.value) + len(delEntry.key))
  }
}

type Storage struct {
  mu sync.Mutex
  me int

  cache *ShardCache

  // disk storage state
  dbSession *mgo.Session
  db *mgo.Collection
  snapshots *mgo.Collection
  dedupsnaps *mgo.Collection

  // logging for background writes
  writeLog map[int]WriteOp
  applied int
}

type WriteOp struct {
  shard int
  key string
  value string
  dbok bool
}

func (st *Storage) makeCache(capacity uint64) {
  st.cache = MakeCache(capacity)
}

func (st *Storage) connectToDiskDB(url string) {
  var err error
  st.dbSession, err = mgo.Dial(url)
  if err != nil {
    panic(err)
  }
  meStr := strconv.Itoa(st.me)
  st.db = st.dbSession.DB("db").C("kvstore" + meStr)
  st.snapshots = st.dbSession.DB("db").C("snapshots" + meStr)
  st.dedupsnaps = st.dbSession.DB("db").C("dedupsnaps" + meStr)
}

func (st *Storage) DBClear() {
  st.db.RemoveAll(bson.M{})
  check, _ := st.snapshots.Find(bson.M{}).Count()
  if (check > 0) {
    st.snapshots.RemoveAll(bson.M{})
    st.dedupsnaps.RemoveAll(bson.M{})
  }
}

func (st *Storage) CacheClear() {
  st.cache.Clear()
}

func (st *Storage) Clear() {
  st.DBClear()
  st.CacheClear()
}

func MakeStorage(me int, capacity uint64, dbURL string) *Storage {
  st := new(Storage)
  st.me = me
  st.makeCache(capacity)
  st.connectToDiskDB(dbURL)
  st.writeLog = make(map[int]WriteOp)

  fmt.Printf("Making storage...\n")

  go st.writeInBackground()
  return st
}

func (st *Storage) Get(key string, shardNum int) string {

  value, ok := st.cache.Get(key)
  if !ok {
    result := KVPair{}
    err := st.db.Find(bson.M{"shard": shardNum, "key": key}).One(&result)
    if err != nil {
      ok = false
    } else {
      st.cache.Put(key, result.Value)
      value = result.Value
    }
  }
  return value
}

func (st *Storage) Put(key string, value string, doHash bool, shardNum int) string {
  prev, ok := st.cache.Get(key)
  var dbok bool
  if !ok {
    result := KVPair{}
    err := st.db.Find(bson.M{"shard": shardNum, "key": key}).One(&result)
    if err != nil {
      dbok = false
    } else {
      prev = result.Value
      dbok = true
    }
  }

  //var deleted *list.List // list of cache values removed to send to disk
  if doHash {
    toBeHashed := prev + value
    hash := strconv.Itoa(int(hash(toBeHashed)))
    st.cache.Put(key, hash)
    //deleted = st.cache.Put(key, hash)
  } else {
    st.cache.Put(key, value)
    //deleted = st.cache.Put(key, value)
  }

  st.writeLog[st.applied] = WriteOp{shardNum, key, value, dbok}
  st.applied++

  // insert removed cache entries one at a time into DB, possibly faster if done together?
  /*for e := st.cache.list.Front(); e != nil; e = e.Next() {
    entry := e.(*entry)
    shard := key2shard(entry.key)
    err := st.db.Insert(&KVPair{shard, entry.key, entry.value})
    if err != nil {
      panic(err)
    }
  }*/
  return prev
}

func (st *Storage) CreateSnapshot(confignum int, dedup map[string]ClientReply) {
  cachedata := st.cache.KVPairs()
  results := []KVPair{}
  index := 0
  for len(results) >= 100 {
    st.db.Find(bson.M{}).Skip(index * GrabSize).Limit(GrabSize).All(&results)
    for i := 0; i < len(results); i++ {
      st.snapshots.Insert(&SnapshotKV{confignum, results[i].Shard, results[i].Key, results[i].Value, false})
    }
    index++
  }
  for i := 0; i < len(cachedata); i++ {
    st.snapshots.Insert(&SnapshotKV{confignum, cachedata[i].Shard, cachedata[i].Key, cachedata[i].Value, true})
  }
  for key, value := range dedup {
    st.dedupsnaps.Insert(&SnapshotDedup{confignum, key, value.Value, value.Err, value.Counter})
  }
}

func (st *Storage) ReadSnapshotDB(confignum int, shardnum int, index int, cache bool) (p map[string]string, fin bool) {
  piece := make(map[string]string)
  results := []SnapshotKV{}
  if cache {
    st.snapshots.Find(bson.M{"cache": "true", "config": confignum}).Skip(index * GrabSize).Limit(GrabSize).All(&results)
  } else {
    st.snapshots.Find(bson.M{"cache": "false", "config": confignum}).Skip(index * GrabSize).Limit(GrabSize).All(&results)
  }
  for i := 0; i < len(results); i++ {
    piece[results[i].Key] = results[i].Value
  }
  if len(results) < GrabSize {
    return piece, true
  }
  return piece, false
}

func (st *Storage) ReadSnapshotDedup(confignum int) map[string]ClientReply {
  dedup := make(map[string]ClientReply)
  results := []SnapshotDedup{}
  st.dedupsnaps.Find(bson.M{"config": confignum}).All(&results)
  for i := 0; i < len(results); i++ {
    dedup[results[i].Key] = ClientReply{results[i].Value, results[i].Err, results[i].Counter}
  }
  return dedup
}

func (st *Storage) writeInBackground() {
  current := 0
  for {
    if st.applied > current {
      currentWrite := st.writeLog[current]
      var err error
      if !currentWrite.dbok {
        err = st.db.Insert(&KVPair{currentWrite.shard, currentWrite.key, currentWrite.value})
      } else {
        err = st.db.Update(bson.M{"shard": currentWrite.shard, "key": currentWrite.key}, bson.M{"$set": bson.M{"value": currentWrite.value}})
      }
      if err != nil {
        panic(err)
      }
      delete(st.writeLog, current)
      current++
    }
    time.Sleep(250 * time.Millisecond)
  }
}

func (st *Storage) closeDBConnection() {
  st.dbSession.Close()
}
