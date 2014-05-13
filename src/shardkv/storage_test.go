package shardkv

import "testing"
import "strconv"
//import "time"
import "fmt"
import "math/rand"

func TestCacheBasic(t *testing.T) {
  fmt.Printf("Test: Cache Basic Put/Get\n")
  
  cache := MakeCache(1000000)
  numPuts := 500
  numGets := 500
  for i := 0; i < numPuts; i++ {
    cache.Put(strconv.Itoa(i), strconv.Itoa(i*i))
  }
  newSize := cache.Size()
  fmt.Printf("Size after %v puts: %v\n", numPuts, newSize)
  for i := 0; i < numGets; i++ {
    value, ok := cache.Get(strconv.Itoa(i))
    if !ok || value != strconv.Itoa(i*i) {
      t.Fatalf("Get got wrong value!; value=%v, expected=%v", value, i*i);
    }
  }
  for i := 0; i < numPuts; i++ {
    ok := cache.Delete(strconv.Itoa(i))
    if !ok {
      t.Fatalf("Value which should've existed could not be removed!")
    }
  }
  afterSize := cache.Size()
  fmt.Printf("Size after %v removes: %v\n", numPuts, afterSize)

  fmt.Printf(" ... Passed\n")
}

func TestCacheUpdates(t *testing.T) {
  fmt.Printf("Test: Cache Colliding Puts\n") 

  cache := MakeCache(1000000)
  numPuts := 50
  for i := 0; i < 50; i++ {
    cache.Put(strconv.Itoa(i), strconv.Itoa(i*i)) 
  }
  newSize := cache.Size()
  fmt.Printf("Size after %v distinct puts: %v\n", numPuts, newSize)
  for i := 0; i < 25; i++ {
    cache.Put(strconv.Itoa(2*i), strconv.Itoa(2*i))
  }
  newSize = cache.Size()
  fmt.Printf("Size after %v colliding puts: %v\n", numPuts, newSize)
  for i := 0; i < 50; i++ {
    value, ok := cache.Get(strconv.Itoa(i))
    if i % 2 == 0 && (!ok || value != strconv.Itoa(i)) {
      t.Fatalf("Get got wrong value!; value=%v, expected=%v", value, i)
    } else if i % 2 == 1 && (!ok || value != strconv.Itoa(i*i)) {
      t.Fatalf("Get got wrong value!; value=%v, expected=%v", value, i*i)
    }
  }

  fmt.Printf(" ... Passed\n")
}

func TestConcurrentOps(t *testing.T) {
  fmt.Printf("Test: Concurrent Puts/Gets\n")

  fmt.Printf(" ... Passed\n")
}

func TestCacheOverflow(t *testing.T) {
  fmt.Printf("Test: Cache Memory Overflow\n")

  cache := MakeCache(1000)
  for i := 0; i < 500; i++ {
    cache.Put(strconv.Itoa(i), strconv.Itoa(i*i))
  }
  newSize := cache.Size()
  if newSize > 1000 {
    t.Fatalf("Cache capacity overflowed! Oh noes!; capacity: %v, actual size: %v", 1000, newSize)
  }

  fmt.Printf(" ... Passed\n")
}


func TestStorageBasic(t *testing.T) {
  fmt.Printf("Test: Storage Basic Units\n")

  storage := MakeStorage(0, 200, "127.0.0.1:27017")
  storage.Clear()

  numPuts := 50
  for i := 0; i < numPuts; i++ {
    storage.Put(strconv.Itoa(i), strconv.Itoa(i*i), false, 1, 1)
  }
  for i := 0; i < numPuts; i++ {
    value := storage.Get(strconv.Itoa(i), 1)
    if value != strconv.Itoa(i*i) {
      t.Fatalf("Get got wrong value!; value=%v, expected=%v", value, i*i)
    }
  }
  storage.closeDBConnection()
  fmt.Printf(" ... Passed\n")
}

func TestStorageSnapshots(t *testing.T) {
  fmt.Printf("Test: Storage Snapshotting\n")

  storage := MakeStorage(0, 200, "127.0.0.1:27017")
  storage.Clear()

  numPuts := 50
  for i := 0; i < numPuts; i++ {
    storage.Put(strconv.Itoa(i), strconv.Itoa(i*i), false, 1, 1)
  }

  configNum := 1
  dedup := make(map[string]ClientReply)
  dedup["what"] = ClientReply{Value: "wheat", Err: ErrWrongGroup, Counter: 47}
  dedup["gogo"] = ClientReply{Value: "hobo", Err: OK, Counter: 9001}
  dedup["bubbles"] = ClientReply{Value: "gorgonzola", Err: ErrNoKey, Counter: 9999999}
  dedup["hackey"] = ClientReply{Value: "chocolate", Err: OK, Counter: 1}
  dedup["socks"] = ClientReply{Value: "stinky", Err: OK, Counter: 476}
  storage.CreateSnapshot(configNum, dedup)

  piece, _ := storage.ReadSnapshotDB(1, 1, 0, true)
  storage.ReadSnapshotDedup(1)
  for k, v := range piece {
    cacheval := storage.Get(k, 1)
    if v != cacheval {
      t.Fatalf("Snapshot cache values differed from actual cache values!; actual=%v, expected=%v", v, cacheval)
    }
  }

  fmt.Printf(" ... Passed\n")
}

func TestStorageHeavy(t *testing.T) {
  fmt.Printf("Test: All Storage Operations, Heavy\n")

  storage := MakeStorage(0, 10000, "127.0.0.1:27017")
  storage.Clear()

  numPuts := 200
  numGets := 200
  
  for i := 0; i < numPuts; i++ {
    rando := int(rand.Int31n(15000))
    storage.Put(strconv.Itoa(rando), strconv.Itoa(rando * rando), false, 1, 1)
  }
  for i := 0; i < numPuts; i++ {
    storage.Put(strconv.Itoa(7 * i), strconv.Itoa(49 * i * i), false, 1, 1)
  }
  
  for i := 0; i < numGets; i++ {
    value := storage.Get(strconv.Itoa(7 * i), 1)
    if value != strconv.Itoa(49 * i * i) {
      t.Fatalf("Get got wrong value!; value=%v, expected=%v", value, strconv.Itoa(49 * i * i))
    }
  }

  configNum := 1
  dedup := make(map[string]ClientReply)
  dedup["what"] = ClientReply{Value: "wheat", Err: ErrWrongGroup, Counter: 47}
  dedup["gogo"] = ClientReply{Value: "hobo", Err: OK, Counter: 9001}
  dedup["bubbles"] = ClientReply{Value: "gorgonzola", Err: ErrNoKey, Counter: 9999999}
  dedup["hackey"] = ClientReply{Value: "chocolate", Err: OK, Counter: 1}
  dedup["socks"] = ClientReply{Value: "stinky", Err: OK, Counter: 476}
  storage.CreateSnapshot(configNum, dedup)

  configNum++
  for i := 0; i < numPuts; i++ {
    rando := int(rand.Int31n(15000))
    storage.Put(strconv.Itoa(rando), strconv.Itoa(rando * rando), false, 2, 2)
    fmt.Println(i)
  }

  dedup = make(map[string]ClientReply)
  dedup["bobobo"] = ClientReply{Value: "hokeypokey", Err: OK, Counter: 4381094}
  dedup["sesquicentennial"] = ClientReply{Value: "lackadaisical", Err: ErrWrongGroup, Counter: 329482903}
  dedup["solo"] = ClientReply{Value: "teddygrahams", Err: ErrNoKey, Counter: 9999999}
  dedup["hotpockets"] = ClientReply{Value: "kirakira", Err: OK, Counter: 1}
  dedup["bilirubin"] = ClientReply{Value: "entimentity", Err: OK, Counter: 476}
  storage.CreateSnapshot(configNum, dedup)

  configNum++

  finished := false
  index := 0
  for !finished {
    _, fin := storage.ReadSnapshotDB(1, 1, index, true)
    finished = fin 
    index++
  }
  
  finished = false
  index = 0
  for !finished {
    _, fin := storage.ReadSnapshotDB(1, 1, index, false)
    finished = fin 
    index++
  }

  finished = false
  index = 0
  for !finished {
    _, fin := storage.ReadSnapshotDB(1, 2, index, true)
    finished = fin 
    index++
  }
  
  finished = false
  index = 0
  for !finished {
    _, fin := storage.ReadSnapshotDB(1, 2, index, false)
    finished = fin 
    index++
  }
  
  storage.ReadSnapshotDedup(1)
  finished = false
  index = 0
  for !finished {
    _, fin := storage.ReadSnapshotDB(2, 1, index, true)
    finished = fin
    index++
  }

  finished = false
  index = 0
  for !finished {
    _, fin := storage.ReadSnapshotDB(2, 1, index, false)
    finished = fin
    index++
  }

  finished = false
  index = 0
  for !finished {
    _, fin := storage.ReadSnapshotDB(2, 2, index, true)
    finished = fin
    index++
  }

  finished = false
  index = 0
  for !finished {
    _, fin := storage.ReadSnapshotDB(2, 2, index, false)
    finished = fin
    index++
  }
  storage.ReadSnapshotDedup(2)

  fmt.Printf(" ... Passed\n")
}
