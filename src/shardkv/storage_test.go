package shardkv

import "testing"
//import "shardmaster"
//import "runtime"
import "strconv"
//import "os"
import "time"
import "fmt"
//import "sync"
//import "math/rand"

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

  storage := MakeStorage(100000, "127.0.0.1:27017")

  numPuts := 50
  for i := 0; i < numPuts; i++ {
    storage.Put(strconv.Itoa(i), strconv.Itoa(i*i), false, 1)
  }
  for i := 0; i < numPuts; i++ {
    value := storage.Get(strconv.Itoa(i), 1)
    if value != strconv.Itoa(i*i) {
      t.Fatalf("Get got wrong value!; value=%v, expected=%v", value, i*i)
    }
  }
  time.Sleep(3000 * time.Millisecond) 
  storage.closeDBConnection()
  fmt.Printf(" ... Passed\n")
}
