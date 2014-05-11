package benchmark

import "testing"
import "shardkv"
import "shardmaster"

import "fmt"
import "runtime"
import "time"

// Specific scenario: 3 machines.
// 1 shardmaster per machine.
// 3 shards, 1 shardkv server per group.
// 1 client per machine.

const SERVER1 = "simple.mit.edu:8080"
const SERVER2 = "nextcode.mit.edu:8080"
const SERVER3 = "nextres.mit.edu:8080"

// Run this with parameter 0, 1, or 2.
const Index = 0

const NumOperations = 100

func TestNetwork(t *testing.T) {
  runtime.GOMAXPROCS(4)

  // Start shardmster
  fmt.Printf("Starting shardmaster...\n")
  smHosts := make([]string, 3)
  smHosts[0] = SERVER1
  smHosts[1] = SERVER2
  smHosts[2] = SERVER3
  shardmaster.StartServer(smHosts, Index)
  smClerk := shardmaster.MakeClerk(smHosts)

  // Start ShardKV server
  fmt.Printf("Starting shard server...\n")
  gids := make([]int64, 3)
  skvHosts := make([]string, 3)
  skvHosts[0] = SERVER1
  skvHosts[1] = SERVER2
  skvHosts[2] = SERVER3
  for i := 0; i < 3; i++ {
    gids[i] = int64(100 + i)
    shardkv.StartServer(gids[i], smHosts, skvHosts, Index)
    smClerk.Join(gids[i], skvHosts)
  }

  // Start client
  fmt.Printf("Starting client...\n")
  client := shardkv.MakeClerk(smHosts)

  // Make requests
  time.Sleep(5000 * time.Millisecond)
  startTime := time.Now().UnixNano()
  for i := 0; i < NumOperations; i++ {
    if i % 100 == 0 {
      fmt.Printf("sending operation %d\n", i)
    }
    client.Put("a", "x")
    client.Get("a")
  }
  endTime := time.Now().UnixNano()

  latency := elapsed(startTime, endTime) / float64(NumOperations)

  fmt.Printf("Total num operations: %d\n", NumOperations)
  fmt.Printf("Latency: %.3f s\n", latency)
  fmt.Printf("Note: a put() and get() query make up one operation.\n")
}
