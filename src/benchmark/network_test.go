package benchmark

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

const NumOperations = 100

// Run this with parameter 0, 1, or 2.
func setup(index int) {
  runtime.GOMAXPROCS(4)

  // Start shardmster
  smHosts := make([]string, 3)
  smHosts[0] = SERVER1
  smHosts[1] = SERVER2
  smHosts[2] = SERVER3
  sm := shardmaster.StartServer(smHosts, index)
  smClerk := shardmaster.MakeClerk(smHosts)

  // Start ShardKV server
  gids := make([]int64, 3)
  skvHosts := make([]string, 3)
  skvHosts[0] = SERVER1
  skvHosts[1] = SERVER2
  skvHosts[2] = SERVER3
  skvs := make([]*shardkv.ShardKV, 3)
  for i := 0; i < 3; i++ {
    gids[i] = int64(100 + i)
    skvs[i] = shardkv.StartServer(gids[i], smHosts, skvHosts, i)
    smClerk.Join(gids[i], skvHosts)
  }

  // Start client
  client := shardkv.MakeClerk(smHosts)

  fmt.Printf("%s\n", sm)  // print sm to avoid go errors

  startTime := time.Now().UnixNano()
  for i := 0; i < NumOperations; i++ {
    client.Put("a", "x")
    client.Get("a")
  }
  endTime := time.Now().UnixNano()

  latency := elapsed(startTime, endTime) / float64(NumOperations)

  fmt.Printf("Total num operations: %d\n", NumOperations)
  fmt.Printf("Latency: %.3f s\n", latency)
  fmt.Printf("Note: a put() and get() query make up one operation.\n")
}
