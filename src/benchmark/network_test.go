package benchmark

import "testing"
import "shardkv"
import "shardmaster"

import "fmt"
import "runtime"
import "time"

// Specific scenario: 3 machines.
// 1 shardmaster per machine. [6824]
// 3 shards, 1 shardkv server per group. [6825-6827]
// 1 client per machine.

var SERVERS = []string{"simple.mit.edu", "nextcode.mit.edu", "nextres.mit.edu"}
const PORT0 = 6824

// Run this with parameter 0, 1, or 2.
const Index = 0

const NumOperations = 100

func TestNetwork(t *testing.T) {
  runtime.GOMAXPROCS(4)
  shardmaster.Network = "tcp"  // set connection types to tcp
  shardkv.Network = "tcp"

  // Start shardmster
  fmt.Printf("Starting shardmaster...\n")
  smHosts := make([]string, 3)
  for i := 0; i < 3; i++ {
    smHosts[i] = fmt.Sprintf("%s:%d", SERVERS[i], PORT0)
  }
  shardmaster.StartServer(smHosts, Index)

  // Start ShardKV server
  fmt.Printf("Starting shard server...\n")
  gids := make([]int64, 3)
  skvHosts := make([][]string, 3)
  for i := 0; i < 3; i++ {
    gids[i] = int64(100 + i)
    skvHosts[i] = make([]string, 3)
    for j := 0; j < 3; j++ {
      skvHosts[i][j] = fmt.Sprintf("%s:%d", SERVERS[j], PORT0 + 1 + i)
    }
    shardkv.StartServer(gids[i], smHosts, skvHosts[i], Index)
  }

  // Start shardmaster clerk, if this is the first machine.
  fmt.Printf("Starting shardmaster clerk...\n")
  if Index == 0 {
    smClerk := shardmaster.MakeClerk(smHosts)
    for i := 0; i < 3; i++ {
      smClerk.Join(gids[i], skvHosts[i])
    }
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
