package benchmark

import "testing"
import "paxos"
import "shardkv"
import "shardmaster"

import "fmt"
import "runtime"
import "time"

// Specific scenario: 3 machines.
// 1 shardmaster per machine. [6824]
// 3 shards, 1 shardkv server per group. [6825-6827]
// 1 client per machine.

const N = 2
var SERVERS = []string{"simple.mit.edu", "nextcode.mit.edu", "nextres.mit.edu"}
const PORT0 = 6824

// Run this with parameter 0, 1, or 2.
const Index = 0

const NumOperations = 10

func TestNetwork(t *testing.T) {
  runtime.GOMAXPROCS(4)
  paxos.Network = "tcp"  // set connection types to tcp
  shardmaster.Network = "tcp"
  shardkv.Network = "tcp"

  // Start shardmster
  fmt.Printf("Starting shardmaster...\n")
  smHosts := make([]string, N)
  for i := 0; i < N; i++ {
    smHosts[i] = fmt.Sprintf("%s:%d", SERVERS[i], PORT0)
  }
  sm := shardmaster.StartServer(smHosts, Index)

  // Start ShardKV server
  fmt.Printf("Starting shard server...\n")
  gids := make([]int64, N)
  skvHosts := make([][]string, N)
  skvs := make([]*shardkv.ShardKV, N)
  for i := 0; i < N; i++ {
    gids[i] = int64(100 + i)
    skvHosts[i] = make([]string, N)
    for j := 0; j < N; j++ {
      skvHosts[i][j] = fmt.Sprintf("%s:%d", SERVERS[j], PORT0 + 1 + i)
    }
    skvs[i] = shardkv.StartServer(gids[i], smHosts, skvHosts[i], Index)
  }

  // Start shardmaster clerk, if this is the first machine.
  fmt.Printf("Starting shardmaster clerk...\n")
  if Index == 0 {
    smClerk := shardmaster.MakeClerk(smHosts)
    for i := 0; i < N; i++ {
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
    client.Put("a", "x")
    client.Get("a")
  }
  endTime := time.Now().UnixNano()

  latency := elapsed(startTime, endTime) / float64(NumOperations)

  fmt.Printf("Total num operations: %d\n", NumOperations)
  fmt.Printf("Latency: %.3f s\n", latency)
  fmt.Printf("Note: a put() and get() query make up one operation.\n")

  // Wait for every to finish, then clean up
  fmt.Printf("Cleaning up...\n")
  time.Sleep(10000 * time.Millisecond)
  sm.Kill()
  for i := 0; i < N; i++ {
    skvs[i].Kill()
  }
}
