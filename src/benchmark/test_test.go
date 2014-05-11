package benchmark

import "testing"
import "shardmaster"
import "shardkv"

import "fmt"
import "os"
import "runtime"
import "strconv"
import "time"

type SetupArgs struct {
  NumMasters int  // # shardmasters
  NumGroups int  // # groups serving shards
  NumReplicas int  // # servers in each group

  NumClients int  // # clients requesting in parallel
  NumProcesses int  // # actual parallelism
  NumOperations int  // # get/put queries per client
}

type System struct {
  ShardMasterHosts []string
  ShardMasters []*shardmaster.ShardMaster
  ShardMasterClerk *shardmaster.Clerk
  GroupIDs []int64
  ShardKVHosts [][]string
  ShardKVs [][]*shardkv.ShardKV
  Clerks []*shardkv.Clerk
}

func port(tag string, host int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  os.Mkdir(s, 0777)
  s += "skv-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += tag + "-"
  s += strconv.Itoa(host)
  return s
}

// Start all servers/clients on local machine.
// Returns a system and a clean function
func localSetup(name string, args SetupArgs) (System, func()) {
  runtime.GOMAXPROCS(args.NumProcesses)

  // Start shardmasters
  smHosts := make([]string, args.NumMasters)
  sms := make([]*shardmaster.ShardMaster, args.NumMasters)
  for i := 0; i < args.NumMasters; i++ {
    smHosts[i] = port(name + "m", i)
    sms[i] = shardmaster.StartServer(smHosts, i)
  }
  smClerk := shardmaster.MakeClerk(smHosts)

  // Start ShardKV servers
  gids := make([]int64, args.NumGroups)
  skvHosts := make([][]string, args.NumGroups)
  skvs := make([][]*shardkv.ShardKV, args.NumGroups)
  for i := 0; i < args.NumGroups; i++ {
    gids[i] = int64(100 + i)
    skvHosts[i] = make([]string, args.NumReplicas)
    skvs[i] = make([]*shardkv.ShardKV, args.NumReplicas)
    for j := 0; j < args.NumReplicas; j++ {
      skvHosts[i][j] = port(name + "s", i * args.NumReplicas + j)
      skvs[i][j] = shardkv.StartServer(gids[i], smHosts, skvHosts[i], j)
    }
    smClerk.Join(gids[i], skvHosts[i])
  }

  // Start clients
  clerks := make([]*shardkv.Clerk, args.NumClients)
  for i := 0; i < args.NumClients; i++ {
    clerks[i] = shardkv.MakeClerk(smHosts)
  }

  system := System{
    ShardMasterHosts: smHosts,
    ShardMasters: sms,
    ShardMasterClerk: smClerk,
    GroupIDs: gids,
    ShardKVHosts: skvHosts,
    ShardKVs: skvs,
    Clerks: clerks,
  }

  clean := func() {
    for i := 0; i < args.NumMasters; i++ {
      sms[i].Kill()
    }
    for i := 0; i < args.NumGroups; i++ {
      for j := 0; j < args.NumReplicas; j++ {
        skvs[i][j].Kill()
      }
    }
  }

  return system, clean
}

func elapsed(start int64, end int64) float64 {
  return float64(end - start) / 1000000000.0
}

// Helper function for common code across benchmarks
func TesterHelper(name string, args SetupArgs) {
  system, clean := localSetup(name, args)
  defer clean()

  fmt.Printf("Testing benchmark %s\n", name)

  done := make(chan bool)
  counter := 0  // # instructions completed, for throughput
  totalLatency := 0.0  // total latency, in time
  for i := 0; i < args.NumClients; i++ {
    go func(client *shardkv.Clerk) {
      timeBegin := time.Now().UnixNano()
      for j := 0; j < args.NumOperations; j++ {
        client.Put("a", "x")
        client.Get("a")
        counter++
        if (j + 1) % 100 == 0 {
          fmt.Printf("placed operation %d\n", j + 1)
        }
      }
      timeEnd := time.Now().UnixNano()
      totalLatency += elapsed(timeBegin, timeEnd)
      done <- true
    }(system.Clerks[i])
  }

  // Calculate throughput, only for middle time frame.
  throughput := 0.0
  go func() {
    for ; counter < args.NumClients * args.NumOperations / 4; {
      time.Sleep(100 * time.Millisecond)
    }
    timeBegin := time.Now().UnixNano()
    counterBegin := counter
    for ; counter < args.NumClients * args.NumOperations * 3 / 4; {
      time.Sleep(100 * time.Millisecond)
    }
    timeEnd := time.Now().UnixNano()
    counterEnd := counter
    throughput = float64(counterEnd - counterBegin) / elapsed(timeBegin, timeEnd)
    done <- true
  }()

  // Wait for all clients, and for the throughput calculator, to finish.
  for i := 0; i < args.NumClients; i++ {
    <- done
  }
  <- done

  // Calculate latency.
  latency := totalLatency / float64(args.NumClients * args.NumOperations)

  fmt.Printf("Total num operations: %d\n", args.NumOperations)
  fmt.Printf("Throughput: %.3f op/s\n", throughput)
  fmt.Printf("Latency: %.3f s\n", latency)
  fmt.Printf("Note: a put() and get() query make up one operation.\n")
}

func TestSingleClient(t *testing.T) {
  args := SetupArgs{
    NumMasters: 3,
    NumGroups: 3,
    NumReplicas: 3,
    NumClients: 1,
    NumProcesses: 4,
    NumOperations: 1000,
  }
  TesterHelper("singleclient", args)
}
