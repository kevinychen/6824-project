package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "math/big"
import crand "crypto/rand"
import "time"
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

type ShardMaster struct {
  mu sync.Mutex
  obliviousLock sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  id string

  configs []Config // indexed by config num

  localLog map[int]Op
  openRequests map[string]int
  horizon int
  maxConfigNum int

  obliviousMap map[int]string
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := crand.Int(crand.Reader, max)
  x := bigx.Int64()
  return x
}

func getUniqueKey() string {
  return strconv.FormatInt(nrand(), 10) + ":" + strconv.FormatInt(nrand(), 10)
}

// PERSISTENCE
// func (sm *ShardMaster) appendOperation(seq int, op Op) {
//   sm.enc.Encode(seq)
//   sm.enc.Encode(op)
// }

// END PERSISTENCE

type Op struct {
  Type string
  ID string
  GID int64
  Servers []string
  Shard int
  Num int
}

// PERSISTENCE

// Call under Lock
func (sm *ShardMaster) RecoverFromLog() {
  sm.SyncUntil(sm.px.Max())
}

/*
func (sm *ShardMaster) appendPaxosLog(op Op, seq int) {
  enc.Encode(seq)
  enc.Encode(op)
}

func rollback(sm *ShardMaster) {
  // This function doesn't do anything; it's just
  //   a reference for gob decoding.
  f, err := os.Open(sm.paxosLogFile)
  if err != nil {
    log.Fatal(err)
  }
  dec := gob.NewDecoder(f)
  for {
    var err1, err2 error
    var seq int
    var op Op
    err1 = dec.Decode(&seq)
    err2 = dec.Decode(&op)
    if err1 != nil || err2 != nil {
      break
    }
  }
}
*/
// END PERSISTENCE


// Get Status
func (sm *ShardMaster) PollDecidedValue(seq int) Op {
  // Wait for response
  to := 10 * time.Millisecond
  for {
    decided, returnOp := sm.px.Status(seq)
    if decided {
      decidedOp := returnOp.(Op)
      return decidedOp
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
  return Op{"Query", "noopID", 0, make([]string, 0), 0, 0}
}

func (sm *ShardMaster) CallSafeDone() {
  minimum := sm.horizon
  for _, v := range sm.openRequests {
    if v < minimum {
      minimum = v
    }
  }
  sm.px.Done(minimum)
  _ = sm.px.Min()
}

func getMin(config Config) (int64, int) {
  minShards := NShards << 3
  minGroup := int64(0)
  for key, _ := range config.Groups {
    counter := 0
    for _, v := range config.Shards {
      if v == key {
        counter++
      }
    }
    if counter < minShards {
      minShards = counter
      minGroup = key
    }
  }
  return minGroup, minShards
}

func getMax(config Config) (int64, int, int) {
  maxShards := -1
  maxGroup := int64(0)
  lastShard := -1
  for key, _ := range config.Groups {
    counter := 0
    temp := -1
    for i, v := range config.Shards {
      if v == key {
        counter++
        temp = i
      }
    }
    if counter > maxShards {
      maxShards = counter
      maxGroup = key
      lastShard = temp
    }
  }
  return maxGroup, maxShards, lastShard
}

func copyConfig(config Config) Config {
  var newShards [NShards]int64
  tempConfig := Config{config.Num, newShards, make(map[int64][]string)}
  for i, v := range config.Shards {
    tempConfig.Shards[i] = v
  }
  for k, v := range config.Groups {
    tempConfig.Groups[k] = v
  }
  return tempConfig
}

func (sm *ShardMaster) ApplyJoin(GID int64, Servers []string) {
  newCon := copyConfig(sm.configs[sm.maxConfigNum])
  newCon.Num++
  newCon.Groups[GID] = Servers
  newCon.Shards = distributeShards(newCon.Shards, newCon.Groups)
  sm.configs = append(sm.configs, newCon)
  DPrintf("%d JOIN %d: %d %v\n", sm.me, newCon.Num, GID, newCon.Shards)
  sm.maxConfigNum++
}

func (sm *ShardMaster) ApplyLeave(GID int64) {
  newCon := copyConfig(sm.configs[sm.maxConfigNum])
  newCon.Num++
  delete(newCon.Groups, GID)
  newCon.Shards = distributeShards(newCon.Shards, newCon.Groups)
  sm.configs = append(sm.configs, newCon)
  DPrintf("%d LEAVE %d: %d %v\n", sm.me, newCon.Num, GID, newCon.Shards)
  sm.maxConfigNum++
}

func (sm *ShardMaster) ApplyMove(Shard int, GID int64) {
  newCon := copyConfig(sm.configs[sm.maxConfigNum])
  newCon.Num++
  newCon.Shards[Shard] = GID
  sm.configs = append(sm.configs, newCon)
  sm.maxConfigNum++
}

func (sm *ShardMaster) ApplyQuery() {
  // pass
}

func (sm *ShardMaster) ApplyOp(op Op, seqNum int) {
  if op.Type == "Join" {
    sm.ApplyJoin(op.GID, op.Servers)
  } else if op.Type == "Leave" {
    sm.ApplyLeave(op.GID)
  } else if op.Type == "Move" {
    sm.ApplyMove(op.Shard, op.GID)
  } else if op.Type == "Query" {
    sm.ApplyQuery()
  }
}

// Sync method, must be LOCKED
func (sm *ShardMaster) SyncUntil(seqNum int) {
  DPrintf("%d Start SYNC: %d\n", sm.me, sm.horizon)
  for i := sm.horizon; i <= seqNum; i++ {
    decided, _ := sm.px.Status(sm.horizon)
    if !decided {
      noOp := Op{"Query", "noopID", 0, make([]string, 0), 0, 0}
      sm.px.SlowStart(i, noOp)
    }
    decidedOp := sm.PollDecidedValue(i)
//    sm.localLog[seqNum] = decidedOp
    DPrintf("Seq %d: ", i)
    sm.ApplyOp(decidedOp, i)
  }
  if sm.horizon <= seqNum + 1 {
    sm.horizon = seqNum + 1;
  }
  sm.CallSafeDone()
  DPrintf("%d End SYNC: %d\n", sm.me, sm.horizon)
}

func (sm *ShardMaster) ProposeOp(op Op) (Op, int) {
  for !sm.dead {
    sm.mu.Lock()
    seq := sm.px.Max() + 1
    sm.px.FastStart(seq, op)
    sm.openRequests[op.ID] = seq
    sm.mu.Unlock()

    decidedOp := sm.PollDecidedValue(seq)

    sm.mu.Lock()
    if decidedOp.ID == op.ID {
      delete(sm.openRequests, op.ID)
      sm.mu.Unlock()
      return decidedOp, seq
    } else {
      time.Sleep(time.Millisecond)
      sm.mu.Unlock()
    }
  }
  return Op{}, -1
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  id := getUniqueKey()
  op := Op{"Join", id, args.GID, args.Servers, 0, 0}
  _, seq := sm.ProposeOp(op)
  sm.mu.Lock()
  defer sm.mu.Unlock()
  sm.SyncUntil(seq)
  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  id := getUniqueKey()
  op := Op{"Leave", id, args.GID, make([]string, 0), 0, 0}
  _, seq := sm.ProposeOp(op)
  sm.mu.Lock()
  defer sm.mu.Unlock()
  sm.SyncUntil(seq)
  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  id := getUniqueKey()
  op := Op{"Move", id, args.GID, make([]string, 0), args.Shard, 0}
  _, seq := sm.ProposeOp(op)
  sm.mu.Lock()
  defer sm.mu.Unlock()
  sm.SyncUntil(seq)
  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  id := getUniqueKey()
  op := Op{"Query", id, 0, make([]string, 0), 0, args.Num}

  decidedOp, seq := sm.ProposeOp(op)
  sm.mu.Lock()
  defer sm.mu.Unlock()
  sm.SyncUntil(seq)
//  sm.reset()


  if decidedOp.Num < 0 || decidedOp.Num > sm.maxConfigNum {
    reply.Config = copyConfig(sm.configs[sm.maxConfigNum])
  } else {
    reply.Config = copyConfig(sm.configs[decidedOp.Num])
  }
  return nil
}

// OBLIVIOUS REPLICATION

func (sm *ShardMaster) StoreHash(args *StoreHashArgs, reply *StoreHashReply) error {
  sm.obliviousLock.Lock()
  defer sm.obliviousLock.Unlock()
  sm.obliviousMap[args.Seq] = args.Hash
  return nil
}

func (sm *ShardMaster) List(args *ListArgs, reply *ListReply) error {
  sm.obliviousLock.Lock()
  defer sm.obliviousLock.Unlock()
  reply.Hashes = sm.obliviousMap
  return nil
}

// END OBLIVIOUS REPLICATION

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

func (sm *ShardMaster) reset() {
  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}
  sm.localLog = make(map[int]Op)
  sm.openRequests = make(map[string]int)

  sm.horizon = 0
  sm.maxConfigNum = 0

  sm.RecoverFromLog()
}

func (sm *ShardMaster) HardReset() {
  sm.mu.Lock()
  defer sm.mu.Unlock()

  /*
  fakeConfigs := make([]Config, 0)
  for _, value := range sm.configs {
    fakeConfigs = append(fakeConfigs, value)
  }
  */

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}
  sm.localLog = make(map[int]Op)
  sm.openRequests = make(map[string]int)

  sm.horizon = 0
  sm.maxConfigNum = 0

  sm.RecoverFromLog()

  /*
  for i, value := range fakeConfigs {
    if !reflect.DeepEqual(value, sm.configs[i]) {
      fmt.Printf("%v COMPARE TO %v\n", value, sm.configs[i])
      log.Fatal("")
    }
  }
  */
}

// Distribute shards among groups, minimizing shard movement
func distributeShards(Shards [NShards]int64, Groups map[int64][]string) [NShards]int64 {
  nGroups := len(Groups)
  shardsPerGroup := NShards / nGroups
  if shardsPerGroup < 1 { // at least 1 shard per group
    shardsPerGroup = 1
  }
  shardCounts := make(map[int64]int)

  // Initialize shard counts to 0
  for gid, _ := range Groups {
    shardCounts[gid] = 0
  }

  // First pass get shard counts
  for _, gid := range Shards {
    _, ok := Groups[gid]
    if ok {
      shardCounts[gid]++
    }
  }

  // Second pass
  for i, gid := range Shards {
    // Move shard if in invalid group or over average capacity
    _, ok := Groups[gid]
    if !ok || shardCounts[gid] > shardsPerGroup {
      minGid := int64(1 << 63 - 1)
      // Find min gid below capacity
      for newGid, _ := range Groups {
        if shardCounts[newGid] < shardsPerGroup && newGid < minGid { // Find below capacity group
          minGid = newGid
        }
      }
      if minGid < int64(1 << 63 - 1) {
        // Move shard to new group
        Shards[i] = minGid
        // Update shard counts
        shardCounts[gid]--
        shardCounts[minGid]++
      }
      // If none exists, then all groups at average capacity
    }
  }
  return Shards
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me
  sm.id = servers[me]

  sm.localLog = make(map[int]Op)
  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  sm.openRequests = make(map[string]int)
  sm.horizon = 0
  sm.maxConfigNum = 0

  sm.obliviousMap = make(map[int]string)

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)
  sm.px.DeleteBarrier = -1

  os.Remove(servers[me])
  l, e := net.Listen(Network, servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
