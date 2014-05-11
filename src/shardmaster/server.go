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
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num

  localLog map[int]Op
  openRequests map[string]int
  horizon int
  maxConfigNum int

  paxosLogFile string
  enc *gob.Encoder
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

type Op struct {
  Type string
  ID string
  GID int64
  Servers []string
  Shard int
  Num int
}

// PERSISTENCE
func appendPaxosLog(enc *gob.Encoder, op Op, seq int) {
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
  var seq int
  var op Op
  dec.Decode(&seq)
  dec.Decode(&op)
}

func (sm *ShardMaster) logPaxos() {
  current := 0
  f, err := os.OpenFile(sm.paxosLogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)  // note: file never closed
  if err != nil {
    log.Fatal(err)
  }
  sm.enc = gob.NewEncoder(f)
  for {
    if sm.horizon > current {
      appendPaxosLog(sm.enc, sm.localLog[current], current)
      f.Sync()
      delete(sm.localLog, current)
      current++
    }
    time.Sleep(25 * time.Millisecond)
  }
}

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
    if v - 1 < minimum {
      minimum = v - 1
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
  for i, v := range newCon.Shards {
    if v == 0 {
      newCon.Shards[i] = GID
    }
  }
  for {
    minGroup, minCount := getMin(newCon)
    _, maxCount, lastShard := getMax(newCon)
    if maxCount - minCount < 2 {
      break
    }
    newCon.Shards[lastShard] = minGroup
  }
  sm.configs = append(sm.configs, newCon)
  sm.maxConfigNum++
}

func (sm *ShardMaster) ApplyLeave(GID int64) {
  newCon := copyConfig(sm.configs[sm.maxConfigNum])
  newCon.Num++
  delete(newCon.Groups, GID)
  var randomGroup int64

  for k, _ := range newCon.Groups {
    randomGroup = k
    break
  }

  for i, v := range newCon.Shards {
    if v == GID {
      newCon.Shards[i] = randomGroup
    }
  }

  for {
    minGroup, minCount := getMin(newCon)
    _, maxCount, lastShard := getMax(newCon)
    if maxCount - minCount < 2 {
      break
    }
    newCon.Shards[lastShard] = minGroup
  }
  sm.configs = append(sm.configs, newCon)
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
  for i := sm.horizon; i <= seqNum; i++ {
    decided, _ := sm.px.Status(sm.horizon)
    if !decided {
      noOp := Op{"Query", "noopID", 0, make([]string, 0), 0, 0}
      sm.px.Start(i, noOp)
    }
    decidedOp := sm.PollDecidedValue(i)
    sm.localLog[seqNum] = decidedOp
    sm.ApplyOp(decidedOp, i)
  }
  sm.horizon = seqNum + 1;
  sm.CallSafeDone()
}

func (sm *ShardMaster) ProposeOp(op Op) (Op, int) {
  for !sm.dead {
    sm.mu.Lock()
    seq := sm.px.Max() + 1
    sm.px.Start(seq, op)
    sm.openRequests[op.ID] = seq
    sm.mu.Unlock()

    decidedOp := sm.PollDecidedValue(seq)

    sm.mu.Lock()
    if decidedOp.ID == op.ID {
      sm.mu.Unlock()
      delete(sm.openRequests, op.ID)
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
  _, _ = sm.ProposeOp(op)
  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  id := getUniqueKey()
  op := Op{"Move", id, args.GID, make([]string, 0), args.Shard, 0}
  _, _ = sm.ProposeOp(op)
  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  id := getUniqueKey()
  op := Op{"Query", id, 0, make([]string, 0), 0, args.Num}

  decidedOp, seq := sm.ProposeOp(op)
  sm.mu.Lock()
  defer sm.mu.Unlock()
  sm.SyncUntil(seq)

  if decidedOp.Num < 0 || decidedOp.Num > sm.maxConfigNum {
    reply.Config = copyConfig(sm.configs[sm.maxConfigNum])
  } else {
    reply.Config = copyConfig(sm.configs[decidedOp.Num])
  }
  return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
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

  sm.localLog = make(map[int]Op)
  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  sm.openRequests = make(map[string]int)
  sm.horizon = 0
  sm.maxConfigNum = 0

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  sm.paxosLogFile = fmt.Sprintf("logs/sm_paxos_log_%d.log", sm.me)
  os.Create(sm.paxosLogFile)

  go sm.logPaxos()

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
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
