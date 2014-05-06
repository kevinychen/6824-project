package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"
import "strings"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

type ServerState struct {
  shardMap map[int]map[string]string
  dedup map[string]ClientReply
}

type ClientReply struct {
  Value string
  Err Err
  Counter int64
}

type Op struct {
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Type string
  Key string
  Value string
  DoHash bool
  ConfigNum int
  Config shardmaster.Config
  ID string
}

type ShardKV struct {
  mu sync.Mutex
  configLock sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos
  uid string

  gid int64 // my replica group ID

  // Your definitions here.
  snapshots map[int]ServerState
  current ServerState
  localLog map[string]Op
  results map[string]ClientReply
  configs map[int]shardmaster.Config
  Counter int64
  horizon int
  max int
  configNum int
}

func (kv *ShardKV) PollDecidedValue(seq int) Op {
  // Wait for response
  to := 10 * time.Millisecond
  for {
    decided, returnOp := kv.px.Status(seq)
    if decided {
      decidedOp := returnOp.(Op)
      return decidedOp
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
}

// Parse Client ID and counter
func parseID(totalID string) (string, int64) {
  split := strings.Split(totalID, ":")
  counter, _ := strconv.ParseInt(split[1], 10, 64)
  return split[0], counter
}

func (kv *ShardKV) CallSafeDone() {
  minimum := kv.horizon
  kv.px.Done(minimum)
  _ = kv.px.Min()
}

// Locked
func (kv *ShardKV) GetMaxSeq() int {
  seq := kv.px.Max() + 1
  if seq <= kv.max {
    kv.max++
    seq = kv.max
  }
  if seq > kv.max {
    kv.max = seq
  }
  return seq
}

// Locked
func (kv *ShardKV) Pull(args *PullArgs, reply *PullReply) error {
  desiredConfig := args.ConfigNum
  for {
    kv.configLock.Lock()
    if kv.configNum > desiredConfig {
      kv.configLock.Unlock()
      break
    }
    kv.configLock.Unlock()
    time.Sleep(20 * time.Millisecond)
  }
  reply.ShardMap = make(map[int]map[string]string)
  for key, value := range kv.snapshots[desiredConfig].shardMap {
    reply.ShardMap[key] = make(map[string]string)
    for k2, v2 := range value {
      reply.ShardMap[key][k2] = v2
    }
  }
  reply.Dedup = make(map[string]ClientReply)
  for key, value := range kv.snapshots[desiredConfig].dedup {
    reply.Dedup[key] = ClientReply{Value:value.Value, Err:value.Err,
    Counter:value.Counter}
  }
  reply.Err = OK
  return nil
}

// Locked
func (kv *ShardKV) AskForShard(gid int64, configNum int, shard int) {
  for !kv.dead {
    if configNum == 0 {
      return
    }
    servers, ok := kv.configs[configNum].Groups[gid]
    if ok {
      for _, srv := range servers {
        args := &PullArgs{}
        args.ConfigNum = configNum
        reply := new(PullReply)
        callOk := call(srv, "ShardKV.Pull", args, &reply)
        if callOk && reply.Err == OK {
          // Pull state over
          for key, value := range reply.ShardMap[shard] {
            if kv.current.shardMap[shard] == nil {
              kv.current.shardMap[shard] = make(map[string]string)
            }
            kv.current.shardMap[shard][key] = value
          }
          for key, value := range reply.Dedup {
            if value.Counter > kv.current.dedup[key].Counter {
              kv.current.dedup[key] = ClientReply{Value:value.Value, Err:value.Err, Counter:value.Counter}
            }
          }
          return
        } else {
        }
      }
    }
    time.Sleep(100 * time.Millisecond)
  }
}

// Locked
func (kv *ShardKV) SyncShards(configNum int) {
  prevConfig := kv.configs[configNum - 1]
  newConfig := kv.configs[configNum]
  for shard, group := range newConfig.Shards {
    // new shard we don't have
    if group == kv.gid && group != prevConfig.Shards[shard] {
      kv.AskForShard(prevConfig.Shards[shard], configNum - 1, shard)
    }
  }

}

// Call Under Lock
func (kv *ShardKV) Reconfigure() {
  latestConfig := kv.sm.Query(-1)
  for i := kv.configNum + 1; i <= latestConfig.Num; i++ {
    con := kv.sm.Query(i)
    op := Op{Type: "Reconfigure", ConfigNum:con.Num,
    Config: con, ID: fmt.Sprintf("configuration:%d",con.Num)}
    for {
      seq := kv.GetMaxSeq()

      // Call Start
      kv.px.Start(seq, op)

      decidedOp := kv.PollDecidedValue(seq)
      if decidedOp.ID == op.ID {
        kv.SyncUntil(seq)
        return
      }
    }
  }
}

// Locked
func (kv *ShardKV) SyncUntil(seqNum int) {
  for i := kv.horizon; i <= seqNum; i++ {
    decided, _ := kv.px.Status(kv.horizon)
    if !decided {
      noOp := Op{Type:"Get", Key:"noopID", ID:NoOpID}
      kv.px.Start(i, noOp)
    }
    decidedOp := kv.PollDecidedValue(i)
    kv.ApplyOp(decidedOp, i)
  }
  kv.horizon = seqNum
}

// Locked
func (kv *ShardKV) TakeSnapshot(confignum int) {
  kv.configLock.Lock()
  defer kv.configLock.Unlock()
  if confignum - kv.configNum != 1 {
    return
  }
  data := ServerState{make(map[int]map[string]string), make(map[string]ClientReply)}
  for key, value := range kv.current.shardMap {
    data.shardMap[key] = make(map[string]string)
    for k2, v2 := range value {
      data.shardMap[key][k2] = v2
    }
  }

  for key, value := range kv.current.dedup {
    data.dedup[key] = ClientReply{Value:value.Value, Err:value.Err,
    Counter:value.Counter}
  }

  kv.snapshots[kv.configNum] = data
  kv.configNum = confignum
}

// Apply Op locally
// Locked
func (kv *ShardKV) ApplyOp(op Op, seqNum int) {
  key := op.Key
  val := op.Value
  doHash := op.DoHash
  id := op.ID
  clientConfigNum := op.ConfigNum
  kvConfigNum := kv.configNum
  shardNum := key2shard(key)

  if op.Type != "Reconfigure" && (clientConfigNum != kvConfigNum ||
    kv.configs[kvConfigNum].Shards[shardNum] != kv.gid) {
    kv.results[id] = ClientReply{Err:ErrorString}
    return
  }

  clientID, counter := parseID(id)
  creply, _ := kv.current.dedup[clientID]
  if creply.Counter >= counter && creply.Counter > 0 {
    kv.results[id] = ClientReply{Value:creply.Value, Err:creply.Err, Counter:creply.Counter}
    return
  }

  if kv.current.shardMap[shardNum] == nil {
    kv.current.shardMap[shardNum] = make(map[string]string)
  }
  if op.Type == "Put" {
    prev := kv.current.shardMap[shardNum][key]
    // Put hash
    if doHash {
      toBeHashed := prev + val
      hash := strconv.Itoa(int(hash(toBeHashed)))
      kv.current.shardMap[shardNum][key] = hash
      kv.results[id] = ClientReply{Value:prev, Err:OK, Counter:counter}
    } else {
      kv.current.shardMap[shardNum][key] = val
      kv.results[id] = ClientReply{Value:prev, Err:OK, Counter:counter}
    }
    kv.current.dedup[clientID] = ClientReply{Value:prev, Counter: counter, Err:OK}
  } else if op.Type == "Reconfigure" {
    _, ok := kv.configs[op.Config.Num]
    if ok || op.Config.Num - kv.configNum != 1 {
      return
    }
    kv.configs[op.Config.Num] = op.Config
    kv.TakeSnapshot(op.Config.Num)
    kv.SyncShards(op.Config.Num)
  } else {
    value := kv.current.shardMap[shardNum][key]
    kv.results[id] = ClientReply{Value:value, Err:OK, Counter:counter}
    kv.current.dedup[clientID] = ClientReply{Value:value, Counter: counter, Err:OK}
  }
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  op := Op{Type:"Get", Key:args.Key, ID:args.ID, ConfigNum:args.ConfigNum}
  
  for {
    seq := kv.GetMaxSeq()
    // Call Start
    kv.px.Start(seq, op)

    decidedOp := kv.PollDecidedValue(seq)
    if decidedOp.ID == op.ID {
      kv.SyncUntil(seq)
      reply.Err = kv.results[op.ID].Err
      reply.Value = kv.results[op.ID].Value
      return nil
    }
  }
  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  op := Op{Type:"Put", Key:args.Key, ID:args.ID, ConfigNum:args.ConfigNum, Value:args.Value, DoHash:args.DoHash}

  for {
    seq := kv.GetMaxSeq()
    // Call Start
    kv.px.Start(seq, op)

    decidedOp := kv.PollDecidedValue(seq)
    if decidedOp.ID == op.ID {
      kv.SyncUntil(seq)
      reply.PreviousValue = kv.results[op.ID].Value
      reply.Err = kv.results[op.ID].Err
      return nil
    }
  }
  return nil
}


//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  kv.Reconfigure()
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})
  gob.Register(ServerState{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  // Don't call Join().
  kv.localLog = make(map[string]Op)
  kv.Counter = 1
  kv.horizon = 0
  kv.configNum = -1
  kv.max = 0
  kv.snapshots = make(map[int]ServerState)

  kv.current = ServerState{make(map[int]map[string]string), make(map[string]ClientReply)}
  kv.results = make(map[string]ClientReply)
  kv.configs = make(map[int]shardmaster.Config)
  kv.configs[-1] = shardmaster.Config{}

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)
  kv.uid = strconv.FormatInt(nrand(), 10)


  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            DPrintf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        DPrintf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
