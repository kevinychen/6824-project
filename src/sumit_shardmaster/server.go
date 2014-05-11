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
import "sort"
import "time"

type ShardMaster struct {
  mu sync.Mutex
  logLock sync.Mutex
  execLock sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  opLog map[int]Op
  maxOp int
}

type Op struct {
  // Your data here.
  OpType string
  GID int64
  Servers []string
  Shard int
  Num int
}

func opEquality(op1 Op, op2 Op) bool {
  if op1.OpType != op2.OpType {
    return false
  }
  if op1.GID != op2.GID {
    return false
  }
  if len(op1.Servers) != len(op2.Servers) {
    return false
  }
  for i := 0; i < len(op1.Servers); i++ {
    if op1.Servers[i] != op2.Servers[i] {
        return false
    }
  }
  if op1.Shard != op2.Shard {
    return false
  }
  if op1.Num != op2.Num {
    return false
  }
  return true
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  op := Op{"join", args.GID, args.Servers, -1, -1}
  seq := sm.Insert(op)
  sm.ExecutePrevious(seq, op)

  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  op := Op{"leave", args.GID, nil, -1, -1}
  seq := sm.Insert(op)
  sm.ExecutePrevious(seq, op)

  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  op := Op{"move", args.GID, nil, args.Shard, -1}
  seq := sm.Insert(op)
  sm.ExecutePrevious(seq, op)

  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  op := Op{"query", -1, nil, -1, args.Num}
  seq := sm.Insert(op)
  reply.Config = sm.ExecutePrevious(seq, op)

  return nil
}

func (sm *ShardMaster) ExecutePrevious(seq int, currentOp Op) Config {
    sm.execLock.Lock()
    defer sm.execLock.Unlock()

    if seq <= sm.maxOp {
        return sm.Execute(currentOp)
    } else {
        var config Config
        var quickfix int // Attempt at fixing problem with early sequence
        if sm.maxOp == 0 {
            quickfix = 0
        } else {
            quickfix = sm.maxOp + 1
        }
        for i := quickfix; i <= seq; i++ {
            sm.logLock.Lock()
            op, alreadyLogged := sm.opLog[i]
            sm.logLock.Unlock()
            if !alreadyLogged {
                decided := false
                var v interface{}
                decided, v = sm.px.Status(i)
                if !decided {
                    op = sm.PutNOP(i)
                } else {
                    op = v.(Op)
                    sm.logLock.Lock()
                    sm.opLog[i] = op
                    sm.logLock.Unlock()
                }
            }
            config = sm.Execute(op)
        }
        sm.maxOp = seq
        // clean up
        for key, _ := range sm.opLog {
            if key <= seq {
                delete(sm.opLog, key)
            }
        }
        sm.px.Done(seq)
        return config
    }
}

func (sm *ShardMaster) Execute(op Op) Config {
    if op.OpType == "join" {
        sm.ExecuteJoin(op)
        return Config{} 
    } else if op.OpType == "leave" {
        sm.ExecuteLeave(op)
        return Config{}
    } else if op.OpType == "move" {
        sm.ExecuteMove(op)
        return Config{}
    } else if op.OpType == "query" {
        return sm.ExecuteQuery(op)
    }
    return Config{}
}

func copyConfig(config Config) Config {
    newConfig := Config{}
    newConfig.Num = config.Num
    for i,_ := range newConfig.Shards {
        newConfig.Shards[i] = config.Shards[i]
    }
    newConfig.Groups = make(map[int64][]string)
    for k, v := range config.Groups {
       newConfig.Groups[k] = v 
    }
    return newConfig
}

func (sm *ShardMaster) ExecuteJoin(op Op) {
    currentConfig := sm.configs[len(sm.configs) - 1]
    newConfig := copyConfig(currentConfig)
    newConfig.Num += 1
    newConfig.Groups[op.GID] = op.Servers

    // get shard counts
    counts := make(map[int64]int)
    for i := 0; i < len(newConfig.Shards); i++ {
        _, in := counts[newConfig.Shards[i]]
        if in {
            counts[newConfig.Shards[i]] += 1
        } else {
            counts[newConfig.Shards[i]] = 1
        }
    }

    // get number of groups
    numGroups := len(newConfig.Groups) 
    //fmt.Println(numGroups)

    myshards := 0
    for i := 0; i < len(newConfig.Shards); i++ {
        if newConfig.Shards[i] == 0 {
            newConfig.Shards[i] = op.GID
            myshards += 1
            continue
        }
        if myshards > (10 / numGroups) {
            break
        }
        if (counts[newConfig.Shards[i]] - 1) >= (10 / numGroups) {
            counts[newConfig.Shards[i]] -= 1
            newConfig.Shards[i] = op.GID
            myshards += 1
        }
    }
    sm.configs = append(sm.configs, newConfig)
}

type Int64Slice []int64
func (a Int64Slice) Len() int {return len(a)}
func (a Int64Slice) Less(i, j int) bool {return a[i] < a[j]}
func (a Int64Slice) Swap(i, j int) {a[i], a[j] = a[j], a[i]}

func (arr Int64Slice) Sort() {
    sort.Sort(arr)
}

func (sm *ShardMaster) ExecuteLeave(op Op) {
    currentConfig := sm.configs[len(sm.configs) - 1]
    newConfig := copyConfig(currentConfig)
    newConfig.Num += 1
    delete(newConfig.Groups, op.GID)
  
    /*if sm.me == 0 {
        fmt.Println("Before Leave " + strconv.FormatInt(op.GID, 10))
        fmt.Println(currentConfig.Shards)
    }*/
    // get shard counts
    counts := make(map[int64]int)
    for i := 0; i < len(newConfig.Shards); i++ {
        _, in := counts[newConfig.Shards[i]]
        if in {
            counts[newConfig.Shards[i]] += 1
        } else {
            counts[newConfig.Shards[i]] = 1
        }
    }
    delete(counts, op.GID)

    // get number of groups
    numActiveGroups := len(counts) 

    keys := make(Int64Slice, len(newConfig.Groups))
    b := 0
    for k, _ := range(newConfig.Groups) {
        keys[b] = k
        b++
    }
    keys.Sort()
    for i := 0; i < len(newConfig.Shards); i++ {
        if newConfig.Shards[i] == op.GID {
            found := false
            for _, k := range keys {
                count, ok := counts[k]
                if !ok {
                    newConfig.Shards[i] = k
                    counts[k] = 1
                    found = true
                    break
                }
                if count < (10 / numActiveGroups) {
                    newConfig.Shards[i] = k
                    counts[k] += 1
                    found = true
                    break
                }
            }
            if found {
                continue 
            } else {
                for _, k := range keys {
                    if counts[k] < (10 / numActiveGroups + 1) {
                        newConfig.Shards[i] = k
                        counts[k] += 1
                        break
                    }
                }
            }
        }
    }
    /*if sm.me == 0 {
        fmt.Println("After Leave " + strconv.FormatInt(op.GID, 10))
        fmt.Println(newConfig.Shards)
    }*/
    sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) ExecuteMove(op Op) {
    currentConfig := copyConfig(sm.configs[len(sm.configs) - 1]) 
    currentConfig.Num += 1
    currentConfig.Shards[op.Shard] = op.GID
    sm.configs = append(sm.configs, currentConfig)
}

func (sm *ShardMaster) ExecuteQuery(op Op) Config {
    if op.Num == -1 || op.Num > (len(sm.configs)) {
        return sm.configs[len(sm.configs) - 1]
    }
    return sm.configs[op.Num]
}

func (sm *ShardMaster) Insert(op Op) int {
    for !sm.dead {
        sm.mu.Lock() // propose one op at a time
        seq := sm.px.Max() + 1
        sm.px.Start(seq, op)
        sm.mu.Unlock()
        var agreedOp Op
        var decided bool

        // wait for result
        to := 10 * time.Millisecond
        for !sm.dead && seq >= sm.maxOp {
            var v interface{}
            decided, v = sm.px.Status(seq)
            if decided {
                agreedOp = v.(Op)
                sm.logLock.Lock()
                sm.opLog[seq] = agreedOp
                sm.logLock.Unlock()

                // check if op found in log 
                if opEquality(op, agreedOp) {
                    return seq
                }
                break
            }
            time.Sleep(to)
            if to < 10 * time.Second {
                to *= 2
            }
        }
    }
    return 0
}

func (sm *ShardMaster) PutNOP(seq int) Op {
    op := Op{"nop", -1, nil, -1, -1}
    sm.px.Start(seq, op)
    var agreedOp Op
    var decided bool

    // wait for result
    to := 10 * time.Millisecond
    for !sm.dead {
        var v interface{}
        decided, v = sm.px.Status(seq)
        if decided {
            agreedOp = v.(Op)
            // update log
            sm.logLock.Lock()
            sm.opLog[seq] = agreedOp
            sm.logLock.Unlock()

            return agreedOp
        }

        time.Sleep(to)
        if to < 10 * time.Second {
            to *= 2
        }
    }
    return Op{}
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

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}
  sm.opLog = make(map[int]Op)

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

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
