package oblivious

import "net"
import "fmt"
import "net/rpc"
import "log"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "encoding/hex"
import "crypto/md5"

type ObliviousReplica struct {
  logLock sync.Mutex
  l net.Listener
  me string
  dead bool // for testing
  unreliable bool // for testing
  UID string
  dataPath string


}

func GetMD5Hash(text string) string {
    hasher := md5.New()
    hasher.Write([]byte(text))
    return hex.EncodeToString(hasher.Sum(nil))
}

// PERSISTENCE

func (or *ObliviousReplica) logEntry(hash string, payload string) {
  filename := or.dataPath + "/" + hash
  f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)
  if err != nil {
    log.Fatal(err)
  }

  enc := gob.NewEncoder(f)
  enc.Encode(payload)

  f.Close()
}

func (or *ObliviousReplica) loadEntry(hash string) string {
  filename := or.dataPath + "/" + hash
  f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)
  if err != nil {
    log.Fatal(err)
  }

  dec := gob.NewDecoder(f)
  var payload string
  dec.Decode(&payload)

  return payload
}

// END PERSISTENCE

func (or *ObliviousReplica) Log(args *LogArgs, reply *LogReply) error {
  or.logEntry(args.Hash, args.Entry)
  return nil
}

func (or *ObliviousReplica) Load(args *LoadArgs, reply *LoadReply) error {
  reply.Entry = or.loadEntry(args.Hash)
  return nil
}

// please don't change this function.
func (or *ObliviousReplica) Kill() {
  or.dead = true
  or.l.Close()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(me string) *ObliviousReplica {

  or := new(ObliviousReplica)
  or.me = me

  rpcs := rpc.NewServer()
  rpcs.Register(or)

  or.UID = GetMD5Hash(me)
  os.Mkdir("data", 0700)

  or.dataPath = "data/replica-" + or.UID
  os.Mkdir(or.dataPath, 0700)

  os.Remove(me)
  l, e := net.Listen(Network, me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  or.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for or.dead == false {
      conn, err := or.l.Accept()
      if err == nil && or.dead == false {
        if or.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if or.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && or.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        or.Kill()
      }
    }
  }()

  return or
}
