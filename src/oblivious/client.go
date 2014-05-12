package oblivious

// 
// Oblivious clerk
// Please don't change this file.
//

import "net/rpc"
import "time"
import "fmt"

type Clerk struct {
  replica string // oblivious replica
}

func MakeClerk(replica string) *Clerk {
  ck := new(Clerk)
  ck.replica = replica
  return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial(Network, srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

func (ck *Clerk) Log(hash, entry string) {
  for {
    // try each known server.
    args := LogArgs{hash, entry}
    var reply LogReply
    ok := call(ck.replica, "ObliviousReplica.Log", &args, &reply)
    if ok {
      break
    }
    time.Sleep(100 * time.Millisecond)
  }
}

func (ck *Clerk) Load(hash string) string {
  for {
    // try each known server.
    args := LoadArgs{hash}
    var reply LoadReply
    ok := call(ck.replica, "ObliviousReplica.Load", &args, &reply)
    if ok {
      return reply.Entry
      break
    }
    time.Sleep(100 * time.Millisecond)
  }
  return ""
}
