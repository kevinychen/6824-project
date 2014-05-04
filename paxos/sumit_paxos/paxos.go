//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]


  // Your data here
  acceptors map[int]Acceptor
  statuses map[int]Status
  max int
  min int
  peersDone []int
}

type Acceptor struct {
  PrepareNum int
  AcceptNum int
  AcceptVal interface{}
}

type Status struct {
  value interface{}
  done bool
}

type PrepareArgs struct {
  Seq int
  Prop int
  //UID string	
}

type AcceptArgs struct {
  Seq int
  Prop int
  Value interface{}
  //UID string
}

type PrepareReply struct {
  HighPrepare int
  HighProposal int
  Value interface{}
  OK bool
  //FromUID string 
}

type AcceptReply struct {
  OK bool
}

type DecideArgs struct {
  Seq int
  Value interface{}
  Me int
  MaxSeq int
}

type DecideReply struct {
  OK bool
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()

  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  state, ok := px.acceptors[args.Seq]
  if args.Seq > px.max {
      px.max = args.Seq
  }
  if args.Seq >= px.min {
      if ok {
          reply.HighPrepare = state.PrepareNum
          reply.HighProposal = state.AcceptNum
          reply.Value = state.AcceptVal
          if args.Prop <= state.PrepareNum {
              reply.OK = false
          } else {
              px.acceptors[args.Seq] = Acceptor{args.Prop, state.AcceptNum, state.AcceptVal}
              reply.OK = true
          }
      } else {
          px.acceptors[args.Seq] = Acceptor{args.Prop, 0, nil}
          reply.HighPrepare = 0
          reply.HighProposal = 0
          reply.Value = nil
          reply.OK = true
      }
  }
  return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
    px.mu.Lock()
    defer px.mu.Unlock()

    if args.Seq > px.max {
        px.max = args.Seq
    }
    if args.Seq >= px.min {
        state, ok := px.acceptors[args.Seq]
        if args.Prop < state.PrepareNum && ok {
            reply.OK = false
        } else {
            px.acceptors[args.Seq] = Acceptor{args.Prop, args.Prop, args.Value}
            reply.OK = true
        }
    }
    return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  if args.Seq >= px.min {
      px.statuses[args.Seq] = Status{args.Value, true}
  }

  if args.Seq > px.max {
      px.max = args.Seq
  }
  px.peersDone[args.Me] = args.MaxSeq

  reply.OK = true

  min := 0
  if len(px.peersDone) > 0 {
      min = px.peersDone[0]
      for i := 1; i < len(px.peersDone); i++ {
          if px.peersDone[i] < min {
              min = px.peersDone[i]
          }
      }
  }
  px.min = min + 1
  for key, _ := range px.statuses {
    if key < px.min {
        delete(px.statuses, key)
    }
  }
  for key, _ := range px.acceptors {
    if key < px.min {
        delete(px.acceptors, key)
    }
  }
  return nil
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  px.mu.Lock()
  defer px.mu.Unlock()

  if seq > px.max {
      px.max = seq
  }
  _, started := px.statuses[seq]
  if seq >= px.min && !started {
      px.statuses[seq] = Status{nil, false}
      go func() {
          // start proposal process
          maxProp := 0
          for {
              px.mu.Lock()
              decided := px.statuses[seq].done
              px.mu.Unlock()

              if px.dead || decided || seq < px.min {
                  break
              }

              // start prepare
              n := maxProp + len(px.peers) - (maxProp % len(px.peers)) + px.me
              var AcceptVal interface{}
              prepareConfirms := 0
              AcceptNum := 0

              for _, peer := range px.peers {
                  prepareargs := &PrepareArgs{seq, n}
                  preparereply := &PrepareReply{}
                  ok := true
                  if px.peers[px.me] != peer {
                      ok = call(peer, "Paxos.Prepare", prepareargs, preparereply)
                  } else {
                      px.Prepare(prepareargs, preparereply)
                  }
                  if ok {
                      if preparereply.HighPrepare > maxProp {
                          maxProp = preparereply.HighPrepare
                      }
                      if preparereply.OK {
                          prepareConfirms += 1
                          if preparereply.HighProposal > AcceptNum {
                              AcceptNum = preparereply.HighProposal
                              AcceptVal = preparereply.Value
                          }
                      }
                  }
              }

              // start accept
              if prepareConfirms > len(px.peersDone) / 2 {
                  v_prime := v
                  acceptConfirms := 0
                  if AcceptVal != nil {
                      v_prime = AcceptVal
                  }
                  for _, peer := range px.peers {
                      acceptargs := &AcceptArgs{seq, n, v_prime}
                      acceptreply := &AcceptReply{}
                      ok := true
                      if px.peers[px.me] != peer {
                          ok = call(peer, "Paxos.Accept", acceptargs, acceptreply)
                      } else {
                          px.Accept(acceptargs, acceptreply)
                      }
                      if ok && acceptreply.OK {
                          acceptConfirms += 1
                      }
                  }
                  if acceptConfirms > len(px.peersDone) / 2 {
                      px.mu.Lock()
                      MaxSeq := px.peersDone[px.me]
                      px.mu.Unlock()

                      for _, peer := range px.peers {
                          decideargs := &DecideArgs{seq, v_prime, px.me, MaxSeq}
                          decidereply := &DecideReply{}
                          if px.peers[px.me] != peer {
                              call(peer, "Paxos.Decide", decideargs, decidereply)
                          } else {
                              px.Decide(decideargs, decidereply)
                          }
                      }
                  }
              }
         }
     }()
  }
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  px.peersDone[px.me] = seq

  min := 0

  if len(px.peersDone) > 0 {
      min = px.peersDone[0]
      for i := 1; i < len(px.peersDone); i++ {
          if px.peersDone[i] < min {
              min = px.peersDone[i]
          }
     }
  }
  px.min = min + 1

  for key, _ := range px.acceptors {
      if key < px.min {
          delete(px.acceptors, key)
      }
  }

  for key, _ := range px.statuses {
      if key < px.min {
          delete(px.statuses, key)
      }
  }
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  return px.max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  px.mu.Lock()
  defer px.mu.Unlock()
  return px.min
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()

  _, ok := px.statuses[seq]
  if ok {
      return px.statuses[seq].done, px.statuses[seq].value
  }
  return false, nil
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me


  // Your initialization code here.
  px.acceptors = make(map[int]Acceptor)
  px.statuses = make(map[int]Status)
  px.max = -1
  px.min = 0
  px.peersDone = make([]int, len(peers))
  for i := 0; i < len(peers); i++ {
      px.peersDone[i] = -1
  }

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
