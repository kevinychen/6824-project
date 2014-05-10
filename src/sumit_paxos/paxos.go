package paxos

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
import "math"
import "math/rand"

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]

  // start added fields
  instances map[int]*Instance // keep track of Paxos instances at different seq
  max int // keep track of 
  majority int
  peersDone []int // needed to decide what memory to free 
}

type ProposalNum struct {
  Hi int
  Low int // possibly better used - currently like a convenient hack 
}

type Instance struct {
  mu sync.Mutex // allow for fine-grained locking on each Paxos instance 

  proposer Proposer  
  acceptor Acceptor
}

type Proposer struct {
  decided bool
  decidedVal interface{}
  highNum ProposalNum
}

type Acceptor struct {
  prepareNum ProposalNum
  acceptNum ProposalNum
  acceptVal interface{}
}

type PrepareArgs struct {
  Seq int
  Prop ProposalNum
}

type AcceptArgs struct {
  Seq int
  Prop ProposalNum
  Value interface{}
}

type PrepareReply struct {
  Prop ProposalNum
  Value interface{}
  Ok bool
}

type AcceptReply struct {
  Ok bool
}

type DecideArgs struct {
  Seq int
  Value interface{}
  Me int
  MaxSeq int
}

type DecideReply struct {
  MaxSeq int
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

//
// Allocates a Paxos instance with default parameters such that they can 
// be overwritten cleanly when the protocol is executed.
//
func MakeInstance() *Instance {
  inst := &Instance{}

  inst.proposer = Proposer{decided: false, highNum: ProposalNum{-1, -1}}
  inst.acceptor = Acceptor{prepareNum: ProposalNum{-1, -1}, acceptNum: ProposalNum{-1, -1}, acceptVal: nil}

  return inst
}

//
// A comparator for the ProposalNum class; these comparisons are frequent
// so the comparator is helpful.
//
func ProposalComp(a ProposalNum, b ProposalNum) bool {
  if a.Hi > b.Hi {
    return true
  } else if a.Hi < b.Hi {
    return false
  } else {
    return a.Low > b.Low
  }
}

//
// Gets the reference to the Paxos instance for the given sequence value.
//
func (px *Paxos) GetInstance(seq int) *Instance {
  px.mu.Lock()
  defer px.mu.Unlock()

  _, ok := px.instances[seq]
  if !ok {
    px.instances[seq] = MakeInstance()
  }
  return px.instances[seq]
}

//
// Updates the local array holding the machine's perspective of other 
// machines' finished Paxos instances. If the value is higher than 
// previously known, it is replaced, and the information used for 
// freeing memory.
//
func (px *Paxos) MergePeersDone(doneVal int, sender int) {
  px.mu.Lock()
  defer px.mu.Unlock()

  if doneVal > px.peersDone[sender] {
    px.peersDone[sender] = doneVal
  }
}

//
// A wrapper for call that ensures that RPCs aren't sent over 
// the network to the machine sending them - instead, the operations
// are just called locally.
//
func (px *Paxos) MyCall(srv string, name string, args interface{}, reply interface{}) bool {
  var err error

  if srv == px.peers[px.me] {
    if name == "Paxos.Prepare" {
      prepareArgs := args.(PrepareArgs)
      prepareReply := reply.(*PrepareReply)
      err = px.Prepare(&prepareArgs, prepareReply)
    } else if name == "Paxos.Accept" {
      acceptArgs := args.(AcceptArgs)
      acceptReply := reply.(*AcceptReply)
      err = px.Accept(&acceptArgs, acceptReply)
    } else if name == "Paxos.Decide" {
      decideArgs := args.(DecideArgs)
      decideReply := reply.(*DecideReply)
      err = px.Decide(&decideArgs, decideReply)
    } else {
        return false
    }
    if err == nil {
        return true
    }

    fmt.Println(err)
    return false
  } else {
    result := call(srv, name, args, reply)
    return result
  }
}

//
// The RPC handler for Prepare requests. The receiver checks if the 
// proposal number is greater than any received, and if so replaces 
// it, and sends the highest accepted proposal number and corresponding
// value back. 
//
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  // no longer in memory
  if args.Seq < px.Min() {
    return nil
  }

  inst := px.GetInstance(args.Seq) // locked (also)
  inst.mu.Lock() // the instance fields should not be concurrently accessed
  if ProposalComp(args.Prop, inst.acceptor.prepareNum) {
    inst.acceptor.prepareNum = args.Prop
    reply.Prop = inst.acceptor.acceptNum
    reply.Value = inst.acceptor.acceptVal
    reply.Ok = true
  } else {
    reply.Ok = false
  }
  inst.mu.Unlock()

  return nil
}

//
// The RPC handler for Accept requests. The receiver checks if the proposal 
// number is greater or equal than any seen, and replaces the accepted proposal  
// fields if so.
//
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  if args.Seq < px.Min() {
    return nil
  }

  inst := px.GetInstance(args.Seq) // locked (also)
  inst.mu.Lock() // the instance fields should not be concurrently accessed 
  if ProposalComp(args.Prop, inst.acceptor.prepareNum) || args.Prop == inst.acceptor.prepareNum {
    inst.acceptor.prepareNum = args.Prop
    inst.acceptor.acceptNum = args.Prop
    inst.acceptor.acceptVal = args.Value
    reply.Ok = true
  } else {
    reply.Ok = false
  }
  inst.mu.Unlock()

  return nil
}

//
// The RPC handler for Decide requests. The receiver marks the Paxos instance
// for the given sequence number as decided and stores the decided value, also
// updating the array for highest completed sequence numbers. 
//
func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
  px.MergePeersDone(args.MaxSeq, args.Me)
  reply.MaxSeq = px.peersDone[px.me]

  if args.Seq < px.Min() {
    return nil
  }

  inst := px.GetInstance(args.Seq) // locked

  // IMPORTANT: DO NOT CHANGE v_a! set decidedVal instead
  inst.mu.Lock() // no concurrent access of instance fields 
  inst.proposer.decided = true
  inst.proposer.decidedVal = args.Value
  inst.mu.Unlock()

  return nil
}

//
// Runs the prepare phase for Paxos, passing control to the accept 
// phase driver if the prepare phase receives a majority of approvals.
//
func (px *Paxos) DrivePrepare(seq int, v interface{}) {
  inst := px.GetInstance(seq)
  
  for !inst.proposer.decided && !px.dead {
    inst.mu.Lock()
    prop := ProposalNum{inst.proposer.highNum.Hi + 1, px.me}
    inst.proposer.highNum = prop 
    inst.mu.Unlock()

    // send Prepare to all peers

    numPrepareOks := 0
    numPrepareRejects := 0
    v_prime := v
    highNum := ProposalNum{-1, -1}

    for i := 0; i < len(px.peers) && !px.dead; i++ {
      prepareArgs := PrepareArgs{Seq: seq, Prop: prop}
      prepareReply := PrepareReply{}

      // Lack of response is essentially a rejection
      if !px.MyCall(px.peers[i], "Paxos.Prepare", prepareArgs, &prepareReply) {
        prepareReply.Ok = false
      }

      if prepareReply.Ok {
        numPrepareOks++

        // keep track of highest prepareOk proposal num and value
        if ProposalComp(prepareReply.Prop, highNum) {
          highNum = prepareReply.Prop
          v_prime = prepareReply.Value
        }

        // track highest proposal num seen
        inst.mu.Lock()
        if ProposalComp(prepareReply.Prop, inst.proposer.highNum) {
          inst.proposer.highNum = prepareReply.Prop
        }
        inst.mu.Unlock()
      } else {
        numPrepareRejects = numPrepareRejects + 1
      }

      // abort early if enough oks or too many rejections
      if numPrepareOks >= px.majority {
        break
      }
      if numPrepareRejects > len(px.peers) - px.majority {
        break
      }
    }

    // received majority of responses, so go ahead to accept phase
    if numPrepareOks >= px.majority {
      px.DriveAccept(seq, prop, v_prime)
    }
  }
}

//
// Runs the accept phase for Paxos, passing control to the decision notification 
// phase driver if the accept phase receives a majority of acceptances.
//
func (px *Paxos) DriveAccept(seq int, prop ProposalNum, value interface{}) {
    // send Accept to all peers

    numAcceptOks := 0
    numAcceptRejects := 0 //haha

    for i := 0; i < len(px.peers) && !px.dead; i++ {
      acceptArgs := AcceptArgs{Seq: seq, Prop: prop, Value: value}
      acceptReply := AcceptReply{}

      if !px.MyCall(px.peers[i], "Paxos.Accept", acceptArgs, &acceptReply) {
        acceptReply.Ok = false
      }

      if acceptReply.Ok {
        numAcceptOks = numAcceptOks + 1
      } else {
        numAcceptRejects = numAcceptRejects + 1
      }

      // abort early if enough oks or too many rejections
      if numAcceptOks >= px.majority {
        break
      }
      if numAcceptRejects > len(px.peers) - px.majority {
        break
      }
    }

    // if majority accepts, send decision messages
    if numAcceptOks >= px.majority {
      px.DriveDecide(seq, value)
    }
}

//
// Runs the decision notification phase after acceptances have been obtained.
//
func (px *Paxos) DriveDecide(seq int, value interface{}) {
    for i := 0; i < len(px.peers) && !px.dead; i++ {
      decideArgs := DecideArgs{Seq: seq, Value: value, Me: px.me, MaxSeq: px.peersDone[px.me]}
      decideReply := DecideReply{}

      if px.MyCall(px.peers[i], "Paxos.Decide", decideArgs, &decideReply) {
        px.MergePeersDone(decideReply.MaxSeq, i)
      }
    }
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
  if seq > px.max {
    px.max = seq
  }
  px.mu.Unlock()

  go px.DrivePrepare(seq, v) // start the Paxos protocol
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  px.mu.Lock()
  if seq > px.peersDone[px.me] {
    px.peersDone[px.me] = seq
  }
  px.mu.Unlock()
  px.Min() // force memory freeing
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
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
  minDone := math.MaxInt32
  for i := 0; i < len(px.peers); i++ {
    if px.peersDone[i] < minDone {
      minDone = px.peersDone[i]
    }
  }
  px.mu.Unlock()

  // actually free memory
  for i := 0; i < (minDone + 1); i++ {
    px.mu.Lock()
    delete(px.instances, i)
    px.mu.Unlock()
  }

  return (minDone + 1)
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  px.mu.Lock()
  inst, ok := px.instances[seq]
  px.mu.Unlock()

  decision := false
  var decisionVal interface{} = nil

  if ok {
    inst.mu.Lock()
    if inst.proposer.decided {
      decision = true
      decisionVal = inst.proposer.decidedVal
    }
    inst.mu.Unlock()
  }

  return decision, decisionVal
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
  px.instances = make(map[int]*Instance)
  px.max = -1
  px.peersDone = make([]int, len(peers))
  px.majority = len(peers) / 2 + 1
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
