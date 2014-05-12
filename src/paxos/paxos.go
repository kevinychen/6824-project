package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart. (Now it does)
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

import "encoding/gob"
import "encoding/hex"
import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"
import crand "crypto/rand"
import "crypto/md5"
import "strconv"
import "math/big"
import "io/ioutil"
import "strings"

const LeaderLifetime = 20
const Debug=0
var Network = "unix"
func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

func randStr() string {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := crand.Int(crand.Reader, max)
  x := bigx.Int64()
  return strconv.FormatInt(x, 10)
}

type PrepareArgs struct {
  SequenceNumber int
  ProposalNumber int64
}

type PrepareReply struct {
  SeenNumber int64
  SeenValue interface{}
  SeenPeerId int
  MinSequence int
  Err string
}

type AcceptArgs struct {
  SequenceNumber int
  ProposalNumber int64
  ProposalValue interface{}
  PeerId int
}

type AcceptReply struct {
  SeenNumber int64
  MinSequence int
  Err string
}

type DecideArgs struct {
  SequenceNumber int
  DecidedValue interface{}
  PeerId int
  ProposalNumber int64
}

type DecideReply struct {
  MinSequence int
  Err string
}

type Instance struct {
  Seq int
  Decided bool
  Value interface{}
  LeaderId int
  LeaderName string
  LeaderProposalNum int64
}

type InstanceState struct {
  Seq int
  MaxPn int64
  MaxAn int64
  MaxAv interface{}
  MaxAPeerId int
}

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]
  UID string
  logPath string
  encoder *gob.Encoder
  decisionFile *os.File

  DeleteBarrier int

  // Your data here.
  instances map[int]Instance
  maxProposalNs map[int]int64
  maxAcceptNs map[int]int64
  maxAcceptVs map[int]interface{}
  maxSequenceN int
  minSeqNums []int
  instanceMutex sync.Mutex
  // multi paxos
  maxAcceptPeerIds map[int]int
}



// PERSISTENCE
func GetMD5Hash(text string) string {
    hasher := md5.New()
    hasher.Write([]byte(text))
    return hex.EncodeToString(hasher.Sum(nil))
}

func (px *Paxos) getInstanceFile(seq int) string {
  return fmt.Sprintf("%s/instance.%d.log", px.logPath, seq)
}

func (px *Paxos) getDecisionFile() string {
  return fmt.Sprintf("%s/decision.log", px.logPath)
}

func (px *Paxos) getMaxFile() string {
  return fmt.Sprintf("%s/max.log", px.logPath)
}

func (px *Paxos) initializeEncoder() {
  filename := px.getDecisionFile()
  var err error
  px.decisionFile, err = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
  if err != nil {
    log.Fatal(err)
  }
  px.encoder = gob.NewEncoder(px.decisionFile)
}

func (px *Paxos) logDecision(seq int, instance Instance) {
  px.encoder.Encode(instance)
  px.decisionFile.Sync()
  // Delete any associated instance files, the decision has been committed
  // Needs more clever implementation to ensure decisions aren't overridden
  //  _ = os.Remove(px.getInstanceFile(seq))
}

func (px *Paxos) logInstance(seq int, state InstanceState) {
  filename := px.getInstanceFile(seq)
  f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)
  if err != nil {
    log.Fatal(err)
  }
  enc := gob.NewEncoder(f)
  enc.Encode(state)
  f.Close()
}

func (px *Paxos) logMax() {
  filename := px.getMaxFile()
  f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)
  if err != nil {
    log.Fatal(err)
  }
  enc := gob.NewEncoder(f)
  enc.Encode(px.maxSequenceN)
  f.Close()
}

// Call under LOCK, puts directly into maps
func (px *Paxos) loadInstances() {
  files, _ := ioutil.ReadDir(px.logPath)
  for _, fn := range files {
    if strings.Contains(fn.Name(), "decision") {
      continue
    }
    f, err := os.OpenFile(px.logPath + "/" + fn.Name(), os.O_RDONLY, 0666)
    if err != nil {
      log.Fatal(err)
    }
    dec := gob.NewDecoder(f)
    state := InstanceState{}
    dec.Decode(&state)

    seq := state.Seq
    px.maxProposalNs[seq] = state.MaxPn
    px.maxAcceptNs[seq] = state.MaxAn
    px.maxAcceptVs[seq] = state.MaxAv
    px.maxAcceptPeerIds[seq] = state.MaxAPeerId
  }
}

// Load Persisted Decisions
// Call under Lock
func (px *Paxos) loadDecision() {
  filename := px.getDecisionFile()
  f, err := os.OpenFile(filename, os.O_RDONLY, 0666)
  if err != nil {
    log.Fatal(err)
  }
  dec := gob.NewDecoder(f)
  for {
    instance := Instance{}
    err = dec.Decode(&instance)
    if err != nil {
      break
    }
    if instance.Decided {
      px.instances[instance.Seq] = instance
    }
  }
}

func (px *Paxos) loadMax() {
  filename := px.getMaxFile()
  f, err := os.OpenFile(filename, os.O_RDONLY, 0666)
  if err == nil {
    dec := gob.NewDecoder(f)
    dec.Decode(&px.maxSequenceN)
  }
}

// Call under Lock
func (px *Paxos) Recover() {
  px.loadInstances()
  px.loadDecision()
  px.loadMax()
}

// END PERSISTENCE

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
  c, err := rpc.Dial(Network, srv)
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

func (px *Paxos) getProposalNumber() int64 {
  t := time.Now().UnixNano()
  proposalNum :=  (t / 100) * int64(len(px.peers)) + int64(px.me)
  return proposalNum
}

func (px *Paxos) freeInstances(minNum int) {
  for k, v := range px.instances {
    if v.Seq < minNum {
      delete(px.instances, k)
      delete(px.maxProposalNs, k)
      delete(px.maxAcceptNs, k)
      delete(px.maxAcceptVs, k)
      delete(px.maxAcceptPeerIds, k)
    }
  }
}

// Prepare
func (px *Paxos) doPrepare(args *PrepareArgs) *PrepareReply {
  proposalNumber := args.ProposalNumber
  seq := args.SequenceNumber
  reply := new(PrepareReply)
  // Acquire acceptor state lock
  px.mu.Lock()
  defer px.mu.Unlock()
  if proposalNumber > px.maxProposalNs[seq] {
    px.maxProposalNs[seq] = proposalNumber
    reply.SeenNumber = px.maxAcceptNs[seq]
    reply.SeenValue = px.maxAcceptVs[seq]
    reply.SeenPeerId = px.maxAcceptPeerIds[seq]
  } else {
    reply.Err = "PREPARE_REJECT"
  }
  reply.MinSequence = px.minSeqNums[px.me]
  // Persist state
  px.logInstance(seq, InstanceState{seq, px.maxProposalNs[seq],
    px.maxAcceptNs[seq], px.maxAcceptVs[seq], px.maxAcceptPeerIds[seq]})
  return reply
}

// RPC Wrapper for doPrepare
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  *reply = *px.doPrepare(args)
  return nil
}


// Accept
func (px *Paxos) doAccept(args *AcceptArgs) *AcceptReply {
  proposalNumber := args.ProposalNumber
  proposalValue := args.ProposalValue
  seq := args.SequenceNumber
  peerId := args.PeerId
  reply := new(AcceptReply)

  px.mu.Lock()
  defer px.mu.Unlock()
  if proposalNumber >= px.maxProposalNs[seq] {
    px.maxProposalNs[seq] = proposalNumber
    px.maxAcceptNs[seq] = proposalNumber
    px.maxAcceptVs[seq] = proposalValue
    px.maxAcceptPeerIds[seq] = peerId
    reply.SeenNumber = px.maxProposalNs[seq]
  } else {
    reply.Err = "ACCEPT_REJECT"
  }
  reply.MinSequence = px.minSeqNums[px.me]
  // Persist state
  px.logInstance(seq, InstanceState{seq, px.maxProposalNs[seq],
    px.maxAcceptNs[seq], px.maxAcceptVs[seq], px.maxAcceptPeerIds[seq]})
  return reply
}

// RPC Wrapper for doAccept
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  *reply = *px.doAccept(args)
  return nil
}

// Decide
func (px *Paxos) doDecide(args *DecideArgs) *DecideReply {
  px.mu.Lock()
  defer px.mu.Unlock()

  sequenceNumber := args.SequenceNumber
  value := args.DecidedValue
  leaderId := args.PeerId
  proposalNum := args.ProposalNumber
  reply := new(DecideReply)
  reply.MinSequence = px.minSeqNums[px.me]

  instance := Instance{Seq: sequenceNumber, Decided: true,
    Value: value, LeaderId: leaderId, LeaderName: px.peers[leaderId],
    LeaderProposalNum: proposalNum}
  // Persist decision
  px.logDecision(sequenceNumber, instance)

  px.instances[sequenceNumber] = instance
  return reply
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
  *reply = *px.doDecide(args)
  return nil
}

// Update sequence num array
func (px *Paxos) atomicSequenceNumUpdate(seq int, peer int) {
  px.mu.Lock()
  defer px.mu.Unlock()

  if px.minSeqNums[peer] < seq {
    px.minSeqNums[peer] = seq
  }
}

// Proposer
func (px *Paxos) doPropose(seq int, v interface{}) {
  remainder := seq % LeaderLifetime // seq - remainder is index of last election round
  leaderId := -1 // id of leader
  retry := false // true if skipping the prepare phase failed the first time

  // Leader is the winner of the last election round if it finished
  if px.instances[seq - remainder].Decided {
    leaderId = px.instances[seq - remainder].LeaderId
  }

  // While not decided, keep proposing!
  for !px.instances[seq].Decided && px.dead == false {
    proposalNum := px.getProposalNumber()
    // Use old proposal number for leader on the first attempt
    if px.me == leaderId && !retry {
      proposalNum = px.instances[seq - remainder].LeaderProposalNum
    }
    maxSeenN := int64(0)
    maxSeenV := v
    maxSeenPeerId := px.me
    prepareCount := 0
    numPeers := len(px.peers)

    px.mu.Lock()
    _, ok := px.instances[seq]
    if ok {
      px.mu.Unlock()
      break
    }
    px.mu.Unlock()

    /* Do prepare phase if we are
     * 1. not the leader
     * 2. in an election round
     * 3. past the first attempt
     */
    if !(px.me == leaderId) || remainder == 0 || retry {
      for i, _ := range px.peers {
        var ok bool
        reply := new(PrepareReply)
        args := new(PrepareArgs)
        *args = PrepareArgs{SequenceNumber: seq, ProposalNumber: proposalNum}
        // Send prepares
        if i == px.me {
          ok = true // local call had better return
          reply = px.doPrepare(args)
        } else {
          ok = call(px.peers[i], "Paxos.Prepare", args, reply)
        }

        if ok && reply.Err == "" {
          px.atomicSequenceNumUpdate(reply.MinSequence, i)
          if reply.SeenNumber > maxSeenN {
            maxSeenN = reply.SeenNumber
            maxSeenV = reply.SeenValue
            maxSeenPeerId = reply.SeenPeerId
          }
          prepareCount++
        }
      }
    }


    /* Do accept phase if we are
     * 1. leader and first attempt
     * 2. majority of prepares
     */
    if (px.me == leaderId && !retry) || prepareCount * 2 > numPeers {
      acceptCount := 0
      for i, _ := range px.peers {
        var ok bool
        reply := new(AcceptReply)
        args := new(AcceptArgs)
        *args = AcceptArgs{SequenceNumber: seq, ProposalNumber: proposalNum,
          ProposalValue: maxSeenV, PeerId: maxSeenPeerId}
        // Send accepts
        if i == px.me {
          ok = true
          reply = px.doAccept(args)
        } else {
          ok = call(px.peers[i], "Paxos.Accept", args, reply)
        }

        if ok && reply.Err == "" {
          px.atomicSequenceNumUpdate(reply.MinSequence, i)
          acceptCount++
        }
      }

      if acceptCount * 2 > numPeers {
        // Send decides
        decideReply := new(DecideReply)
        decideArgs := &DecideArgs{SequenceNumber: seq, DecidedValue: maxSeenV,
          PeerId: maxSeenPeerId, ProposalNumber: proposalNum}
        for i, _ := range px.peers {
          var ok bool
          if i == px.me {
            decideReply = px.doDecide(decideArgs)
          } else {
            ok = call(px.peers[i], "Paxos.Decide", decideArgs, decideReply)
          }

          if ok && decideReply.Err == "" {
            px.atomicSequenceNumUpdate(decideReply.MinSequence, i)
          }
        }

        break;
      }
    }

    time.Sleep(1000000 * 100)
    retry = true
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
  if seq > px.maxSequenceN {
    px.maxSequenceN = seq
    px.logMax()
  }
  px.mu.Unlock()
  go px.doPropose(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  px.atomicSequenceNumUpdate(seq, px.me)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  return px.maxSequenceN
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
func (px *Paxos) minHelper() int {
  min := px.DeleteBarrier
  for i, _ := range px.peers {
    if px.minSeqNums[i] < min {
      min = px.minSeqNums[i]
    }
  }
  return min + 1
}


func (px *Paxos) Min() int {
  px.mu.Lock()
  minNum := px.minHelper()
  px.freeInstances(minNum)
  px.mu.Unlock()

  return minNum
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
  defer px.mu.Unlock()
  inst, ok := px.instances[seq]
  if ok {
    if inst.Decided {
      return true, inst.Value
    }
  }
  return false, nil
}

// Get the leader for a paxos round
func (px *Paxos) GetLeader(seq int) string {
  px.mu.Lock()
  defer px.mu.Unlock()
  remainder := seq % LeaderLifetime
  inst, ok := px.instances[seq - remainder]
  if ok {
    if inst.Decided {
      return inst.LeaderName
    }
  }
  return ""
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

// Simulate loss of memory
func (px *Paxos) HardReset() {
  px.mu.Lock()
  px.instanceMutex.Lock()
  defer px.mu.Unlock()
  defer px.instanceMutex.Unlock()

  px.instances = make(map[int]Instance)
  px.maxProposalNs = make(map[int]int64)
  px.maxAcceptNs = make(map[int]int64)
  px.maxAcceptVs = make(map[int]interface{})
  px.maxAcceptPeerIds = make(map[int]int)

  px.maxSequenceN = 0
  for z := 0; z < len(px.peers); z++ {
    px.minSeqNums[z] = -1
  }
  px.Recover()
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
  fmt.Printf("peers: %v\n", px.peers)
  px.UID = GetMD5Hash(peers[me])
  os.Mkdir("logs", 0700)
  px.logPath = "logs/peer-" + px.UID
  os.Mkdir(px.logPath, 0700)
  px.DeleteBarrier = (1 << 31) - 1

  px.initializeEncoder()

  // Your initialization code here.
  px.instances = make(map[int]Instance)
  px.maxProposalNs = make(map[int]int64)
  px.maxAcceptNs = make(map[int]int64)
  px.maxAcceptVs = make(map[int]interface{})
  px.minSeqNums = make([]int, len(peers), len(peers))
  px.maxAcceptPeerIds = make(map[int]int)
  for z := 0; z < len(peers); z++ {
    px.minSeqNums[z] = -1
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
    l, e := net.Listen(Network, peers[me]);
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
      // Close decision log file
      px.decisionFile.Close()
    }()
  }


  return px
}
