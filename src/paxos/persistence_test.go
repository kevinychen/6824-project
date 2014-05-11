package paxos

import "testing"
import "runtime"
//import "strconv"
import "os"
import "time"
import "fmt"
import "math/rand"

func TestBasicPersistence(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("basic", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  fmt.Printf("Test: Persistent: Single proposer ...\n")

  pxa[0].Start(0, "hello")
  waitn(t, pxa, 0, npaxos)

  // RESET
  pxa[0].HardReset()

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Persistent: Many proposers, same value ...\n")

  for i := 0; i < npaxos; i++ {
    pxa[i].Start(1, 77)
  }
  waitn(t, pxa, 1, npaxos)

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Persistent: Many proposers, different values ...\n")

  pxa[0].Start(2, 100)
  pxa[1].Start(2, 101)
  pxa[2].Start(2, 102)
  waitn(t, pxa, 2, npaxos)

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Persistent: Out-of-order instances ...\n")

  pxa[0].Start(7, 700)
  pxa[0].Start(6, 600)
  pxa[1].Start(5, 500)
  waitn(t, pxa, 7, npaxos)
  pxa[0].Start(4, 400)
  pxa[1].Start(3, 300)

  // RESET
  pxa[0].HardReset()
  pxa[1].HardReset()

  waitn(t, pxa, 6, npaxos)
  waitn(t, pxa, 5, npaxos)
  // RESET
  pxa[0].HardReset()
  pxa[1].HardReset()
  waitn(t, pxa, 4, npaxos)
  // RESET
  pxa[1].HardReset()
  waitn(t, pxa, 3, npaxos)

  if pxa[0].Max() != 7 {
    t.Fatalf("wrong Max()")
  }

  fmt.Printf("  ... Passed\n")
}

func TestDeafPersistent(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const npaxos = 5
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("deaf", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  fmt.Printf("Test: Persistent: Deaf proposer ...\n")

  pxa[0].Start(0, "hello")
  waitn(t, pxa, 0, npaxos)

  os.Remove(pxh[0])
  os.Remove(pxh[npaxos-1])

  pxa[1].Start(1, "goodbye")
  waitmajority(t, pxa, 1)
  time.Sleep(1 * time.Second)
  if ndecided(t, pxa, 1) != npaxos - 2 {
    t.Fatalf("a deaf peer heard about a decision")
  }

  pxa[0].Start(1, "xxx")

  // RESET
  pxa[1].HardReset()
  pxa[0].HardReset()

  waitn(t, pxa, 1, npaxos-1)

  // RESET
  pxa[1].HardReset()
  pxa[0].HardReset()

  time.Sleep(1 * time.Second)
  if ndecided(t, pxa, 1) != npaxos - 1 {
    t.Fatalf("a deaf peer heard about a decision")
  }

  pxa[npaxos-1].Start(1, "yyy")

  // RESET ALL BUT 1
  for i := 0; i < npaxos - 1; i++ {
    pxa[i].HardReset()
  }

  waitn(t, pxa, 1, npaxos)

  fmt.Printf("  ... Passed\n")
}

func TestForgetPersistent(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const npaxos = 6
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)
  
  for i := 0; i < npaxos; i++ {
    pxh[i] = port("gc", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  fmt.Printf("Test: Persistent: Forgetting ...\n")

  // initial Min() correct?
  for i := 0; i < npaxos; i++ {
    m := pxa[i].Min()
    if m > 0 {
      t.Fatalf("wrong initial Min() %v", m)
    }
  }

  pxa[0].Start(0, "00")
  pxa[1].Start(1, "11")
  pxa[2].Start(2, "22")
  pxa[0].Start(6, "66")
  pxa[1].Start(7, "77")

  waitn(t, pxa, 0, npaxos)

  // Min() correct?
  for i := 0; i < npaxos; i++ {
    m := pxa[i].Min()
    if m != 0 {
      t.Fatalf("wrong Min() %v; expected 0", m)
    }
  }

  waitn(t, pxa, 1, npaxos)

  // Min() correct?
  for i := 0; i < npaxos; i++ {
    m := pxa[i].Min()
    if m != 0 {
      t.Fatalf("wrong Min() %v; expected 0", m)
    }
  }

  // everyone Done() -> Min() changes?
  for i := 0; i < npaxos; i++ {
    pxa[i].Done(0)
  }
  for i := 1; i < npaxos; i++ {
    pxa[i].Done(1)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i].Start(8 + i, "xx")
  }
  allok := false
  for iters := 0; iters < 12; iters++ {
    allok = true
    for i := 0; i < npaxos; i++ {
      s := pxa[i].Min()
      if s != 1 {
        allok = false
      }
    }
    if allok {
      break
    }
    time.Sleep(1 * time.Second)
  }
  if allok != true {
    t.Fatalf("Min() did not advance after Done()")
  }

  fmt.Printf("  ... Passed\n")
}

func TestRPCCountPersistent(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Persistent: RPC counts aren't too high ...\n")

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("count", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  ninst1 := 5
  seq := 0
  for i := 0; i < ninst1; i++ {
    pxa[0].Start(seq, "x")
    waitn(t, pxa, seq, npaxos)

    // RESET
    pxa[(i + 0) % npaxos].HardReset()
    pxa[(i + 1) % npaxos].HardReset()
    pxa[(i + 2) % npaxos].HardReset()
    seq++
  }

  time.Sleep(2 * time.Second)

  total1 := 0
  for j := 0; j < npaxos; j++ {
    total1 += pxa[j].rpcCount
  }

  // per agreement:
  // 3 prepares
  // 3 accepts
  // 3 decides
  expected1 := ninst1 * npaxos * npaxos
  if total1 > expected1 {
    t.Fatalf("too many RPCs for serial Start()s; %v instances, got %v, expected %v",
      ninst1, total1, expected1)
  }

  ninst2 := 5
  for i := 0; i < ninst2; i++ {
    for j := 0; j < npaxos; j++ {
      go pxa[j].Start(seq, j + (i * 10))
    }
    waitn(t, pxa, seq, npaxos)
    seq++
  }

  // RESET
  pxa[0].HardReset()
  pxa[2].HardReset()

  time.Sleep(2 * time.Second)

  total2 := 0
  for j := 0; j < npaxos; j++ {
    total2 += pxa[j].rpcCount
  }
  total2 -= total1

  // worst case per agreement:
  // Proposer 1: 3 prep, 3 acc, 3 decides.
  // Proposer 2: 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
  // Proposer 3: 3 prep, 3 acc, 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
  expected2 := ninst2 * npaxos * 15
  if total2 > expected2 {
    t.Fatalf("too many RPCs for concurrent Start()s; %v instances, got %v, expected %v",
      ninst2, total2, expected2)
  }

  fmt.Printf("  ... Passed\n")
}

//
// many agreements (without failures)
//
func TestManyPersistent(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Persistent: Many instances ...\n")

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("many", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
    pxa[i].Start(0, 0)
  }

  const ninst = 50
  for seq := 1; seq < ninst; seq++ {
    // only 5 active instances, to limit the
    // number of file descriptors.
    for seq >= 5 && ndecided(t, pxa, seq - 5) < npaxos {
      time.Sleep(20 * time.Millisecond)
    }
    for i := 0; i < npaxos; i++ {
      pxa[i].Start(seq, (seq * 10) + i)
    }
    // RESET
    pxa[(seq + 0) % npaxos].HardReset()
    pxa[(seq + 1) % npaxos].HardReset()
  }

  // RESET
  pxa[0].HardReset()
  pxa[1].HardReset()
  pxa[2].HardReset()

  for {
    done := true
    for seq := 1; seq < ninst; seq++ {
      if ndecided(t, pxa, seq) < npaxos {
        done = false
      }
    }
    if done {
      break
    }
    time.Sleep(100 * time.Millisecond)
  }

  fmt.Printf("  ... Passed\n")
}

//
// a peer starts up, with proposal, after others decide.
// then another peer starts, without a proposal.
// 
func TestOldPersistent(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Persistent: Minority proposal ignored ...\n")

  const npaxos = 5
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("old", i)
  }

  pxa[1] = Make(pxh, 1, nil)
  pxa[2] = Make(pxh, 2, nil)
  pxa[3] = Make(pxh, 3, nil)
  pxa[1].Start(1, 111)

  waitmajority(t, pxa, 1)

  pxa[0] = Make(pxh, 0, nil)
  // RESET
  pxa[2].HardReset()
  pxa[3].HardReset()

  pxa[0].Start(1, 222)

  waitn(t, pxa, 1, 4)

  // RESET
  pxa[0].HardReset()
  pxa[1].HardReset()
  pxa[2].HardReset()
  pxa[3].Kill()

  if true {
    pxa[4] = Make(pxh, 4, nil)
    pxa[0].Start(1, 333)
    waitn(t, pxa, 1, npaxos)
    ndecided(t, pxa, 1)
  }

  fmt.Printf("  ... Passed\n")
}

//
// many agreements, with unreliable RPC
//
func TestManyUnreliablePersistent(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Persistent: Many instances, unreliable RPC ...\n")

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("manyun", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
    pxa[i].unreliable = true
    pxa[i].Start(0, 0)
  }

  const ninst = 50
  for seq := 1; seq < ninst; seq++ {
    // only 3 active instances, to limit the
    // number of file descriptors.
    for seq >= 3 && ndecided(t, pxa, seq - 3) < npaxos {
      time.Sleep(20 * time.Millisecond)
    }
    for i := 0; i < npaxos; i++ {
      pxa[i].Start(seq, (seq * 10) + i)
    }

    pxa[((seq * 7 + 9) % 13) % npaxos].HardReset()
    pxa[((seq * 11 + 17) % 7) % npaxos].HardReset()
  }

  for {
    done := true
    for seq := 1; seq < ninst; seq++ {
      if ndecided(t, pxa, seq) < npaxos {
        done = false
      }
    }
    if done {
      break
    }
    time.Sleep(100 * time.Millisecond)
  }
  
  fmt.Printf("  ... Passed\n")
}

func TestPartitionPersistent(t *testing.T) {
  runtime.GOMAXPROCS(4)

  tag := "partition"
  const npaxos = 5
  var pxa []*Paxos = make([]*Paxos, npaxos)
  defer cleanup(pxa)
  defer cleanpp(tag, npaxos)

  for i := 0; i < npaxos; i++ {
    var pxh []string = make([]string, npaxos)
    for j := 0; j < npaxos; j++ {
      if j == i {
        pxh[j] = port(tag, i)
      } else {
        pxh[j] = pp(tag, i, j)
      }
    }
    pxa[i] = Make(pxh, i, nil)
  }
  defer part(t, tag, npaxos, []int{}, []int{}, []int{})

  seq := 0

  fmt.Printf("Test: Persistent: No decision if partitioned ...\n")

  part(t, tag, npaxos, []int{0,2}, []int{1,3}, []int{4})
  pxa[1].Start(seq, 111)
  // RESET
  pxa[0].HardReset()
  pxa[2].HardReset()

  checkmax(t, pxa, seq, 0)
  
  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Persistent: Decision in majority partition ...\n")

  part(t, tag, npaxos, []int{0}, []int{1,2,3}, []int{4})
  // RESET
  pxa[0].HardReset()

  time.Sleep(2 * time.Second)

  waitmajority(t, pxa, seq)


  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Persistent: All agree after full heal ...\n")

  part(t, tag, npaxos, []int{1}, []int{0,2,3,4}, []int{})

  pxa[2].HardReset()
  pxa[0].HardReset()

  pxa[0].Start(seq, 1000) // poke them

  pxa[4].Start(seq, 1004)


  part(t, tag, npaxos, []int{0,1,2,3,4}, []int{}, []int{})


  waitn(t, pxa, seq, npaxos)

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Persistent: One peer switches partitions ...\n")


  for iters := 0; iters < 20; iters++ {
    seq++

    part(t, tag, npaxos, []int{0,1,2}, []int{3,4}, []int{})
    pxa[0].Start(seq, seq * 10)
    pxa[3].Start(seq, (seq * 10) + 1)


    waitmajority(t, pxa, seq)
    if ndecided(t, pxa, seq) > 3 {
      t.Fatalf("too many decided")
    }
  // RESET
  pxa[0].HardReset()
  pxa[1].HardReset()
    
    part(t, tag, npaxos, []int{0,1}, []int{2,3,4}, []int{})
  // RESET
  pxa[0].HardReset()
  pxa[1].HardReset()
    waitn(t, pxa, seq, npaxos)
  // RESET
  pxa[3].HardReset()
  pxa[4].HardReset()
  }

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Persistent: One peer switches partitions, unreliable ...\n")


  for iters := 0; iters < 20; iters++ {
    seq++

    for i := 0; i < npaxos; i++ {
      pxa[i].unreliable = true
    }

    part(t, tag, npaxos, []int{0,1,2}, []int{3,4}, []int{})
    for i := 0; i < npaxos; i++ {
      pxa[i].Start(seq, (seq * 10) + i)
    }
  // RESET
  pxa[0].HardReset()
  pxa[1].HardReset()
  pxa[2].HardReset()
  pxa[3].HardReset()
  pxa[4].HardReset()
    waitn(t, pxa, seq, 3)
    if ndecided(t, pxa, seq) > 3 {
      t.Fatalf("too many decided")
    }
    
    part(t, tag, npaxos, []int{0,1}, []int{2,3,4}, []int{})

    for i := 0; i < npaxos; i++ {
      pxa[i].unreliable = false
    }

    waitn(t, pxa, seq, 5)
  }

  fmt.Printf("  ... Passed\n")
}

func TestLotsPersistent(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Persistent: Many requests, changing partitions ...\n")

  tag := "lots"
  const npaxos = 5
  var pxa []*Paxos = make([]*Paxos, npaxos)
  defer cleanup(pxa)
  defer cleanpp(tag, npaxos)

  for i := 0; i < npaxos; i++ {
    var pxh []string = make([]string, npaxos)
    for j := 0; j < npaxos; j++ {
      if j == i {
        pxh[j] = port(tag, i)
      } else {
        pxh[j] = pp(tag, i, j)
      }
    }
    pxa[i] = Make(pxh, i, nil)
    pxa[i].unreliable = true
  }
  defer part(t, tag, npaxos, []int{}, []int{}, []int{})

  done := false

  // re-partition periodically
  ch1 := make(chan bool)
  go func() {
    defer func(){ ch1 <- true }()
    for done == false {
      var a [npaxos]int
      for i := 0; i < npaxos; i++ {
        a[i] = (rand.Int() % 3)
      }
      pa := make([][]int, 3)
      for i := 0; i < 3; i++ {
        pa[i] = make([]int, 0)
        for j := 0; j < npaxos; j++ {
          if a[j] == i {
            pa[i] = append(pa[i], j)
          }
        }
      }
      part(t, tag, npaxos, pa[0], pa[1], pa[2])
      time.Sleep(time.Duration(rand.Int63() % 200) * time.Millisecond)
    }
  }()

  seq := 0

  // periodically start a new instance
  ch2 := make(chan bool)
  go func () {
    defer func() { ch2 <- true } ()
    for done == false {
      // how many instances are in progress?
      nd := 0
      for i := 0; i < seq; i++ {
        if ndecided(t, pxa, i) == npaxos {
          nd++
        }
      }
      if seq - nd < 10 {
        for i := 0; i < npaxos; i++ {
          pxa[i].Start(seq, rand.Int() % 10)
        }
        seq++
      }
      time.Sleep(time.Duration(rand.Int63() % 300) * time.Millisecond)
    }
  }()

  // periodically check that decisions are consistent
  ch3 := make(chan bool)
  go func() {
    defer func() { ch3 <- true }()
    for done == false {
      for i := 0; i < seq; i++ {
        ndecided(t, pxa, i)
      }
      time.Sleep(time.Duration(rand.Int63() % 300) * time.Millisecond)
    }
  }()

  time.Sleep(20 * time.Second)
  done = true
  <- ch1
  <- ch2
  <- ch3

  // repair, then check that all instances decided.
  for i := 0; i < npaxos; i++ {
    pxa[i].unreliable = false
    // RESET
    pxa[i].HardReset()
  }
  part(t, tag, npaxos, []int{0,1,2,3,4}, []int{}, []int{})
  time.Sleep(5 * time.Second)

  for i := 0; i < seq; i++ {
    waitmajority(t, pxa, i)
  }

  fmt.Printf("  ... Passed\n")
}
