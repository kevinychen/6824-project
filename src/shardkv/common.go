package shardkv
import "hash/fnv"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

var Network = "unix"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
  NoOpID = "No-Op!:1"
  ErrorString = "--OPERATION NOT EXECUTED--"
  GrabSize = 500
)
type Err string

type KVPair struct {
  Shard int
  Key string
  Value string
}

type SnapshotKV struct {
  Config int
  Shard int
  Key string
  Value string
  Cache bool
}

type SnapshotDedup struct {
  Config int
  Key string
  Value string
  Err Err
  Counter int64
}

type PutArgs struct {
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  ID string
  ConfigNum int
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  ID string
  ConfigNum int
}

type GetReply struct {
  Err Err
  Value string
}

type PullArgs struct {
  ConfigNum int

}

type PullReply struct {
  ShardMap map[int]map[string]string
  Dedup map[string]ClientReply
  Err Err
}

type PushArgs struct {
  ConfigNum int
  ShardMap map[int]map[string]string
  Dedup map[string]ClientReply
}

type PushReply struct {
  Err Err
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

