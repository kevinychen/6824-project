package oblivious

// Set of oblivious replicas, serve as remote best-effort oblivious backup

const Network = "unix"

type LogArgs struct {
  Hash string
  Entry string
}

type LogReply struct {
}

type LoadArgs struct {
  Hash string
}

type LoadReply struct {
  Entry string
}
