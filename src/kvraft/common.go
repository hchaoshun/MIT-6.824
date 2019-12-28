package raftkv

type Err string
const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)


// Put or Append
type PutAppendArgs struct {
	Key   		string
	Value 		string
	Op    		string // "Put" or "Append"
	ClientId	int64
	RequestSeq	int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
