package shardctrler


import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"
import "time"
import "log"
import "sync/atomic"


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead	int32
	configs []Config // indexed by config num
	waitChMap map[int] chan Op //传递有下层raft服务的appch传来的command index-> chan(op)
	seqMap map[int64]int
}


type Op struct {
	// Your data here.
	ClientId		int64
	SeqId			int
	Index			int
	OpType			string
	// join
	Servers			map[int][]string // gid->servers mappping
	// leave
	GIDs			[]int
	// move
	Shard			int
	GID				int
	// query
	Num 			int
	Cfg   			Config
	Err				Err
}

//
func (sc *ShardCtrler) getWaitCh(index int) chan Op{
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.waitChMap[index]
	if !exist{
		sc.waitChMap[index] = make(chan Op, 1)
		ch = sc.waitChMap[index]
	}
	return ch
}

// 加入一个新的组，创建一个最新的配置，加入新的组以后需要重新负载均衡
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.WrongLeader = true

	if sc.Killed(){
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	_, ifLeader := sc.rf.GetState()
	if !ifLeader{
		reply.WrongLeader = true
		reply.Err=  ErrWrongLeader
		return
	}
	op := Op{
		ClientId: args.ClientId,
		SeqId:	args.SeqId,
		OpType: JoinOp,
		Servers: args.Servers,
	}
	lastIndex,_,_ := sc.rf.Start(op)
	op.Index = lastIndex
	//DPrintf
	// 为了获取raftStart对应下标的缓冲通道
	ch := sc.getWaitCh(lastIndex)
	defer func(){
		sc.mu.Lock()
		delete(sc.waitChMap,op.Index)
		sc.mu.Unlock()
	}()
	// 设置超时ticker
	timer := time.NewTicker(500  * time.Millisecond)
	defer timer.Stop()

	select{
	case replyOp := <- ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId{
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		}else{
			reply.Err = OK
			reply.WrongLeader = false
			return
		}
	case <- timer.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}




}

// 将给定的一个组进行撤离，并且进行负载均衡
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if sc.Killed(){
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	_,ifLeader := sc.rf.GetState()
	if !ifLeader{
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	op := Op{
		ClientId: args.ClientId,
		SeqId:	args.SeqId,
		OpType: LeaveOp,
		GIDs: args.GIDs,
	}
	lastIndex,_,_ := sc.rf.Start(op)
	op.Index = lastIndex
	ch := sc.getWaitCh(lastIndex)
	defer func(){
		sc.mu.Lock()
		delete(sc.waitChMap,op.Index)
		sc.mu.Unlock()
	}()

	
	// 设置超时ticker
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()

	select{
	case replyOp := <-ch:
		//fmt.Printf("[ ----Server[%v]----] : receive a GetAsk :%+v,replyOp:+%v\n", sc.me, args, replyOp)
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.WrongLeader = false
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true

	}

}

// 为指定的分片，分配指定的组
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	if sc.Killed() {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	op := Op{
		ClientId: args.ClientId,
		SeqId: args.SeqId, 
		OpType: MoveOp, 
		GID: args.GID, 
		Shard: args.Shard,
	}
	// 封装Op传到下层start
	//fmt.Printf("[ ----Server[%v]----] : send a Get,op is :%+v \n", sc.me, op)
	lastIndex, _, _ := sc.rf.Start(op)
	op.Index = lastIndex

	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, op.Index)
		sc.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		//fmt.Printf("[ ----Server[%v]----] : receive a GetAsk :%+v,replyOp:+%v\n", sc.me, args, replyOp)
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.WrongLeader = false

			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true

	}
}

// 查询特定的版本，如果给定的版本号不存在切片下标中，直接返回最新配置版本
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if sc.Killed() {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true

		return
	}
	op := Op{ClientId: args.ClientId, SeqId: args.SeqId, OpType: QueryOp, Num: args.Num}

	lastIndex, _, _ := sc.rf.Start(op)
	op.Index = lastIndex

	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, op.Index)
		sc.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()

	select{
	case replyOp := <- ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId{
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		}else{
			reply.Err = OK
			reply.Config = replyOp.Cfg
			reply.WrongLeader = false
			return
	
		}
	case <- timer.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}


}
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead,1)
}

func (sc *ShardCtrler) Killed() bool{
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}


// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.seqMap = make(map[int64]int)
	sc.waitChMap = make(map[int]chan Op)

	go sc.applyMsgHandlerLoop()

	return sc
}

func (sc *ShardCtrler) UnregisterTask(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.waitChMap, index)
}
func (sc *ShardCtrler) ifDuplicate(clientId int64, seqId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	lastSeqId, exist := sc.seqMap[clientId]
	//DPrintf(111, "check if duplicate: lastSeqId is %d and seqId is %d", lastSeqId, seqId)
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}


func (sc *ShardCtrler) applyMsgHandlerLoop(){
	for{
		if sc.Killed(){
			return
		}
		select{
		case msg := <- sc.applyCh:
			// 如果是命令消息，那么应用命令勇士相应客户端
			if msg.CommandValid{
				index := msg.CommandIndex
				// 传来的信息快照如果已经存储直接返回
				op := msg.Command.(Op)
				if !sc.ifDuplicate(op.ClientId, op.SeqId){
					sc.mu.Lock()
					switch op.OpType{
					case JoinOp:
						sc.execJoinCmd(&op)
					case MoveOp:
						sc.execMoveCmd(&op)
					case QueryOp:
						sc.execQueryCmd(&op)
					case LeaveOp:
						sc.execLeaveCmd(&op)
					}
					sc.seqMap[op.ClientId] = op.SeqId
					op.Err = OK
					sc.mu.Unlock()
				}
				// 将命令返回给waitCH
				if _,isLead := sc.rf.GetState(); isLead{
					sc.getWaitCh(index) <- op
				}
			}else{
				log.Fatalf("yukinoshita yukino!")
			}
		}
	}
}
