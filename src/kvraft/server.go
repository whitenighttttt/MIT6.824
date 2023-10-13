package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SeqId    int
	Key      string
	Value    string
	ClientId int64
	Index    int // raft服务层传来的Index
	OpType   string
}
// kv服务层数据结构
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	seqMap    map[int64]int     // 确保只执行一次	clientId -> seqId
	waitChMap map[int]chan Op   // 传递由下层Raft服务的appCh传过来的command	index -> chan(Op)
	kvPersist map[string]string // 存储持久化的KV键值对	K->V

	// snapshot
	lastApplied		int // 最近一次应用日志命令所在索引
	lastIncludeIndex	int // 最近一次应用快照所在日志索引
	persister		*raft.Persister // 共享raft持久化地址
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, kv.persister, kv.applyCh)

	// You may need initialization code here.
	kv.seqMap = make(map[int64]int)
	kv.kvPersist = make(map[string]string)
	kv.waitChMap = make(map[int]chan Op)

	kv.decodeSnapshot(kv.rf.GetLastIncludeIndex(), kv.persister.ReadSnapshot())
	go kv.applyMsgHandlerLoop()
	return kv
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 1.当接收到Get/PutAppend请求时
	//  先判断该客户端的该序列号的请求是否已经被正确处理并响应过了，如果有则返回之前响应的结果
	// 2.判断当前是否为领袖。
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// 3.生成命令传递给下层raft
	op := Op{
		OpType: GetOp,
		 Key: args.Key, 
		 SeqId: args.SeqId, 
		 ClientId: args.ClientId,
	}
	lastIndex, _, _ := kv.rf.Start(op)
	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	// 4.等待应用后返回消息
	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.mu.Lock()
			// 5.只有成功才会进行持久化操作
			reply.Value = kv.kvPersist[args.Key]
			kv.mu.Unlock()
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}

}
func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan Op, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}
// raft通过applyCh提交日志到状态机中，因此需要让状态机应用命令，需要一个不断接收applyCh中日志的协程
// 如果raft返回的操作是Put/append，
// 那么判断持久化后的大小是否达到maxraftstate,如果达到则说明需要发送快照
func (kv *KVServer) applyMsgHandlerLoop() {
	for {
		if kv.killed() {
			return
		}
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid{
				index := msg.CommandIndex
				op := msg.Command.(Op)
				if !kv.ifDuplicate(op.ClientId, op.SeqId) {
					kv.mu.Lock()
					switch op.OpType {
					case PutOp:
						kv.kvPersist[op.Key] = op.Value
						DPrintf(1111, "put后，结果为%v", kv.kvPersist[op.Key])
	
					case AppendOp:
						kv.kvPersist[op.Key] += op.Value
						DPrintf(1111, "Append后，结果为%v", kv.kvPersist[op.Key])
	
					}
					kv.seqMap[op.ClientId] = op.SeqId
					// 如果需要制作快照
					if kv.isNeedSnapshot(){
						go kv.makeSnapshot(msg.CommandIndex)
					}
					kv.mu.Unlock()
				}
				// 将返回的ch返回waitCh
				kv.getWaitCh(index) <- op
			}else if msg.SnapshotValid{
				kv.decodeSnapshot(msg.SnapshotIndex,msg.Snapshot)
			}
		}
	}
}

// 判断是否是重复操作的也比较简单,因为对seq进行递增，所以直接比大小即可
func (kv *KVServer) ifDuplicate(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId, exist := kv.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	// 1.判断该命令是否被执行过了
	// 2.判断是否是leader
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 3.封装Op传到下层start
	op := Op{
		 OpType: args.Op,
		 Key: args.Key, 
		 Value: args.Value, 
		 SeqId: args.SeqId, 
		 ClientId: args.ClientId,
	}
	lastIndex, _, _ := kv.rf.Start(op)
	op.Index = lastIndex
	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()

	// 4.等待应用后返回消息
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case replyOp := <-ch:
		// 通过clientId、seqId确定唯一操作序列
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}

	case <-timer.C:
		reply.Err = ErrWrongLeader
	}

	defer timer.Stop()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	DPrintf(11, "%v: is killed", kv)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}