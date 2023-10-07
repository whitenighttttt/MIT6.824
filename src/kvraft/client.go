package kvraft

import (
	"6.5840/labrpc"
	"crypto/rand"
	"math/big"
)


type Clerk struct {
	servers []*labrpc.ClientEnd

	seqId    int
	leaderId int // 上一次RPC发现的主机iD
	clientId int64 // 客户端id
}

// 初始化
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	//ck.leaderId = mathrand.Intn(len(ck.servers))
	ck.leaderId = int(nrand()) % len(ck.servers)

	return ck
}
// 正常查询到结果不存在才会结束该次get，否则一直重试
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.seqId++
	args := GetArgs{
		Key: key, 
		ClientId: ck.clientId,
		SeqId: ck.seqId
	}
	serverId := ck.leaderId
	for {
		reply := GetReply{}
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == ErrNoKey {
				ck.leaderId = serverId
				return ""
			} else if reply.Err == OK {
				ck.leaderId = serverId
				return reply.Value
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}

		// 节点发生crash等原因
		serverId = (serverId + 1) % len(ck.servers)

	}
}

// 同上所示
func (ck *Clerk) PutAppend(key string, value string, op string) {

	ck.seqId++
	serverId := ck.leaderId
	args := PutAppendArgs{
		Key: key, 
		Value: value, 
		Op: op, 
		ClientId: ck.clientId, 
		SeqId: ck.seqId
	}
	for {

		reply := PutAppendReply{}
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}

		serverId = (serverId + 1) % len(ck.servers)

	}

}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOp)
}