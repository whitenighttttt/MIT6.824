package raft

import(
	"math/rand"
	"time"
	// "fmt"
)

const baseElectionTimeout = 300
const None = -1

// 开始选举实现
func (rf* Raft) StartElection(){
	if rf.killed(){
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.becomeCandidate()
	term := rf.currentTerm
	done := false
	votes := 1
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.log.LastLogIndex
	args.LastLogTerm = rf.getLastEntryTerm()
	defer rf.persist()

	for i,_ := range rf.peers{
		if rf.me == i{
			continue
		}
		// 开携程去拉选票
		go func(serverId int){
			var reply RequestVoteReply
			ok := rf.sendRequestVote(serverId,&args,&reply)
			if !ok || !reply.VoteGranted{
				return 
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 丢弃无效票
			if rf.currentTerm > reply.Term{
				DPrintf(111, "%v: reply.Term is %d, refuse to gather", rf.SayMeL(), reply.Term)
				return
			}
			votes++
			// 不够
			if done || votes <= len(rf.peers)/2{
				return
			}
			done = true
			if rf.state != Candidate || rf.currentTerm != term{
				return
			}
			//fmt.Println("id:",rf.me,"获得半数以上选票，选票数为：",votes,"将成为leader(currentTerm=",rf.currentTerm,",state =",rf.state,")")
			rf.becomeLeader()
			DPrintf(222, "\n[%d] got enough votes, and now is the leader(currentTerm=%d, state=%v)!starting to append heartbeat...\n", rf.me, rf.currentTerm, rf.state)
			// 立即发送心跳实现,当选以后给其他节点发送心跳压制
			go rf.StartAppendEntries(true)
		}(i)
	}

}

func (rf *Raft) pastElectionTimeout() bool{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	f := time.Since(rf.lastElection) > rf.electionTimeout
	return f
}

func (rf* Raft) resetElectionTimer(){
	electionTimeout := baseElectionTimeout + (rand.Int63()%baseElectionTimeout) // 300-600
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElection = time.Now()
	DPrintf(222, "%v: %d has refreshed the electionTimeout at term %d to a random value %d...\n", rf.SayMeL(), rf.me, rf.currentTerm, rf.electionTimeout/1000000)
} 

func (rf* Raft) becomeCandidate(){
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	//fmt.Println("选举时间超时! id:",rf.me,"转换为Candidate!")
} 

func (rf* Raft) becomeLeader(){
	rf.state = Leader
	DPrintf(100, "%v :becomes leader and reset TrackedIndex\n", rf.SayMeL())
	rf.resetTrackedIndex()
}


// 定义一个心跳&日志的同步处理器，这个方法是candidate和follower节点的处理
func (rf* Raft) HandleHeartbeatRPC(args *RequestAppendEntriesArgs,reply *RequestAppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.FollowerTerm = rf.currentTerm
	reply.Success = true
	// 如果任期不合法，抛弃
	if args.LeaderTerm < rf.currentTerm{
		reply.Success = false
		reply.FollowerTerm = rf.currentTerm
		return
	}
	DPrintf(200, "%v: I am now receiving heartbeat from leader %d at term %d", rf.SayMeL(), args.LeaderId, args.LeaderTerm)
	rf.resetElectionTimer()
	rf.state = Follower
	rf.votedFor = args.LeaderId

	// 承认来着是一个合法的新leader
	if args.LeaderTerm > rf.currentTerm{
		DPrintf(111, "%v: leader %d's term at %d is greater than me", rf.SayMeL(), args.LeaderId, args.LeaderTerm)
		rf.votedFor = None
		rf.currentTerm = args.LeaderTerm
		reply.FollowerTerm = rf.currentTerm
	}
}

// // type RequestVoteArgs struct {
// 	Term int
// 	CandidateId int
// type RequestVoteReply struct {
// 	Term int 
// 	VoteGranted bool
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = true
	reply.Term = rf.currentTerm

	// 竞选任期<=自己任期 反对
	if args.Term < rf.currentTerm {
		DPrintf(111, "%v: 投出反对票给节点%d, 原因：任期", rf.SayMeL(), args.CandidateId)
		reply.VoteGranted = false
		rf.persist()
		return 
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = None
		rf.state = Follower	
		reply.Term = rf.currentTerm
	}
	

	update := false 
	update = update || args.LastLogTerm  > rf.getLastEntryTerm()
	update = update || args.LastLogTerm == rf.getLastEntryTerm() && args.LastLogIndex >= rf.log.LastLogIndex 
	
	if(rf.votedFor == -1 || rf.votedFor == args.CandidateId) && update{
		rf.votedFor  = args.CandidateId
		rf.state = Follower
		rf.resetElectionTimer()
		rf.persist()
		DPrintf(111, "%v: 投出同意票给节点%d", rf.SayMeL(), args.CandidateId)
	} else{
		reply.VoteGranted = false
		DPrintf(111, "%v: 投出反对票给节点%d， 原因：我已经投票给%d", rf.SayMeL(), args.CandidateId, rf.votedFor)
	}

}

