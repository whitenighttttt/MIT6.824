package raft
import "time"

func (rf *Raft) pastHeartbeatTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.lastHeartbeat) > rf.heartbeatTimeout
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartbeat = time.Now()
}

// nextIndex收敛速度优化，nextIndex跳跃算法
// 定义一个心跳兼日志同步处理器，这个方法是Candidate & Follower节点的处理
func (rf *Raft) HandleAppendEnreiesRPC(args *RequestAppendEntriesArgs,reply *RequestAppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.FollowerTerm = rf.currentTerm
	reply.Success = true
	// 抛弃
	if args.LeaderTerm < rf.currentTerm{
		reply.Success = false
		return
	}
	rf.resetElectionTimer()
	rf.state = Follower

	if args.LeaderTerm > rf.currentTerm{
		rf.votedFor = None
		rf.currentTerm = args.LeaderTerm
		reply.FollowerTerm = rf.currentTerm
	}

	defer rf.persist()

	// 加入非空判断
	if rf.log.empty(){
		// 首先可以确定的是，主结点的args.PrevLogIndex = min(rf.nextIndex[i]-1, rf.lastLogIndex)
		// 这可以比从节点的rf.snapshotLastIncludeIndex大、小或者等价， 因为可以根据
		// args.PrevLogIndex的计算式子得出，nextIndex在leader刚选出时是0，
		// 日志为空，要么是节点刚启动的初始状态，要么是被快照截断后的状态
		// 在初始状态，两者都是0，从节点可以全部接收日志，
		// 若被日志截断，则rf.snapshotLastIncludeIndex前面的日志都是无效的，
		// args.PrevLogIndex >  rf.snapshotLastIncludeIndex 这部分
		// 日志肯定不能插入，所以也会丢弃
		if args.PrevLogIndex == rf.snapshotLastIncludeIndex{
			rf.log.appendL(args.Entries...)
			reply.FollowerTerm = rf.currentTerm
			reply.Success = true
			reply.PrevLogIndex = rf.log.LastLogIndex
			reply.PrevLogTerm = rf.getLastEntryTerm()
			return
		}else{
			reply.FollowerTerm = rf.currentTerm
			reply.Success = false
			reply.PrevLogIndex = rf.log.LastLogIndex
			reply.PrevLogTerm = rf.getLastEntryTerm()
			return
		}
	}

	// 收到的log不符合要求
	if args.PrevLogIndex+1 < rf.log.FirstLogIndex || args.PrevLogIndex > rf.log.LastLogIndex{
		DPrintf(111, "args.PrevLogIndex is %d, out of index...", args.PrevLogIndex)
		reply.FollowerTerm = rf.currentTerm
		reply.Success = false
		reply.PrevLogIndex = rf.log.LastLogIndex
		reply.PrevLogTerm = rf.getLastEntryTerm()
	}else if rf.getEntryTerm(args.PrevLogIndex) == args.PrevLogTerm{
		// 请求的上一个index对应的rf的任期 和 请求的任期对应的上
		ok := true
		for i,entry := range args.Entries{
			index := args.PrevLogIndex + 1 + i
			if index > rf.log.LastLogIndex{
				rf.log.appendL(entry)
			}else if rf.log.getOneEntry(index).Term != entry.Term{
				ok = false
				*rf.log.getOneEntry(index) = entry
			}
		}
		if !ok{
			rf.log.LastLogIndex = args.PrevLogIndex + len(args.Entries)
		}
		if args.LeaderCommit > rf.commitIndex{
			if args.LeaderCommit < rf.log.LastLogIndex{
				rf.commitIndex = args.LeaderCommit
			}else{
				rf.commitIndex = rf.log.LastLogIndex
			}
			rf.applyCond.Broadcast()
		}
		reply.FollowerTerm = rf.currentTerm
		reply.Success = true
		reply.PrevLogIndex = rf.log.LastLogIndex
		reply.PrevLogTerm = rf.getLastEntryTerm()
		DPrintf(200, "%v:log entries was overrited, added or done nothing, updating commitIndex to %d...", rf.SayMeL(), rf.commitIndex)
	}else{
		// 对应不上
		prevIndex := args.PrevLogIndex
		for prevIndex >= rf.log.FirstLogIndex && rf.getEntryTerm(prevIndex)==rf.log.getOneEntry(args.PrevLogIndex).Term{
			prevIndex--
		}
		reply.FollowerTerm = rf.currentTerm
		reply.Success = false
		if prevIndex >= rf.log.FirstLogIndex{
			reply.PrevLogIndex = prevIndex
			reply.PrevLogTerm = rf.getEntryTerm(prevIndex)
			DPrintf(111, "%v: stepping over the index of currentTerm to the last log entry of last term", rf.SayMeL())
		}else{
			reply.PrevLogIndex = rf.snapshotLastIncludeIndex
			reply.PrevLogTerm = rf.snapshotLastIncludeTerm
		}
	}
}

// 主节点对日志进行提交，其条件是多余一半的从节点的commitIndex>=leader节点当前提交的commitIndex
func (rf *Raft) tryCommitL(matchIndex int) {

	if matchIndex <= rf.commitIndex {
		// 首先matchIndex应该是大于leader节点的commitIndex才能提交，因为commitIndex及其之前的不需要更新
		return
	}
	// 越界的也不能提交
	if matchIndex > rf.log.LastLogIndex {
		return
	}
	if matchIndex < rf.log.FirstLogIndex {
		return
	}
	// 提交的必须本任期内从客户端收到的日志
	if rf.getEntryTerm(matchIndex) != rf.currentTerm {
		return
	}

	// 计算所有已经正确匹配该matchIndex的从节点的票数
	cnt := 1 //自动计算上leader节点的一票
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// 为什么只需要保证提交的matchIndex必须小于等于其他节点的matchIndex就可以认为这个节点在这个matchIndex记录上正确匹配呢？
		// 因为matchIndex是增量的，如果一个从节点的matchIndex=10，则表示该节点从1到9的子日志都和leader节点对上了
		if matchIndex <= rf.peerTrackers[i].matchIndex {
			cnt++
		}
	}
	//DPrintf(2, "%v: rf.commitIndex = %v ,trycommitindex=%v,matchindex=%v cnt=%v", rf.SayMeL(), rf.commitIndex, index, rf.matchIndex, cnt)
	// 超过半数就提交
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	if cnt > len(rf.peers)/2 {
		rf.commitIndex = matchIndex
		if rf.commitIndex > rf.log.LastLogIndex {
			DPrintf(999, "%v: commitIndex > lastlogindex %v > %v", rf.SayMeL(), rf.commitIndex, rf.log.LastLogIndex)
			panic("")
		}
		// DPrintf(500, "%v: commitIndex = %v ,entries=%v", rf.SayMeL(), rf.commitIndex, rf.log.Entries)
		DPrintf(199, "%v: 主结点已经提交了index为%d的日志，rf.applyCond.Broadcast(),rf.lastApplied=%v rf.commitIndex=%v", rf.SayMeL(), rf.commitIndex, rf.lastApplied, rf.commitIndex)
		rf.applyCond.Broadcast() // 通知对应的applier协程将日志放到状态机上验证
	} else {
		DPrintf(199, "\n%v: 未超过半数节点在此索引上的日志相等，拒绝提交....\n", rf.SayMeL())
	}
}