package raft
import "fmt"

/*
index 代码快照apply应用的index, snapshot表示上层Service传来的快照字节流，包含之前index的数据
Snapshot主要目的是 把安装到快照里的日志抛弃掉,同时更新自身快照下标，属于peers自身主动更新，与leader发送快照不冲突
*/
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader{
		return
	}
	DPrintf(11, "%v: come Snapshot index=%v", rf.SayMeL(), index)
	if rf.log.FirstLogIndex <= index{
		// 如果下标大于自身的提交，说明没被提交不能安装快照,如果自身快照点大于index说明不需要安装
		if index > rf.lastApplied{
			panic(fmt.Sprintf("%v: index=%v rf.lastApplied=%v\n", rf.SayMeL(), index, rf.lastApplied))
		}
		rf.snapshot = snapshot
		rf.snapshotLastIncludeIndex = index
		rf.snapshotLastIncludeTerm = rf.getEntryTerm(index)

		newFirstLogIndex := index + 1
		if newFirstLogIndex <= rf.log.LastLogIndex{
			rf.log.Entries = rf.log.Entries[newFirstLogIndex - rf.log.FirstLogIndex:]
			DPrintf(111, "%v: 被快照截断后的日志为: %v", rf.SayMeL(), rf.log.Entries)
		}else{
			rf.log.LastLogIndex = newFirstLogIndex - 1
			rf.log.Entries = make([]Entry,0)
		}
		rf.log.FirstLogIndex = newFirstLogIndex
		rf.commitIndex = max(rf.commitIndex,index)
		rf.lastApplied = max(rf.lastApplied,index)
		DPrintf(111, "%v:进行快照后，更新commitIndex为%d, lastApplied为%d, "+
			"但是snapshotLastIncludeIndex是%d", rf.SayMeL(), rf.commitIndex, rf.lastApplied, rf.snapshotLastIncludeIndex)
		rf.persist()
		for i:=0; i<len(rf.peers); i++{
			if i == rf.me{
				continue
			}
			go rf.InstallSnapshot(i)
		}
	}
}

func (rf *Raft) InstallSnapshot(serverId int){
	args := RequestInstallSnapShotArgs{}
	reply := RequestInstallSnapShotReply{}
	rf.mu.Lock()
	if rf.state != Leader{
		DPrintf(111, "%v: 状态已变，不是leader节点，无法发送快照", rf.SayMeL())
		rf.mu.Unlock()
		return
	}
	DPrintf(111, "%v: 准备向节点%d发送快照", rf.SayMeL(), serverId)
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludeIndex = rf.snapshotLastIncludeIndex
	args.LastIncludeTerm = rf.snapshotLastIncludeTerm
	args.Snapshot = rf.snapshot
	rf.mu.Unlock()

	ok := rf.sendRequestInstallSnapshot(serverId,&args,&reply)
	if !ok{
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader{
		DPrintf(111, "%v: 因为不是leader，放弃处理%d的快照响应", rf.SayMeL(), serverId)
		return
	}
	if reply.Term < rf.currentTerm{
		DPrintf(111, "%v: 因为是旧的快照响应，放弃处理%d的快照响应, 旧响应的任期是%d", rf.SayMeL(), serverId, reply.Term)
		return
	}

	if reply.Term > rf.currentTerm{
		rf.votedFor = -1
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.persist()
		return
	}
	rf.peerTrackers[serverId].nextIndex = args.LastIncludeIndex +1 
	rf.peerTrackers[serverId].matchIndex = args.LastIncludeIndex
	rf.tryCommitL(rf.peerTrackers[serverId].matchIndex)
}

func (rf *Raft) sendRequestInstallSnapshot(server int,args *RequestInstallSnapShotArgs,reply* RequestInstallSnapShotReply)bool{
	ok := rf.peers[server].Call("Raft.RequestInstallSnapshot",args,reply)
	return ok
}

func (rf *Raft) RequestInstallSnapshot(args *RequestInstallSnapShotArgs,reply *RequestInstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term  = rf.currentTerm
	if args.Term < rf.currentTerm{
		return
	}
	rf.state = Follower
	rf.resetElectionTimer()
	if args.Term > rf.currentTerm{	
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}
	defer rf.persist()
	
	// 超出当前范围，所以需要给当前节点增加log
	if args.LastIncludeIndex > rf.snapshotLastIncludeIndex{
		DPrintf(800, "%v: before install snapshot from leader %d: leader.log=%v", rf.SayMeL(), args.LeaderId, rf.log)
		rf.snapshot = args.Snapshot
		rf.snapshotLastIncludeIndex = args.LastIncludeIndex
		rf.snapshotLastIncludeTerm = args.LastIncludeTerm
		if args.LastIncludeIndex >= rf.log.LastLogIndex{
			rf.log.Entries = make([]Entry,0)
			rf.log.LastLogIndex = args.LastIncludeIndex
		}else{
			rf.log.Entries = rf.log.Entries[rf.log.getRealIndex(args.LastIncludeIndex+1):]
		}
		rf.log.FirstLogIndex = args.LastIncludeIndex  + 1
		
		if args.LastIncludeIndex > rf.lastApplied{
			msg := ApplyMsg{
				SnapshotValid:		true,
				Snapshot:			rf.snapshot,
				SnapshotTerm:		rf.snapshotLastIncludeTerm,
				SnapshotIndex:		rf.snapshotLastIncludeIndex,
			}
			rf.applyHelper.tryApply(&msg)
			rf.lastApplied = args.LastIncludeIndex
		}
		rf.commitIndex = max(rf.commitIndex,args.LastIncludeIndex)
	}
	DPrintf(111, "%v: successfully installing snapshot from LeaderId=%v with args.LastIncludeIndex=%v, args.LastIncludeTerm=%v\n", rf.SayMeL(), args.LeaderId, args.LastIncludeIndex, args.LastIncludeTerm)

}