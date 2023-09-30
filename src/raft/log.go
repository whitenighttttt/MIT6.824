package raft

import "fmt"

type Entry struct{
	Term 		int
	Command		interface{}
}

type Log struct{
	Entries  	[]Entry // int数组类型,记录term,从1开始
	FirstLogIndex	int // 记录最后term的最早位置
	LastLogIndex	int // 最后一个log所在的下标
}

func NewLog() *Log{
	return &Log{
		Entries:	make([]Entry,0),
		FirstLogIndex:1,
		LastLogIndex:0,
	}
}


// 获取距离最近index的偏移量
func (log *Log) getRealIndex(index int)int{
	return index - log.FirstLogIndex
}
// 创建对应数量的Entry
func (log *Log) getOneEntry(index int)  *Entry{
	return &log.Entries[log.getRealIndex(index)]
}
// 这是一个可变参数列表，表示可以接受任意数量的Entry类型的参数。
// 在当前log后面直接加入新的term ,没有覆盖？
func (log *Log) appendL(newEntries ...Entry){
	log.Entries = append(log.Entries[:log.getRealIndex(log.LastLogIndex)+1],newEntries...)
	log.LastLogIndex += len(newEntries)
}
// 获取最近偏移量的log数组
func (log *Log) getAppendEntries(start int)[]Entry{
	ret := append([]Entry{}, log.Entries[log.getRealIndex(start):log.getRealIndex(log.LastLogIndex)+1]...)
	return ret
}
func (log *Log) String() string{
	if log.empty(){
		return "logempty!"
	}
	return fmt.Sprintf("%v", log.getAppendEntries(log.FirstLogIndex))
}

func (log *Log) empty() bool{
	return log.FirstLogIndex > log.LastLogIndex
}

func (rf *Raft) GetLogEntries() []Entry{
	return rf.log.Entries
}

func (rf *Raft) getEntryTerm(index int) int {
	if index == 0 {
		return 0
	}
	// if index == rf.log.FirstLogIndex-1 {
	// 	return rf.snapshotLastIncludeTerm
	// }
	if rf.log.FirstLogIndex <= rf.log.LastLogIndex {
		return rf.log.getOneEntry(index).Term
	}

	DPrintf(999, "invalid index=%v in getEntryTerm rf.log.FirstLogIndex=%v rf.log.LastLogIndex=%v\n", index, rf.log.FirstLogIndex, rf.log.LastLogIndex)
	return -1
}
