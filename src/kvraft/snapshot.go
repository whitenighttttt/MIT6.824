package kvraft
import(
	"6.5840/labgob"
	"bytes"
)

func (kv *KVServer) isNeedSnapshot() bool{
	if kv.maxraftstate == -1{
		return false
	}
	len := kv.persister.RaftStateSize()
	return len - 100 >= kv.maxraftstate
}

// 制作快照
func (kv *KVServer) makeSnapshot(index int){
	_, isleader := kv.rf.GetState()
	if !isleader{
		//Dprintf(1111,"非Leader节点！")
		return 
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 开始编码
	e.Encode(kv.kvPersist)
	e.Encode(kv.seqMap)
	snapshot := w.Bytes()

	// 发送快照给leader系欸但
    kv.rf.Snapshot(index,snapshot)
}

// 解码快照
func (kv *KVServer) decodeSnapshot(index int, snapshot []byte){
	// 节点第一次启动时，持久化数据为空，所以需要判空
	if snapshot == nil || len(snapshot) < 1{
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.lastIncludeIndex = index

	if d.Decode(&kv.kvPersist) != nil || d.Decode(&kv.seqMap) != nil{
		panic("err in decoding snapshot!")
	}
}