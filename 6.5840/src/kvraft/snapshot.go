package kvraft

import (
	"6.5840/labgob"
	"bytes"
)

func (kv *KVServer) isNeedSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	len := kv.persister.RaftStateSize()

	return len-100 >= kv.maxraftstate
}

// 制作快照
func (kv *KVServer) makeSnapshot(index int) {
	_, isleader := kv.rf.GetState()
	if !isleader {
		DPrintf(11111, "非leader节点，无权限做快照")
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf(11111, "是leader，准备编码")

	e.Encode(kv.kvPersist)
	e.Encode(kv.seqMap)
	snapshot := w.Bytes()
	DPrintf(11111, "快照制作完成，准备发送快照")
	// 快照完马上递送给leader节点
	go kv.rf.Snapshot(index, snapshot)
	DPrintf(11111, "完成快照发送")
	DPrintf(11111, "print快照数据：")
}

// 解码快照
func (kv *KVServer) decodeSnapshot(index int, snapshot []byte) {

	// 这里必须判空，因为当节点第一次启动时，持久化数据为空，如果还强制读取数据会报错
	if snapshot == nil || len(snapshot) < 1 {
		DPrintf(11111, "持久化数据为空！")
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.lastIncludeIndex = index

	if d.Decode(&kv.kvPersist) != nil || d.Decode(&kv.seqMap) != nil {
		DPrintf(999, "%v: readPersist snapshot decode error\n", kv.rf.SayMeL())
		panic("error in parsing snapshot")
	}
}