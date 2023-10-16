package shardctrler

import "sort"

// join
func (sc* ShardCtrler) execJoinCmd(op *Op){
	sc.RebalnceShardsForJoin(op.Servers)
}

func (sc* ShardCtrler) RebalnceShardsForJoin(newGroups map[int][]string){
	// 获得最新的配置
	oldConfig := sc.configs[len(sc.configs) - 1]
	newConfig := Config{
		Num:		oldConfig.Num + 1,
		Shards: 	oldConfig.Shards,
		Groups:		make(map[int][]string),
	}
	DPrintf(111, "join之前，检查到旧的复制组为:%v", oldConfig.Groups)
	// 合并
	for gid,servers := range oldConfig.Groups{
		newConfig.Groups[gid] = servers
	}
	for gid,servers := range newGroups{
		newConfig.Groups[gid] = servers
	}

	// 计算枫树 负载均衡
	totalShards  := len(oldConfig.Shards)
	totalGroups := len(newConfig.Groups)
	shardsPerGroup := totalShards/totalGroups
	extraShards := totalShards % totalGroups

	// 获取复制组id按照顺序排序
	// shardCounts := make(map[int]int)是一个map，在后面的遍历时for newGid, 
	//count := range shardCounts，这是乱序的，因为这个方法会在不同的状态机中执行，所以会导致分片结果不一致
	// 建立一个 group_id -> shard_id的映射 
	groupsIDs := make([]int,0,len(newConfig.Groups))
	for gid := range newConfig.Groups{
		groupsIDs = append(groupsIDs,gid)
	}
	sort.Ints(groupsIDs)
	// 计算每个复制组需要的分片数量
	shardCounts := make(map[int]int)
	// 按照顺序为每个gid分配分片
	for _,gid := range groupsIDs{
		shardCounts[gid] = shardsPerGroup
		if extraShards > 0{
			shardCounts[gid] ++
			extraShards--
		}
	}
	// 重新分片
	/*大概思想是遍历每一个分片，如果该分片对应的gid所代表的复制组需要的分片为0
	表示这个分片可以分给其他组，
	所以遍历所有复制组直到找到一个需要分片数大于0的组
	*/
	for shard,gid := range newConfig.Shards{
		if shardCounts[gid] <= 0 {
			// 遍历已经排序的复制组id
			for _,newGid := range groupsIDs{
				count := shardCounts[newGid]
				if count > 0{
					newConfig.Shards[shard] = newGid
					shardCounts[newGid]--
					break
				}
			}
		}else{
			shardCounts[gid]--
		}
	}
	sc.configs = append(sc.configs,newConfig)
}

func (sc *ShardCtrler) execMoveCmd(op *Op){
	sc.MoveShard(op.Shard,op.GID)
}
func (sc *ShardCtrler) MoveShard(shardId int,GID int){
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:		oldConfig.Num+1,
		Shards:		oldConfig.Shards, //复制后在更新
		Groups:		make(map[int][]string),
	}
	// 复制复制组信息
	for gid,servers := range oldConfig.Groups{
		copiedServers := make([]string,len(servers))
		copy(copiedServers,servers)
		newConfig.Groups[gid] = copiedServers
	}

	// move 移动目标shard到目标分组
	if _,exist := newConfig.Groups[GID]; exist{
		newConfig.Shards[shardId] = GID
	}else{
		return
	}
	sc.configs = append(sc.configs,newConfig)
}

func (sc*ShardCtrler) execQueryCmd(op *Op){
	if op.Num == -1 || op.Num >= len(sc.configs){
		op.Cfg  = sc.configs[len(sc.configs)-1]
		return
	}
	op.Cfg = sc.configs[op.Num]
}

func (sc*ShardCtrler) execLeaveCmd(op *Op){
	sc.RebalnceShardsForLeave(op.GIDs)
}

func (sc*ShardCtrler) RebalnceShardsForLeave(removeGIDs []int){
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:		oldConfig.Num+1,
		Shards:		oldConfig.Shards, //复制后在更新
		Groups:		make(map[int][]string),
	}

	// 将removeGIDs转换为Map，方便查找
	removeMap := make(map[int]bool)
	for _,gid := range removeGIDs{
		removeMap[gid] = true
	}
	// 合并就的复制组，排除掉要移除的
	for gid,servers := range oldConfig.Groups{
		if !removeMap[gid]{
			newConfig.Groups[gid] = servers
		}
	}
	// 如果集群中复制组为0，需要将所有分片的GID指定为0
	if len(newConfig.Groups) == 0{
		for shard,_ := range newConfig.Shards{
			newConfig.Shards[shard] = 0
		}
		sc.configs = append(sc.configs, newConfig)
		return
	}
	// 计算目标分片书
	totalShards := len(oldConfig.Shards)
	totalGroups := len(newConfig.Groups)
	if totalGroups == 0{
		return
	}
	shardsPerGroup := totalShards / totalGroups
	extraShards := totalShards % totalGroups

	groupIDs := make([]int,0,len(newConfig.Groups))
	for gid := range newConfig.Groups{
		groupIDs =  append(groupIDs,gid)
	}
	sort.Ints(groupIDs)
	// 重新分配
	shardCounts := make(map[int]int)
	// 按照顺序为每个gid分配分片
	for _,gid := range groupIDs{
		shardCounts[gid] = shardsPerGroup
		if extraShards >0{
			shardCounts[gid]++
			extraShards--
		}
	}

	// 重新分配分片
	for shard,gid := range newConfig.Shards{
		// 当前组是否应该被删除 || 当前组分片数量达到指定数量
		if removeMap[gid] || shardCounts[gid]<=0{
			for _,newGid := range groupIDs{
				// 在新的组列表中寻找可以接收当前分片的组
				count := shardCounts[newGid]
				if count > 0{
					newConfig.Shards[shard] = newGid
					shardCounts[newGid] -- 
					break
				}
			}
		}else{
			shardCounts[gid]--
		}
	}
	sc.configs = append(sc.configs,newConfig)

}
