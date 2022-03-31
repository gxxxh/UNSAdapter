package cluster_manager

import (
	"UNSAdapter/pb_gen/objects"
	"fmt"
	"sync"
)

//todo 管理资源的占用
type ClusterManager struct {
	clusterID string
	mu *sync.RWMutex
	//resourceManager *local.ResourceManager
	clusterInfo *objects.Cluster
	partitionID2PaititionManager map[string]*PartitionManager
}




//type AccleratorManager struct{
//
//}


func NewClusterManager()(*ClusterManager){
	//todo
	return nil
}

func (cm *ClusterManager)GetPartitionManager(partitionID string)(*PartitionManager, error){
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	partitionManager, ok:= cm.partitionID2PaititionManager[partitionID]
	if !ok{
		errorMsg := fmt.Sprintf("no paritionManager for Partition %s in cluster %s", partitionID, cm.clusterID)
		return nil, fmt.Errorf(errorMsg)
	}
	return partitionManager,nil
}

func (cm *ClusterManager)CheckJobResources(allocation *objects.JobAllocation)(bool, error){
	partitionManager, err := cm.GetPartitionManager(allocation.JobID)
	if err!=nil{
		return false, err
	}
	for _, taskalloc := range allocation.TaskAllocations{
		//一个task对应一个node上的多个CPUSocket和1个accelerator
		res, err := partitionManager.CheckTaskResources(taskalloc)
		if err!=nil||!res{
			return false, err
		}
	}
	return true, nil
}

func (cm *ClusterManager)AllocJobResources(allocation *objects.JobAllocation)(error){
	partitionManager, err := cm.GetPartitionManager(allocation.JobID)
	if err!=nil{
		return  err
	}
	for _, taskalloc := range allocation.GetTaskAllocations(){
		err := partitionManager.AllocTaskResources(taskalloc)
		if err!=nil{
			return err
		}
	}
	return nil
}

func (cm *ClusterManager)FreeJobResources(allocation *objects.JobAllocation)(error){
	partitionManager, err := cm.GetPartitionManager(allocation.JobID)
	if err!=nil{
		return  err
	}
	for _, taskalloc := range allocation.GetTaskAllocations(){
		err := partitionManager.FreeTaskResources(taskalloc)
		if err!=nil{
			return err
		}
	}
	return nil
}