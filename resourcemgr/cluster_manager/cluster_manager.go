package cluster_manager

import (
	"github.com/MLSched/UNS/pb_gen/objects"
	"fmt"
)

//todo 管理资源的占用
type ClusterManager struct {
	clusterID string
	//mu sync.RWMutex
	clusterInfo                  *objects.Cluster
	partitionID2PaititionManager map[string]*PartitionManager
	//for predictor
	acceleratorID2Accelerator map[string]*objects.Accelerator
}

//type AccleratorManager struct{
//
//}

func NewClusterManager(clusterID string, clusterInfo *objects.Cluster) *ClusterManager {
	fmt.Println("Loading cluster info to cluster manager")
	partitionID2Mgr := make(map[string]*PartitionManager)
	for _, partitionInfo := range clusterInfo.GetPartitions() {
		partitionID2Mgr[partitionInfo.GetPartitionID()] = NewPartitionManager(clusterID, partitionInfo)
	}
	cm := &ClusterManager{
		clusterID: clusterID,
		//mu:                           sync.RWMutex{},
		clusterInfo:                  clusterInfo,
		partitionID2PaititionManager: partitionID2Mgr,
	}
	//add for predictor
	acceleratorID2Accelerator := make(map[string]*objects.Accelerator)
	for _, partition := range clusterInfo.GetPartitions() {
		for _, node := range partition.GetNodes() {
			for _, cpuSocket := range node.GetCPUSockets() {
				for _, acc := range cpuSocket.GetAccelerators() {
					acceleratorID2Accelerator[acc.GetAcceleratorID()] = acc
				}
			}
		}
	}
	cm.acceleratorID2Accelerator = acceleratorID2Accelerator
	return cm
}

//只有一个partition
func (cm *ClusterManager) GetPratitionID() string {
	for ID, _ := range cm.partitionID2PaititionManager {
		return ID
	}
	return ""
}

func (cm *ClusterManager) GetClusterInfo() *objects.Cluster {
	return cm.clusterInfo
}
func (cm *ClusterManager) GetPartitionManager(partitionID string) (*PartitionManager, error) {
	//cm.mu.RLock()
	//defer cm.mu.RUnlock()
	partitionManager, ok := cm.partitionID2PaititionManager[partitionID]
	if !ok {
		errorMsg := fmt.Sprintf("no paritionManager for Partition %s in cluster %s", partitionID, cm.clusterID)
		return nil, fmt.Errorf(errorMsg)
	}
	return partitionManager, nil
}

func (cm *ClusterManager) CheckJobResources(allocation *objects.JobAllocation) (bool, error) {
	partitionManager, err := cm.GetPartitionManager(allocation.GetPartitionID())
	if err != nil {
		return false, err
	}
	for _, taskalloc := range allocation.TaskAllocations {
		//一个task对应一个node上的多个CPUSocket和1个accelerator
		res, err := partitionManager.CheckTaskResources(taskalloc)
		if err != nil || !res {
			return false, err
		}
	}
	return true, nil
}

//所有task都可以执行后才获取资源执行
func (cm *ClusterManager) AllocJobResources(allocation *objects.JobAllocation) error {
	fmt.Printf("Alloc job resource for job %s, Accelerator ID: %s\n", allocation.GetJobID(),allocation.GetTaskAllocations()[0].AcceleratorAllocation.AcceleratorID)
	partitionManager, err := cm.GetPartitionManager(allocation.GetPartitionID())
	if err != nil {
		return err
	}
	for _, taskalloc := range allocation.GetTaskAllocations() {
		err := partitionManager.AllocTaskResources(taskalloc)
		if err != nil {
			return err
		}
	}
	return nil
}

//释放资源应该是只释放一个任务的资源
func (cm *ClusterManager) FreeTaskResources(taskalloc *objects.TaskAllocation, partitionID string) error {
	fmt.Printf("Free task resource for task %s in job %s,AcceleratorID is %s\n", taskalloc.GetTaskID(),taskalloc.GetJobID(),taskalloc.AcceleratorAllocation.GetAcceleratorID())
	partitionManager, err := cm.GetPartitionManager(partitionID)
	if err != nil {
		return err
	}
	err = partitionManager.FreeTaskResources(taskalloc)
	return err
}

func (cm *ClusterManager) FreeJobResources(allocation *objects.JobAllocation) error {
	fmt.Printf("free job resource for job %s\n", allocation.GetJobID())
	partitionManager, err := cm.GetPartitionManager(allocation.GetPartitionID())
	if err != nil {
		return err
	}
	for _, taskalloc := range allocation.GetTaskAllocations() {
		err := partitionManager.FreeTaskResources(taskalloc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cm *ClusterManager) GetAccelerator(acceleratorID string) *objects.Accelerator {
	return cm.acceleratorID2Accelerator[acceleratorID]
}
func (cm *ClusterManager)GetAllAccelerators()map[string]string{
	accelerators := make(map[string]string)
	for ID, acc := range cm.acceleratorID2Accelerator{
		accelerators[ID] = acc.GetAcceleratorMetaInfo().GetBriefType()
	}
	return accelerators
}

func (cm *ClusterManager) GetAccelerators(acceleratorIDs []string) []*objects.Accelerator {
	accelerators := make([]*objects.Accelerator, 0, len(acceleratorIDs))
	for _, ID := range acceleratorIDs {
		accelerators = append(accelerators, cm.GetAccelerator(ID))
	}
	return accelerators
}

func (cm *ClusterManager) GetNode(partitionID string, nodeID string) *objects.Node {
	partitionManager, _ := cm.GetPartitionManager(partitionID)
	nm, _ := partitionManager.GetNodeManager(nodeID)
	return nm.nodeInfo
}
