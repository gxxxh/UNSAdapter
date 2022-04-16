package cluster_manager

import (
	"UNSAdapter/pb_gen/objects"
	"fmt"
)

//管理一个partition
type PartitionManager struct {
	clusterID    string
	paritionInfo *objects.Partition
	//mu sync.RWMutex
	nodeID2NodeManager map[string]*NodeManager
}

func NewPartitionManager(clusterID string, partitionInfo *objects.Partition) (manager *PartitionManager) {
	nodeID2Mgr := make(map[string]*NodeManager, len(partitionInfo.GetNodes()))
	for _, nodeInfo := range partitionInfo.GetNodes() {
		nodeID2Mgr[nodeInfo.GetNodeID()] = NewNodeManager(clusterID, partitionInfo.GetPartitionID(), nodeInfo)
	}
	return &PartitionManager{
		clusterID:    clusterID,
		paritionInfo: partitionInfo,
		//mu:                 sync.RWMutex{},
		nodeID2NodeManager: nodeID2Mgr,
	}
}

func (m *PartitionManager) GetNodeManager(nodeID string) (*NodeManager, error) {
	//m.mu.RLock()
	//defer m.mu.RLock()
	nodeManager, ok := m.nodeID2NodeManager[nodeID]
	if !ok {
		errorMsg := fmt.Sprintf("no node manager for %s in cluster:%s partiton: %s\n", nodeID, m.clusterID, m.paritionInfo.PartitionID)
		return nil, fmt.Errorf(errorMsg)
	}
	return nodeManager, nil

}
func (m *PartitionManager) CheckTaskResources(allocation *objects.TaskAllocation) (bool, error) {
	nodeManager, err := m.GetNodeManager(allocation.NodeID)
	if err != nil {
		return false, err
	}
	return nodeManager.CheckTaskResources(allocation)
}

func (m *PartitionManager) AllocTaskResources(allocation *objects.TaskAllocation) error {
	nodeManager, err := m.GetNodeManager(allocation.NodeID)
	if err != nil {
		return err
	}
	return nodeManager.AllocTaskResources(allocation)
}
func (m *PartitionManager) FreeTaskResources(allocation *objects.TaskAllocation) error {
	nodeManager, err := m.GetNodeManager(allocation.NodeID)
	if err != nil {
		return err
	}
	return nodeManager.FreeTaskResources(allocation)
}
