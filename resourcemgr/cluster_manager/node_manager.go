package cluster_manager

import (
	"UNSAdapter/pb_gen/objects"
	"sync"
)
//管理一个Node
type NodeManager struct{
	clusterID string
	partitionID string
	nodeInfo                         *objects.Node
	mu sync.RWMutex
	cpuSocketManager *CpuSocketManager
	acceleratorManager *AcceleratorManager
}

func NewNodeManager(clusterID string, partitionID string, nodeInfo *objects.Node)(*NodeManager){
	accelerators := make([]*objects.Accelerator,0)
	for _, cpuSocketInfo := range nodeInfo.GetCPUSockets(){
		acceleratorsMap := cpuSocketInfo.GetAccelerators()
		for acceleratorID := range acceleratorsMap{
			accelerators = append(accelerators, acceleratorsMap[acceleratorID])
		}
	}
	return &NodeManager{
		clusterID:          clusterID,
		partitionID:        partitionID,
		nodeInfo:           nodeInfo,
		mu:                 sync.RWMutex{},
		cpuSocketManager:   NewCpuSocketManager(nodeInfo.GetNodeID(), partitionID, clusterID,nodeInfo.GetCPUSockets()),
		acceleratorManager: NewAcceleratorManager(nodeInfo.GetNodeID(),partitionID, clusterID, accelerators ),
	}
}

//
//func (m *NodeManager) GetCPUSocketManager(cpuSocketID string)(*CpuSocketManager, error){
//	m.mu.RLock()
//	defer m.mu.RUnlock()
//	cpuSocketManager, ok := m.CPUSocketID2CPUSocketManager[cpuSocketID]
//	if !ok{
//		errorMsg := fmt.Sprintf("no CPUSocket Manager for cpuSocket %s, ,cluster: %s,Partition: %s, node: %s\n",
//			cpuSocketID, m.clusterID,m.partitionID, m.nodeInfo.NodeID)
//		return nil,  fmt.Errorf(errorMsg)
//	}
//	return cpuSocketManager,nil
//}
//func (m *NodeManager)GetAcceleratorManager(acceleratorID string)(*AcceleratorManager, error){
//	m.mu.RLock()
//	defer m.mu.RUnlock()
//	accelerator, ok := m.AcceleratorID2AcceleratorManager[acceleratorID]
//	if !ok{
//		errorMsg := fmt.Sprintf("no Accelerator Manager for accelerator %s, ,cluster: %s,Partition: %s, node: %s\n",
//			acceleratorID, m.clusterID,m.partitionID, m.nodeInfo.NodeID)
//		return nil,  fmt.Errorf(errorMsg)
//	}
//	return accelerator,nil
//}

func (m *NodeManager) CheckTaskResources(allocation *objects.TaskAllocation) (bool, error){
	res, err := m.CheckAcceleratorUsing(allocation.AcceleratorAllocation.AcceleratorID)
	if err!=nil||!res{
		return false, err
	}
	//for _, cpuSocketAllocation := range(allocation.CPUSocketAllocations){
	//	res, err := m.CheckCpuSocketUsing(cpuSocketAllocation.CPUSocketID)
	//	if err!=nil||!res {
	//		return false, err
	//	}
	//}
	return true, nil
}


func (m *NodeManager)CheckAcceleratorUsing(acceleratorID string)(bool, error){
	//acceleratorMg, err := m.GetAcceleratorManager(acceleratorID)
	//if err!=nil{
	//	return false, err
	//}
	return m.acceleratorManager.CheckAcceleratorUsing(acceleratorID)
}

//func (m *NodeManager)CheckCpuSocketUsing(cpuSocketID string)(bool, error){
//	cpuSocketMg, err := m.GetCPUSocketManager(cpuSocketID)
//	if err!=nil{
//		return false, err
//	}
//	return cpuSocketMg.CheckCpuSocketUsing(cpuSocketID)
//}


func (m *NodeManager)AllocTaskResources(allocation *objects.TaskAllocation)(error){
	//res, err := m.GetAcceleratorManager(allocation.AcceleratorAllocation.AcceleratorID)
	//if err!=nil{
	//	return err
	//}

	err :=m.acceleratorManager.AllocAccelerator(allocation.AcceleratorAllocation.AcceleratorID)
	if err!=nil{
		return err
	}
	//for _, cpuAllocation := range allocation.CPUSocketAllocations{
	//	cpuManager, err:= m.GetCPUSocketManager(cpuAllocation.CPUSocketID)
	//	if err!=nil{
	//		return err
	//	}
	//	err = cpuManager.AllocCpuSocket(cpuAllocation.CPUSocketID)
	//	if err!=nil{
	//		return err
	//	}
	//}
	return nil
	//todo 不成功释放已经获取的资源
}
func (m *NodeManager)FreeTaskResources(allocation *objects.TaskAllocation)(error){
	//res, err := m.GetAcceleratorManager(allocation.AcceleratorAllocation.AcceleratorID)
	//if err!=nil{
	//	return err
	//}
	err:= m.acceleratorManager.FreeAccelerator(allocation.AcceleratorAllocation.AcceleratorID)
	if err!=nil{
		return err
	}
	//for _, cpuAllocation := range allocation.CPUSocketAllocations{
	//	cpuManager, err:= m.GetCPUSocketManager(cpuAllocation.CPUSocketID)
	//	if err!=nil{
	//		log.Println("no cpumanager, error = %v", err)
	//		continue
	//	}
	//	err = cpuManager.FreeCpuSocket(cpuAllocation.CPUSocketID)
	//	if err!=nil{
	//		//return err
	//		log.Println("free CpuSocket Error, %v", err)
	//	}
	//}
	return nil
}