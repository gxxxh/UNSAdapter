package cluster_manager

import (
	"UNSAdapter/pb_gen/objects"
	"fmt"
	"sync"
)

//管理一个node的多个accelerator
type AcceleratorManager struct{
	nodeID string
	PartitionID string
	clusterID string
	AcceleratorID2AcceleratorInfo map[string]*objects.Accelerator
	mu sync.RWMutex
	acceleratorUsing map[string]bool
}

func NewAcceleratorManager()(*AcceleratorManager){
	return nil//todo
}

func (m *AcceleratorManager)GetAccelerator(acceleratorID string)(*objects.Accelerator, error){
	m.mu.RLock()
	m.mu.RUnlock()
	accelerator, ok := m.AcceleratorID2AcceleratorInfo[acceleratorID]
	if !ok{
		errorMsg := fmt.Sprintf("no Accelerator %s, ,cluster: %s,Partition: %s, node: %s\n",
			acceleratorID, m.clusterID,m.PartitionID, m.nodeID)
		return nil,  fmt.Errorf(errorMsg)
	}
	return accelerator, nil
}

func (m *AcceleratorManager)CheckAcceleratorUsing(acceleratorID string)(bool,error){
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, err := m.GetAccelerator(acceleratorID); err!=nil{
		return false, err
	}
	return m.acceleratorUsing[acceleratorID], nil
}

func (m *AcceleratorManager)AllocAccelerator(acceleratorID string)(error){

	if _, err := m.GetAccelerator(acceleratorID); err!=nil{
		return err
	}
	if m.acceleratorUsing[acceleratorID]==false{
		errorMsg := fmt.Sprintf("Accelerator %s is using. ,cluster: %s,Partition: %s, node: %s\n",
			acceleratorID, m.clusterID,m.PartitionID, m.nodeID)
		return fmt.Errorf(errorMsg)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acceleratorUsing[acceleratorID] = true
	return nil
}

func (m *AcceleratorManager)FreeAccelerator(acceleratorID string)(error){

	if _, err := m.GetAccelerator(acceleratorID);err!=nil{
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acceleratorUsing[acceleratorID] = false
	return nil
}