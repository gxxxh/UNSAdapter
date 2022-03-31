package cluster_manager

import (
	"UNSAdapter/pb_gen/objects"
	"fmt"
	"sync"
)

type CpuSocketManager struct {
	nodeID                    string
	PartitionID               string
	clusterID                 string
	cpuSocketID2CPUSocketInfo map[string]*objects.CPUSocket
	mu                        sync.RWMutex
	cpuSocketUsing            map[string]bool
}

func NewCpuSocketManager() *CpuSocketManager {
	return nil //todo
}

func (m *CpuSocketManager) GetCpuSocket(cpuSocketID string) (*objects.CPUSocket, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cpuSocket, ok := m.cpuSocketID2CPUSocketInfo[cpuSocketID]
	if !ok {
		errorMsg := fmt.Sprintf("no CPUSocket %s, ,cluster: %s,Partition: %s, node: %s\n",
			cpuSocketID, m.clusterID, m.PartitionID, m.nodeID)
		return nil, fmt.Errorf(errorMsg)
	}
	return cpuSocket, nil
}

func (m *CpuSocketManager) CheckCpuSocketUsing(cpuSocketID string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, err := m.GetCpuSocket(cpuSocketID); err != nil {
		return false, err
	}
	return m.cpuSocketUsing[cpuSocketID], nil
}

func (m *CpuSocketManager) AllocCpuSocket(cpuSocketID string) error {
	if _, err := m.GetCpuSocket(cpuSocketID); err != nil {
		return err
	}
	if m.cpuSocketUsing[cpuSocketID] == false {
		errorMsg := fmt.Sprintf("CPUSocket %s is using. ,cluster: %s,Partition: %s, node: %s\n",
			cpuSocketID, m.clusterID, m.PartitionID, m.nodeID)
		return fmt.Errorf(errorMsg)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cpuSocketUsing[cpuSocketID] = true
	return nil
}
func (m *CpuSocketManager) FreeCpuSocket(cpuSocketID string) error {
	if _, err := m.GetCpuSocket(cpuSocketID); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cpuSocketUsing[cpuSocketID] = false
	return nil
}
