package job_manager

import (
	"UNSAdapter/pb_gen/objects"
	"fmt"
	"strconv"
	"sync"
	"time"
)

//todo, 一个job对应一个executionHistoryManager, 每个executionHistory并行。
type JobExecutionHistoryManager struct {
	mu                          *sync.RWMutex
	jobID                       string
	jobExecutionHistory         *objects.JobExecutionHistory
	taskID2TaskExecutionHistory map[string]*objects.TaskExecutionHistory
}

func NewExecutionHistoryManager(jobID string) *JobExecutionHistoryManager {
	return &JobExecutionHistoryManager{
		mu:                          &sync.RWMutex{},
		jobID:                       jobID,
		jobExecutionHistory: nil,
		taskID2TaskExecutionHistory: make(map[string]*objects.TaskExecutionHistory),
	}
}

func (m *JobExecutionHistoryManager) BuildJobExecutionHistory(jobAllocation *objects.JobAllocation){
	m.mu.Lock()
	defer m.mu.Unlock()
	taskExecutionHistories := make([]*objects.TaskExecutionHistory, 0, len(jobAllocation.TaskAllocations))
	for i, taskAllocation := range jobAllocation.GetTaskAllocations() {
		th := &objects.TaskExecutionHistory{
			ExecutionID:                  strconv.Itoa(i), //todo
			NodeID:                       taskAllocation.GetNodeID(),
			JobID:                        taskAllocation.GetJobID(),
			TaskID:                       taskAllocation.GetTaskID(),
			//StartExecutionTimeNanoSecond: taskAllocation.GetStartExecutionTimeNanoSecond().GetValue(),
			StartExecutionTimeNanoSecond: 0,
			DurationNanoSecond:           0,
			HostMemoryAllocation:         taskAllocation.GetHostMemoryAllocation(),
			CPUSocketAllocations:         taskAllocation.GetCPUSocketAllocations(),
			AcceleratorAllocation:        taskAllocation.GetAcceleratorAllocation(),
		}
		m.taskID2TaskExecutionHistory[taskAllocation.GetTaskID()] = th
		taskExecutionHistories = append(taskExecutionHistories, th)
	}
	m.jobExecutionHistory = &objects.JobExecutionHistory{
		JobID:                  jobAllocation.GetJobID(),
		ResourceManagerID:      jobAllocation.GetResourceManagerID(),
		PartitionID:            jobAllocation.GetPartitionID(),
		TaskExecutionHistories: taskExecutionHistories,
	}
}
func (m *JobExecutionHistoryManager)GetJobExecutionHistory()(*objects.JobExecutionHistory){
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.jobExecutionHistory
}

func (m *JobExecutionHistoryManager) GetTaskExecutionHistory(taskID string) (*objects.TaskExecutionHistory, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	taskeh, ok := m.taskID2TaskExecutionHistory[taskID]
	if !ok {
		errorMsg := fmt.Sprintf("no exeuction history for task %s in job %s\n", taskID, m.jobID)
		return nil, fmt.Errorf(errorMsg)
	}
	return taskeh, nil
}

func (m *JobExecutionHistoryManager) SetStartExecutionTimeNanoSecond(taskID string, startTime time.Time) error {

	taskeh, err := m.GetTaskExecutionHistory(taskID)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Printf("update task %s executionHistory startTime to %v\n",taskID,startTime)
	taskeh.StartExecutionTimeNanoSecond = startTime.UnixNano()
	return nil
}

func (m *JobExecutionHistoryManager) SetDurationNanoSecond(taskID string, finishTime int64) error {
	taskeh, err := m.GetTaskExecutionHistory(taskID)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	taskeh.DurationNanoSecond = finishTime - taskeh.StartExecutionTimeNanoSecond
	return nil
}
