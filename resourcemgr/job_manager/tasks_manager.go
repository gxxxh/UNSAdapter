package job_manager

import (
	"UNSAdapter/pb_gen/objects"
	"fmt"
	"sync"
)

// using to check if task if finished
type TasksManager struct{
	mu 	*sync.RWMutex
	jobID string
	taskID2TaskAllocation map[string]*objects.TaskAllocation
	finished map[string]bool
	finishCnt int32 // finished job nums
}

func NewTasksManager(allocation *objects.JobAllocation)*TasksManager{
	tasksMg:=  &TasksManager{
		mu:        &sync.RWMutex{},
		jobID:     allocation.JobID,
		finished:  make(map[string]bool),
		taskID2TaskAllocation: make(map[string]*objects.TaskAllocation),
		finishCnt: 0,
	}
	for _, taskAllocation := range(allocation.GetTaskAllocations()){
		tasksMg.finished[taskAllocation.GetTaskID()] = false
		tasksMg.taskID2TaskAllocation[taskAllocation.GetTaskID()] = taskAllocation
	}
	return tasksMg
}

func (tm *TasksManager)GetTaskAllocation(taskID string)(*objects.TaskAllocation, error){
	tm.mu.RLock()
	tm.mu.RUnlock()
	taskAllocation, ok := tm.taskID2TaskAllocation[taskID]
	if !ok{
		errorMsg := fmt.Sprintf("no task allocation for taskID %s in Job %s\n", taskID, tm.jobID)
		return nil, fmt.Errorf(errorMsg)
	}
	return taskAllocation, nil

}

func (tm *TasksManager)Update(taskID string){
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.finished[taskID] = true
	tm.finishCnt ++
}

func (tm *TasksManager)IsFinished()(bool){
	tm.mu.RLock()
	defer tm.mu.Unlock()
	return tm.finishCnt >= int32(len(tm.finished))
}
