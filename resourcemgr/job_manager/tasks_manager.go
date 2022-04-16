package job_manager

import (
	"UNSAdapter/pb_gen/objects"
	"fmt"
	"github.com/golang/protobuf/ptypes/wrappers"
	"sync"
	"time"
)

// using to check if task if finished
//同时管理taskAllocation 的修改
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
	defer tm.mu.RUnlock()
	taskAllocation, ok := tm.taskID2TaskAllocation[taskID]
	if !ok{
		errorMsg := fmt.Sprintf("no task allocation for taskID %s in Job %s\n", taskID, tm.jobID)
		return nil, fmt.Errorf(errorMsg)
	}
	return taskAllocation, nil

}

func (tm *TasksManager)GetTasksIDs()[]string{
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	tasksIDs := make([]string, 0, len(tm.taskID2TaskAllocation))
	for ID := range(tm.taskID2TaskAllocation){
		tasksIDs  = append(tasksIDs, ID)
	}
	return tasksIDs
}

func (tm *TasksManager)SetTaskAllocationStartExecutionTime(taskID string, now time.Time)error{
	taskAl, err := tm.GetTaskAllocation(taskID)
	if err!=nil{
		return err
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	taskAl.StartExecutionTimeNanoSecond = &wrappers.Int64Value{
		Value: int64(now.UnixNano()),
	}
	return nil
}
func (tm *TasksManager)Update(taskID string){
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.finished[taskID] = true
	tm.finishCnt ++
}

func (tm *TasksManager)IsFinished()(bool){
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.finishCnt >= int32(len(tm.finished))
}
