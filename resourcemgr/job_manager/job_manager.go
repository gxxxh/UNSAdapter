package job_manager

import (
	"UNSAdapter/pb_gen/objects"
	"fmt"
	"github.com/golang/protobuf/ptypes/wrappers"
	"time"

	"sync"
)

//todo lock
type JobsManager struct {
	// info for updateAllocations Event
	updateAllocationsmu           *sync.RWMutex
	jobID2Allocations             map[string]*objects.JobAllocation      //添加：接收到调度
	jobID2TasksManager            map[string]*TasksManager               //添加：接收到调度
	jobID2ExecutionHistoryManager map[string]*JobExecutionHistoryManager //添加：接收到调度
	//info for RMUpdateJobs Event
	updateJobsMu   *sync.Mutex
	jobID2Job      map[string]*objects.Job //添加：新任务到来，删除：丢弃或者运行完成
	newJobIDs      []string
	finishedJobIDs []string
}

func NewJobManager() *JobsManager {
	//todo
	return nil
}

func (jm *JobsManager) GetJobExecutionHistoryManager(jobID string) (*JobExecutionHistoryManager, error) {
	jm.updateAllocationsmu.RLock()
	defer jm.updateAllocationsmu.RUnlock()
	jobeh, ok := jm.jobID2ExecutionHistoryManager[jobID]
	if !ok {
		errorMsg := fmt.Sprintf("no jobExeuctionHistoryManager for %s \n", jobID)
		return nil, fmt.Errorf(errorMsg)
	}
	return jobeh, nil
}
func (jm *JobsManager) GetJobAllocation(jobID string) (*objects.JobAllocation, error) {
	jm.updateAllocationsmu.RLock()
	defer jm.updateAllocationsmu.RUnlock()
	jobAllocation, ok := jm.jobID2Allocations[jobID]
	if !ok {
		errorMsg := fmt.Sprintf("no job allocation for %s \n", jobID)
		return nil, fmt.Errorf(errorMsg)
	}
	return jobAllocation, nil
}

func (jm *JobsManager) GetTasksManager(jobID string) (*TasksManager, error) {
	jm.updateAllocationsmu.RLock()
	defer jm.updateAllocationsmu.RUnlock()
	tasksManager, ok := jm.jobID2TasksManager[jobID]
	if !ok {
		errorMsg := fmt.Sprintf("no TasksManager for job %s\n", jobID)
		return nil, fmt.Errorf(errorMsg)
	}
	return tasksManager, nil

}

// if there is job allocatioon, it means the job has already started running
func (jm *JobsManager) CheckJobRunning(jobID string) bool {
	jm.updateAllocationsmu.RLock()
	defer jm.updateAllocationsmu.RUnlock()
	_, ok := jm.jobID2Allocations[jobID]
	return ok
}

func (jm *JobsManager) HandleNewStartTask(jobID string, taskID string, now time.Time) error {
	tasksManager, err := jm.GetTasksManager(jobID)
	if err != nil {
		return err
	}
	taskAl, err := tasksManager.GetTaskAllocation(taskID)
	if err != nil {
		return err
	}
	taskehm, err := jm.GetJobExecutionHistoryManager(jobID)
	if err != nil {
		return err //no job execution history manager
	}
	// update time
	jm.updateAllocationsmu.Lock()
	defer jm.updateAllocationsmu.Unlock()
	taskAl.StartExecutionTimeNanoSecond = &wrappers.Int64Value{
		Value: int64(now.Nanosecond()),
	}
	err = taskehm.SetStartExecutionTimeNanoSecond(taskID, now)
	return err
}
func (jm *JobsManager) HandleFinishedTask(annotations map[string]string) error {
	jm.updateAllocationsmu.Lock()
	jm.updateAllocationsmu.Unlock()
	tasksMg, err := jm.GetTasksManager(annotations["jobID"])
	if err != nil {
		return err
	}
	ehMg, err := jm.GetJobExecutionHistoryManager(annotations["jobID"])
	if err != nil {
		return err
	}
	jm.updateAllocationsmu.Lock()
	defer jm.updateAllocationsmu.Unlock()
	tasksMg.Update(annotations["taskID"])
	err = ehMg.SetDurationNanoSecond(annotations["tasksID"], time.Now())
	if err != nil {
		return err
	}
	// todo judge if the job finished
	if tasksMg.IsFinished() {
		jm.DeleteJob(annotations["jobID"])
	}

	return nil
}

func (jm *JobsManager) AddNewJobAllocation(allocation *objects.JobAllocation) {
	jm.updateAllocationsmu.Lock()
	defer jm.updateAllocationsmu.Unlock()
	jm.jobID2Allocations[allocation.JobID] = allocation
	jm.jobID2TasksManager[allocation.JobID] = NewTasksManager(allocation)
	//job execution history manager
	jm.jobID2ExecutionHistoryManager[allocation.JobID] = NewExecutionHistoryManager(allocation.JobID)
	jm.jobID2ExecutionHistoryManager[allocation.JobID].BuildJobExecutionHistory(allocation)
}

//add new submited job
func (jm *JobsManager) AddJob(newJob *objects.Job) {
	jm.updateJobsMu.Lock()
	defer jm.updateJobsMu.Unlock()
	jm.jobID2Job[newJob.JobID] = newJob
	jm.newJobIDs = append(jm.newJobIDs, newJob.JobID)
	//todo check every 1s? to send RMUpdateJobsEvent

}

//delete finish job,
func (jm *JobsManager) DeleteJob(jobID string) {
	jm.updateJobsMu.Lock()
	defer jm.updateJobsMu.Unlock()
	jm.finishedJobIDs = append(jm.finishedJobIDs, jobID)
	//todo check every 1? to send RMUpdateAllocationsEvent
}
