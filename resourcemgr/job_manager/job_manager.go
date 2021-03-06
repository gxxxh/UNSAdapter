package job_manager

import (
	"fmt"
	"github.com/MLSched/UNS/pb_gen/objects"
	"log"
	"strconv"
	"time"

	"sync"
)

//todo lock
type JobsManager struct {
	// info for updateAllocations Event
	updateAllocationsMu sync.RWMutex

	jobID2Allocations             map[string]*objects.JobAllocation      //添加：接收到调度
	jobID2TasksManager            map[string]*TasksManager               //添加：接收到调度
	jobID2ExecutionHistoryManager map[string]*JobExecutionHistoryManager //添加：接收到调度
	//pod结束时，处理线程有可能调用该结构finished job id
	finishedJobIDsMu sync.RWMutex
	finishedJobIDs   []string
	// 更新了启动时间的placeholder的任务
	newStartedJobIDsMu sync.RWMutex
	newStartedJobIDs   []string //placeholder任务，需要等待才能开始运行
	//启动job时才修改该结构
	newAllocationIDsMu sync.RWMutex
	newAllocationIDs   []string //新获取的allocation,还没开始执行
	//info for RMUpdateJobs Event
	updateJobsMu sync.RWMutex
	jobID2Job    map[string]*objects.Job //添加：新任务到来，删除：丢弃或者运行完成
	newJobIDs    []string
	jobNums      int
}

func NewJobManager() *JobsManager {
	return &JobsManager{
		updateAllocationsMu:           sync.RWMutex{},
		jobNums:                       0,
		jobID2Allocations:             make(map[string]*objects.JobAllocation, 0),
		jobID2TasksManager:            make(map[string]*TasksManager),
		jobID2ExecutionHistoryManager: make(map[string]*JobExecutionHistoryManager),
		finishedJobIDsMu:              sync.RWMutex{},
		finishedJobIDs:                make([]string, 0),
		newStartedJobIDsMu:            sync.RWMutex{},
		newStartedJobIDs:              make([]string, 0),
		newAllocationIDsMu:            sync.RWMutex{},
		newAllocationIDs:              make([]string, 0),
		updateJobsMu:                  sync.RWMutex{},
		jobID2Job:                     make(map[string]*objects.Job),
		newJobIDs:                     make([]string, 0),
	}
}

//func (jm *JobsManager) GetFinishedJobIDs() []string {
//	jm.updateAllocationsMu.RLock()
//	defer jm.updateAllocationsMu.RUnlock()
//	return jm.finishedJobIDs
//}
func (jm *JobsManager)GetJobNums()int{
	jm.updateJobsMu.Lock()
	defer jm.updateJobsMu.Unlock()
	return jm.jobNums
}
func (jm *JobsManager) GetJobByJobID(jobID string) (*objects.Job, error) {
	jm.updateJobsMu.RLock()
	defer jm.updateJobsMu.RUnlock()
	job, ok := jm.jobID2Job[jobID]
	if !ok {
		errMsg := fmt.Sprintf("no job for ID %s", jobID)
		return nil, fmt.Errorf(errMsg)
	}
	return job, nil
}

func (jm *JobsManager) GetNewStartedJobIDs() []string {
	jm.newStartedJobIDsMu.Lock()
	defer jm.newStartedJobIDsMu.Unlock()
	res := jm.newStartedJobIDs
	jm.newStartedJobIDs = make([]string, 0, len(res))
	return res
}

func (jm *JobsManager) AddNewStartedJobIDs(jobID string) {
	jm.newStartedJobIDsMu.Lock()
	defer jm.newStartedJobIDsMu.Unlock()
	jm.newStartedJobIDs = append(jm.newStartedJobIDs, jobID)
}

func (jm *JobsManager) GetNewAllocationIDs() []string {
	jm.newAllocationIDsMu.RLock()
	defer jm.newAllocationIDsMu.RUnlock()
	res := jm.newAllocationIDs
	return res
}

//任务运行后删除id
func (jm *JobsManager) DeleteNewAllocationID(jobID string) {
	jm.newAllocationIDsMu.Lock()
	defer jm.newAllocationIDsMu.Unlock()
	newAllocationIDs := make([]string, 0, len(jm.newAllocationIDs))
	for _, ID := range jm.newAllocationIDs {
		if ID != jobID {
			newAllocationIDs = append(newAllocationIDs, ID)
		}
	}
	jm.newAllocationIDs = newAllocationIDs
}

func (jm *JobsManager) GetNewAllocationNums() int {
	jm.newAllocationIDsMu.Lock()
	defer jm.newAllocationIDsMu.Unlock()
	return len(jm.newAllocationIDs)
}
func (jm *JobsManager) GetJobExecutionHistoryManager(jobID string) (*JobExecutionHistoryManager, error) {
	jm.updateAllocationsMu.RLock()
	defer jm.updateAllocationsMu.RUnlock()
	jobeh, ok := jm.jobID2ExecutionHistoryManager[jobID]
	if !ok {
		errorMsg := fmt.Sprintf("no jobExeuctionHistoryManager for %s \n", jobID)
		return nil, fmt.Errorf(errorMsg)
	}
	return jobeh, nil
}
func (jm *JobsManager) GetJobAllocation(jobID string) (*objects.JobAllocation, error) {
	jm.updateAllocationsMu.RLock()
	defer jm.updateAllocationsMu.RUnlock()
	jobAllocation, ok := jm.jobID2Allocations[jobID]
	if !ok {
		errorMsg := fmt.Sprintf("no job allocation for %s \n", jobID)
		return nil, fmt.Errorf(errorMsg)
	}
	return jobAllocation, nil
}

func (jm *JobsManager) GetTaskAllocation(jobID string, taskID string) (*objects.TaskAllocation, error) {
	tasksManager, err := jm.GetTasksManager(jobID)
	if err != nil {
		return nil, err
	}
	return tasksManager.GetTaskAllocation(taskID)
}
func (jm *JobsManager) GetTasksManager(jobID string) (*TasksManager, error) {
	jm.updateAllocationsMu.RLock()
	defer jm.updateAllocationsMu.RUnlock()
	tasksManager, ok := jm.jobID2TasksManager[jobID]
	if !ok {
		errorMsg := fmt.Sprintf("no TasksManager for job %s\n", jobID)
		return nil, fmt.Errorf(errorMsg)
	}
	return tasksManager, nil

}

func (jm *JobsManager) GetFinishedJobInfo() (bool, []string, []*objects.JobExecutionHistory) {
	//返回false意味着并不是所有任务都完成
	jm.finishedJobIDsMu.Lock()
	defer jm.finishedJobIDsMu.Unlock()
	if len(jm.finishedJobIDs) < len(jm.jobID2Job) {
		log.Printf("current finished job %d, id is  %v\n",len(jm.finishedJobIDs), jm.finishedJobIDs)
		return false, nil, nil
	}
	finishedJobIDs := jm.finishedJobIDs
	jobExecutionHistories := make([]*objects.JobExecutionHistory, 0, len(finishedJobIDs))
	for _, id := range jm.finishedJobIDs {
		jeh, err := jm.GetJobExecutionHistoryManager(id)
		if err != nil {
			log.Println("GetJobExecutionHistoryManager error, err=%v", err)
			return false, nil, nil
		}
		jobExecutionHistories = append(jobExecutionHistories, jeh.GetJobExecutionHistory())
	}
	jm.finishedJobIDs = make([]string, 0, len(finishedJobIDs))
	return true, jm.finishedJobIDs, jobExecutionHistories
}

// if there is job allocatioon, it means the job has already started running
func (jm *JobsManager) CheckJobRunning(jobID string) bool {
	jm.updateAllocationsMu.RLock()
	defer jm.updateAllocationsMu.RUnlock()
	_, ok := jm.jobID2Allocations[jobID]
	return ok
}

func (jm *JobsManager) UpdateJobStartExecutionTime(jobID string, now time.Time) error {
	tasksManager, err := jm.GetTasksManager(jobID)
	if err != nil {
		return err
	}
	tasksExecutionHistoryManager, err := jm.GetJobExecutionHistoryManager(jobID)
	if err != nil {
		return err
	}
	jobIDs := tasksManager.GetTasksIDs()
	for _, taskID := range jobIDs {
		err = tasksManager.SetTaskAllocationStartExecutionTime(taskID, now)
		if err != nil {
			return err
		}
		err = tasksExecutionHistoryManager.SetStartExecutionTimeNanoSecond(taskID, now)
		if err != nil {
			return err
		}
	}
	return nil
}

func (jm *JobsManager) HandleFinishedTask(annotations map[string]string) error {
	tasksMg, err := jm.GetTasksManager(annotations["jobID"])
	if err != nil {
		return err
	}
	ehMg, err := jm.GetJobExecutionHistoryManager(annotations["jobID"])
	if err != nil {
		return err
	}

	tasksMg.Update(annotations["taskID"])
	finishTime, err := strconv.ParseInt(annotations["finishTime"], 10, 64)
	if err != nil {
		return err
	}
	err = ehMg.SetDurationNanoSecond(annotations["taskID"], finishTime)
	if err != nil {
		return err
	}
	jm.finishedJobIDsMu.Lock()
	defer jm.finishedJobIDsMu.Unlock()
	if tasksMg.IsFinished() {
		log.Printf("Job %s finished\n", annotations["jobID"])
		jm.finishedJobIDs = append(jm.finishedJobIDs, annotations["jobID"])
	}
	return nil
}

func (jm *JobsManager) AddJobAllocation(allocation *objects.JobAllocation) {
	fmt.Printf("add allocation to job manager for job %s, acceleratorID:%s　\n", allocation.JobID, allocation.GetTaskAllocations()[0].GetAcceleratorAllocation().GetAcceleratorID())
	//接受一个allocation，一旦接受就意味着开始执行
	jm.updateAllocationsMu.Lock()
	jm.jobID2Allocations[allocation.JobID] = allocation
	jm.jobID2TasksManager[allocation.JobID] = NewTasksManager(allocation)
	//job execution history manager
	jm.jobID2ExecutionHistoryManager[allocation.JobID] = NewExecutionHistoryManager(allocation.JobID)
	jm.jobID2ExecutionHistoryManager[allocation.JobID].BuildJobExecutionHistory(allocation)

	jm.updateAllocationsMu.Unlock()

	jm.newAllocationIDsMu.Lock()
	jm.newAllocationIDs = append(jm.newAllocationIDs, allocation.JobID)
	jm.newAllocationIDsMu.Unlock()

}

//delete finish job,
func (jm *JobsManager) HandleRMUpdateAllocationEvent(finishedIDs []string) {
	jm.updateAllocationsMu.Lock()
	for _, jobID := range finishedIDs {
		//delete execution history
		delete(jm.jobID2ExecutionHistoryManager, jobID)
		//delete allocation
		delete(jm.jobID2Allocations, jobID)
		delete(jm.jobID2TasksManager, jobID)
		//delete job
		//delete(jm.jobID2Job, jobID)
	}
	jm.updateAllocationsMu.Unlock()
	//delete job info
	jm.updateJobsMu.Lock()
	for _, jobID := range finishedIDs {
		delete(jm.jobID2Job, jobID)
	}
	jm.updateJobsMu.Unlock()
}

//add new submited job
func (jm *JobsManager) AddJob(newJobs []*objects.Job) {
	jm.updateJobsMu.Lock()
	defer jm.updateJobsMu.Unlock()
	for _, newJob := range newJobs {
		jm.jobID2Job[newJob.JobID] = newJob
		jm.jobNums = jm.jobNums + 1
		jm.newJobIDs = append(jm.newJobIDs, newJob.JobID)
	}
}
