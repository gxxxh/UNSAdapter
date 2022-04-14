package local

import (
	"UNSAdapter/events"
	events2 "UNSAdapter/pb_gen/events"
	"UNSAdapter/pb_gen/objects"
	"UNSAdapter/resourcemgr/cluster_manager"
	"UNSAdapter/resourcemgr/job_manager"
	"UNSAdapter/resourcemgr/k8s_manager"
	"UNSAdapter/resourcemgr/simulator"
	"github.com/golang/protobuf/ptypes/wrappers"
	"log"
	"time"
)

var CheckFinishedInterval = 1000 * time.Millisecond
var CheckNotStartedJobInterval = 1000 * time.Millisecond

type ResourceManager struct {
	RMID           string
	clusterManager *cluster_manager.ClusterManager
	//podManager     *k8s_manager.PodManagr
	k8sManager *k8s_manager.K8sManager
	jobManager *job_manager.JobsManager
	jobSimulator *simulator.JobSimulator
}

func NewResourceManager(rmID string, k8sManager *k8s_manager.K8sManager) *ResourceManager {
	return &ResourceManager{
		RMID:           rmID,
		clusterManager: nil,
		k8sManager:     k8sManager,
		jobManager:     job_manager.NewJobManager(),
		jobSimulator:   nil,
	}
}

func (rm *ResourceManager) BuildClusterManager(clusterID string, cluster *objects.Cluster) {
	rm.clusterManager = cluster_manager.NewClusterManager(clusterID, cluster)
}

func (rm *ResourceManager)SetJobSimulator(jobSimulator *simulator.JobSimulator){
	rm.jobSimulator = jobSimulator
}

func (rm *ResourceManager) GetJobManager() (manager *job_manager.JobsManager) {
	return rm.jobManager
}

func (rm *ResourceManager) GetClusterManager() (manager *cluster_manager.ClusterManager) {
	return rm.clusterManager
}

func (rm *ResourceManager) GetResourceManagerID() string {
	return rm.RMID
}


func Push(rmID string, partitionID string, event *events.Event) {

}

//todo job simulator
func (rm *ResourceManager) checkSubmitJobs() {
	for{
		newJobs, isClose := rm.jobSimulator.GetNewJob()
		if !isClose{
			log.Printf("CheckSubmitJobs done!")
			break
		}
		rm.jobManager.AddJob(newJobs)
	}
}

func (rm *ResourceManager) checkFinishedJobs() {

	for {
		time.Sleep(CheckFinishedInterval)
		newStartedJobIDs := rm.jobManager.GetNewStartedJobIDs()
		newlyStartedAllocations := make([]*objects.JobAllocation, 0, len(newStartedJobIDs))
		for _, jobID := range newStartedJobIDs {
			jobAllocation, err := rm.jobManager.GetJobAllocation(jobID)
			if err != nil {
				log.Printf("%v ", err)
				continue
			}
			newlyStartedAllocations = append(newlyStartedAllocations, jobAllocation)
		}
		finishedJobIDs, jobExecutionHistories := rm.jobManager.GetFinishedJobInfo()
		if len(newlyStartedAllocations) == 0 && len(finishedJobIDs) == 0 {
			continue
		}
		ev := &events2.RMUpdateAllocationsEvent{
			UpdatedJobAllocations: newlyStartedAllocations,
			FinishedJobIDs:        finishedJobIDs,
			JobExecutionHistories: jobExecutionHistories,
			CurrentNanoTime: &wrappers.Int64Value{
				Value: int64(time.Now().Nanosecond()),
			},
		}
		//update job info
		rm.jobManager.HandleRMUpdateAllocationEvent(finishedJobIDs)
	}

}

func (rm *ResourceManager)handleSSUpdateAllocation(eo *events2.SSUpdateAllocationsEvent){
	jobAllocations := eo.NewJobAllocations
	for _, jobAllocation := range jobAllocations{
		for _, taskAllocation := range jobAllocation.TaskAllocations{
			taskAllocation.GetStartExecutionTimeNanoSecond()
		}
	}
}
//func (rm *ResourceManager) handleSSUpdateAllocation(eo *events2.SSUpdateAllocationsEvent) {
//	alloctions := eo.NewJobAllocations
//	nonPlaceholders := make([]*objects.JobAllocation, 0, len(alloctions))
//	placeholders := make([]*objects.JobAllocation, 0, len(alloctions))
//	for _, allocation := range alloctions {
//
//		if allocation.GetTaskAllocations()[0].GetPlaceholder() {
//			placeholders = append(placeholders, allocation)
//		} else {
//			nonPlaceholders = append(nonPlaceholders, allocation)
//		}
//	}
//	filteredAllocations := make([]*objects.JobAllocation, 0, len(alloctions))
//	now := time.Now()
//nextNonPlaceholderAlloc:
//	for _, nonPlaceholderAllocation := range nonPlaceholders {
//		//check task is runing
//		if rm.GetJobManager().CheckJobRunning(nonPlaceholderAllocation.GetJobID()) {
//			log.Printf("simulator ignores allocation of jobID = %s since it is already allocated", nonPlaceholderAllocation.GetJobID())
//			continue nextNonPlaceholderAlloc
//		}
//		// check resource is free
//		ok, err := rm.GetClusterManager().CheckJobResources(nonPlaceholderAllocation)
//		if err != nil {
//			log.Printf("%v", err)
//		}
//		if ok {
//			//go rm.StartJob(nonPlaceholderAllocation, now)
//			filteredAllocations = append(filteredAllocations, nonPlaceholderAllocation)
//		} else {
//			log.Printf("resources for job %s's alllocation is not free\n", nonPlaceholderAllocation.GetJobID())
//		}
//	}
//nextPlaceholderAlloc:
//	//todo placeholder尚未起到作用
//	for _, placeholderAllocation := range placeholders {
//		if rm.GetJobManager().CheckJobRunning(placeholderAllocation.GetJobID()) {
//			continue nextPlaceholderAlloc
//		}
//		// check resource
//		ok, err := rm.GetClusterManager().CheckJobResources(placeholderAllocation)
//		if err != nil {
//			log.Printf("")
//		}
//		if ok {
//			//rm.StartJob(placeholderAllocation, time.Now())
//			filteredAllocations = append(filteredAllocations, placeholderAllocation)
//		} else {
//			log.Printf("resources for job %s's alllocation is not free\n", placeholderAllocation.GetJobID())
//		}
//	}
//	if len(filteredAllocations) > 0 {
//		for _, jobAC := range filteredAllocations {
//			for _, taskAC := range jobAC.TaskAllocations {
//				taskAC.StartExecutionTimeNanoSecond = &wrappers.Int64Value{Value: int64(now.Nanosecond())}
//			}
//			rm.jobManager.AddJobAllocation(jobAC)
//		}
//		//push UpdateAllocations Event
//		ev := &events2.RMUpdateAllocationsEvent{UpdatedJobAllocations: filteredAllocations}
//
//	}
//}

//检查未启动的allocations，若可以执行，则开始执行
func (rm *ResourceManager) checkNotStartedJob() {
	for {
		time.Sleep(CheckNotStartedJobInterval)
		now := time.Now()
		notStartedJobIDS := rm.jobManager.GetNewAllocationIDs()
		for _, jobID := range notStartedJobIDS {
			jobAllocation, err := rm.jobManager.GetJobAllocation(jobID)
			if err != nil {
				continue
			}
			ok, err := rm.clusterManager.CheckJobResources(jobAllocation)
			if err != nil {
				continue
			}
			if ok {
				// remove from newAllocationID
				rm.jobManager.DeleteNewAllocationID(jobID)
				//placeholder
				if jobAllocation.GetTaskAllocations()[0].GetPlaceholder() {
					rm.jobManager.AddNewStartedJobIDs(jobID)
					//update startTime
					rm.jobManager.UpdateJobStartExecutionTime(jobID, now)
				}
				go rm.StartJob(jobAllocation, now)
			}
		}
	}
}

func (rm *ResourceManager) StartJob(jobAllocation *objects.JobAllocation, now time.Time) {
	// acqurie resources
	err := rm.GetClusterManager().AllocJobResources(jobAllocation)
	if err != nil {
		log.Printf("err %v", err)
		//todo
	}
	//start task
	for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
		err := rm.StartTask(taskAllocation, now)
		if err!=nil{
			log.Printf("start pod for task %s in job %s error, err=[%v]\n", taskAllocation.TaskID, jobAllocation.JobID, err)
		}
	}
}

func (rm *ResourceManager) StartTask(taskAllocation *objects.TaskAllocation, now time.Time) error {
	// start Pod
	err := rm.k8sManager.GetPodManager().StartPod(map[string]string{
		"namespace":rm.k8sManager.GetNamespace(),
		"nodeID":    taskAllocation.NodeID,
		"jobID":     taskAllocation.JobID,
		"taskID":    taskAllocation.TaskID,
		"sleepTime": string(taskAllocation.AllocationTimeNanoSecond),
	})
	return err
}

func (rm *ResourceManager) checkFinishedTasks() {
	for {
		annotations := rm.k8sManager.GetPodManager().GetDeletePodAnnoatations()
		go rm.handleTaskFinish(annotations)
	}
}

//podmanager发现任务完成，写入channel, checkFinishedJobs检查到channel中信息，进行处理
func (rm *ResourceManager) handleTaskFinish(annotations map[string]string) {
	//job manager
	rm.jobManager.HandleFinishedTask(annotations)
	// relearse resources
	jobAllocation, err := rm.jobManager.GetJobAllocation(annotations["jobID"])
	if err != nil {
		log.Printf("%v", err)
	}
	err = rm.clusterManager.FreeJobResources(jobAllocation)
	if err != nil {
		log.Printf("%v", err)
	}
}
