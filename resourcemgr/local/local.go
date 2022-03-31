package local

import (
	"UNSAdapter/events"
	events2 "UNSAdapter/pb_gen/events"
	"UNSAdapter/pb_gen/objects"
	"UNSAdapter/resourcemgr/cluster_manager"
	"UNSAdapter/resourcemgr/job_manager"
	"UNSAdapter/resourcemgr/k8s_manager"
	"log"
	"time"
)

type ResourceManager struct {
	RMID           string
	clusterManager *cluster_manager.ClusterManager
	podManager     *k8s_manager.PodManager
	jobManager     *job_manager.JobsManager
}

func NewResourceManager() *ResourceManager {
	return nil
}

func (rm *ResourceManager) GetJobManager() (manager *job_manager.JobsManager) {
	return rm.jobManager
}

func (rm *ResourceManager) GetPodManager() *k8s_manager.PodManager {
	return rm.podManager
}

func (rm *ResourceManager) GetClusterManager() (manager *cluster_manager.ClusterManager) {
	return rm.clusterManager
}

func (rm *ResourceManager) GetResourceManagerID() string {
	return rm.RMID
}

func (rm *ResourceManager) InitClusterInfo() {

}

func Push(rmID string, partitionID string, event *events.Event) {

}

//todo job simulator
func (rm *ResourceManager) checkSubmitJobs() {

}

func (rm *ResourceManager) checkFinishedJobs(){

}
//
func (rm *ResourceManager) handleSSUpdateAllocation(eo *events2.SSUpdateAllocationsEvent) {
	alloctions := eo.NewJobAllocations
	nonPlaceholders := make([]*objects.JobAllocation, 0, len(alloctions))
	placeholders := make([]*objects.JobAllocation, 0, len(alloctions))
	for _, allocation := range alloctions {
		if allocation.GetTaskAllocations()[0].GetPlaceholder() {
			placeholders = append(placeholders, allocation)
		} else {
			nonPlaceholders = append(nonPlaceholders, allocation)
		}
	}

nextNonPlaceholderAlloc:
	for _, nonPlaceholderAllocation := range nonPlaceholders {
		//check task is runing
		if rm.GetJobManager().CheckJobRunning(nonPlaceholderAllocation.GetJobID()) {
			log.Printf("simulator ignores allocation of jobID = %s since it is already allocated", nonPlaceholderAllocation.GetJobID())
			continue nextNonPlaceholderAlloc
		}
		// check resource is free
		ok, err := rm.GetClusterManager().CheckJobResources(nonPlaceholderAllocation)
		if err != nil {
			log.Printf("%v", err)
			//todo
		}
		if ok {
			rm.StartJob(nonPlaceholderAllocation, time.Now())
		} else {
			log.Printf("resources for job %s's alllocation is not free\n", nonPlaceholderAllocation.GetJobID())
		}
	}
nextPlaceholderAlloc:
	//todo difference between nonplaceholder
	for _, placeholderAllocation := range placeholders {
		if rm.GetJobManager().CheckJobRunning(placeholderAllocation.GetJobID()) {
			continue nextPlaceholderAlloc
		}
		// check resource
		ok, err := rm.GetClusterManager().CheckJobResources(placeholderAllocation)
		if err != nil {
			log.Printf("")
		}
		if ok {
			rm.StartJob(placeholderAllocation, time.Now())
		} else {
			log.Printf("resources for job %s's alllocation is not free\n", placeholderAllocation.GetJobID())
		}
	}
}

func (rm *ResourceManager) StartJob(jobAllocation *objects.JobAllocation, now time.Time) {
	rm.jobManager.AddNewJobAllocation(jobAllocation)
	// todo startTime 是任务开始执行时间，还是pod创建的时间
	// acqurie resources
	err := rm.GetClusterManager().AllocJobResources(jobAllocation)
	if err != nil {
		log.Printf("err %v", err)
		//todo
	}
	//start task
	for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
		rm.StartTask(jobAllocation.JobID, taskAllocation, now)
	}
}

func (rm *ResourceManager) StartTask(jobID string, taskAllocation *objects.TaskAllocation, now time.Time) error {
	// start Pod
	rm.podManager.StartPod(taskAllocation)
	//update  startTime in task allocation and exeuction history
	err := rm.jobManager.HandleNewStartTask(jobID, taskAllocation.TaskID, now)
	if err != nil {
		return err
	}
	return nil
}

//todo 所有task结束后一起回收资源？还是结束一个task回收一个？
func (rm *ResourceManager) HandleTaskFinish(annotations map[string]string) error {
	//job manager
	rm.jobManager.HandleFinishedTask(annotations)
	// relearse resources
	jobAllocation, err := rm.jobManager.GetJobAllocation(annotations["jobID"])
	if err != nil {
		return err
	}
	return rm.clusterManager.FreeJobResources(jobAllocation)
}
