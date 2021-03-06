package local

import (
	"UNSAdapter/resourcemgr/cluster_manager"
	"UNSAdapter/resourcemgr/job_manager"
	"UNSAdapter/resourcemgr/k8s_manager"
	"UNSAdapter/resourcemgr/simulator"
	utils2 "UNSAdapter/resourcemgr/utils"
	"UNSAdapter/utils"
	"fmt"
	"github.com/MLSched/UNS/events"
	"github.com/MLSched/UNS/pb_gen/configs"
	events2 "github.com/MLSched/UNS/pb_gen/events"
	"github.com/MLSched/UNS/pb_gen/objects"
	"github.com/MLSched/UNS/schedulers"
	"github.com/MLSched/UNS/schedulers/interfaces"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"
)

var CheckFinishedInterval = 3 * time.Second
var CheckNotStartedJobInterval = time.Second

type ResourceManager struct {
	wg             *sync.WaitGroup
	RMID           string
	config         *configs.RMConfiguration
	clusterManager *cluster_manager.ClusterManager
	//podManager     *k8s_manager.PodManagr
	k8sManager   *k8s_manager.K8sManager
	jobManager   *job_manager.JobsManager
	jobSimulator *simulator.JobSimulator
	serviceInst  interfaces.Service
}

func NewResourceManager(simulatorConfig *configs.DLTSimulatorConfiguration, withPod bool) *ResourceManager {
	schedulers.InitLocalSchedulersService()
	rm := &ResourceManager{
		wg:             &sync.WaitGroup{},
		RMID:           simulatorConfig.GetResourceManagerID(),
		config:         simulatorConfig.GetRmConfiguration(),
		clusterManager: cluster_manager.NewClusterManager("Cluster-ID", simulatorConfig.GetRmConfiguration().GetCluster()),
		k8sManager:     nil,
		jobManager:     job_manager.NewJobManager(),
		jobSimulator:   simulator.NewJobSimulator(),
		serviceInst:    schedulers.GetServiceInstance(),
	}
	if withPod{
		rm.k8sManager = k8s_manager.NewK8sManager()
	}
	return rm
}

func (rm *ResourceManager) GetJobManager() (manager *job_manager.JobsManager) {
	return rm.jobManager
}

func (rm *ResourceManager)GetJobSimulator()(js *simulator.JobSimulator){
	return rm.jobSimulator
}
func (rm *ResourceManager) GetClusterManager() (manager *cluster_manager.ClusterManager) {
	return rm.clusterManager
}

func (rm *ResourceManager) GetResourceManagerID() string {
	return rm.RMID
}

func (rm *ResourceManager) RegisterResourceManager() {
	result := rm.serviceInst.RegisterRM(&events2.RMRegisterResourceManagerEvent{Configuration: rm.config}, rm)
	if !result.Succeeded {
		panic(result.Reason)
	}
	if size := len(rm.config.GetCluster().GetPartitions()); size == 0 || size > 1 {
		panic("ContinuousAsyncDLTSimulator partition count is not 1.")
	}
}

func (rm *ResourceManager) initEnvironment() {
	// start scheduler service
	rm.serviceInst.StartService()
	// register resource manager
	rm.RegisterResourceManager()
	//start k8s manager
	if rm.k8sManager!=nil{
		go rm.k8sManager.Run()
	}
	//start accept jobs
	//go rm.jobSimulator.Run()
}
func (rm *ResourceManager) Run() {
	rm.initEnvironment()
	if rm.k8sManager!=nil{
		rm.wg.Add(3)
		go rm.checkFinishedTasks()
		go rm.checkSubmitJobs()
		go rm.checkNotStartedJob()
		go rm.checkFinishedJobs()
		rm.wg.Wait()
	}else{
		rm.wg.Add(2)
		//go rm.checkFinishedTasks()
		go rm.checkSubmitJobs()
		//go rm.checkNotStartedJob()
		go rm.checkFinishedJobs()
		rm.wg.Wait()
	}


}

func (rm *ResourceManager) checkSubmitJobs() {
	for {
		newJobs, isClose := rm.jobSimulator.GetNewJob()
		if !isClose {
			break
		}
		newJobIDs := make([]string, 0, len(newJobs))
		for _, job := range newJobs {
			newJobIDs = append(newJobIDs, job.GetJobID())
			fmt.Printf("Resource Manager find new job %s, submitTime %v\n", job.GetJobID(), time.UnixMicro(job.SubmitTimeNanoSecond/1000))
		}
		fmt.Printf("Resource Manager find new Jobs, ids: %v\n", newJobIDs)
		rm.jobManager.AddJob(newJobs)
		rm.pushNewJobs(newJobs...)
	}
	fmt.Printf("check Submit Jobs done!\n")
	rm.wg.Done()
}

func (rm *ResourceManager) checkFinishedJobs() {
	for {
		time.Sleep(CheckFinishedInterval)
		if rm.k8sManager==nil{
			if rm.jobManager.GetNewAllocationNums()>0{
				break
			}else{
				continue
			}
		}
		newStartedJobIDs := rm.jobManager.GetNewStartedJobIDs()
		newlyStartedAllocations := make([]*objects.JobAllocation, 0, len(newStartedJobIDs))
		for _, jobID := range newStartedJobIDs {
			jobAllocation, err := rm.jobManager.GetJobAllocation(jobID)
			if err != nil {
				fmt.Printf("CheckFinishedJobs error, err=%v \n", err)
				continue
			}
			newlyStartedAllocations = append(newlyStartedAllocations, jobAllocation)
		}
		ok, finishedJobIDs, jobExecutionHistories := rm.jobManager.GetFinishedJobInfo()
		if len(newlyStartedAllocations) == 0 && !ok {
			continue
		}
		schedulerType := rm.config.SchedulersConfiguration.PartitionID2SchedulerConfiguration[rm.clusterManager.GetPratitionID()].SchedulerType.String()
		//save to file
		dltJobs := rm.jobSimulator.GetDLTJobs()
		utils2.SaveFinishedJobInfo(schedulerType, jobExecutionHistories, dltJobs,rm.clusterManager.GetAllAccelerators(), rm.jobSimulator.GetSubmitTime())

		//update job info
		rm.jobManager.HandleRMUpdateAllocationEvent(finishedJobIDs)
		break
	}
	log.Printf("check finished jobs done\n")
	rm.wg.Done()
}

//??????????????????allocations????????????????????????????????????
func (rm *ResourceManager) checkNotStartedJob() {
	i:= 0
	for {
		time.Sleep(CheckNotStartedJobInterval)
		now := time.Now()
		notStartedJobIDS := rm.jobManager.GetNewAllocationIDs()
		for _, jobID := range notStartedJobIDS {
			jobAllocation, err := rm.jobManager.GetJobAllocation(jobID)
			if err != nil {
				fmt.Printf("CheckNotStartedJob err, err=%v\n", err)
				continue
			}

			startTime := jobAllocation.GetTaskAllocations()[0].GetStartExecutionTimeNanoSecond().GetValue()
			cur := now.UnixNano()
			//fmt.Printf("job %s startExecutionTime: %v, cur time %v\n", jobAllocation.GetJobID(),time.UnixMicro(startTime/1000), time.UnixMicro(cur/1000))
			if cur > startTime {
				ok, err := rm.clusterManager.CheckJobResources(jobAllocation)
				if err != nil {
					fmt.Printf("CheckNotStartedJob err, err=%v\n", err)
					continue
				}
				if ok {
					i = i+1
					rm.jobManager.DeleteNewAllocationID(jobID)
					//go rm.StartJob(jobAllocation, now)//????????????????????????????????????????????????????????????
					//?????????????????????GPU????????????????????????????????????????????????????????????????????????
					rm.StartJob(jobAllocation, time.Now())
				}
			}
		}
		if i>=rm.jobManager.GetJobNums()&&rm.jobManager.GetJobNums()>0{
			break
		}
	}
	fmt.Printf("check not start job done\n")
	rm.wg.Done()
}
func (rm *ResourceManager) HandleEvent(event *events.Event) {
	err := func() error {
		switch eo := event.Data.(type) {
		case *events2.SSUpdateAllocationsEvent:
			return rm.handleSSUpdateAllocation(eo)

		default:
			panic(fmt.Sprintf("Resource Manager handle unknown event %+v\n", event))
		}
	}()
	if err != nil {
		events.Reply(event, &events.Result{
			Succeeded: false,
			Reason:    err.Error(),
		})
	} else {
		events.ReplySucceeded(event)
	}
}
func (rm *ResourceManager) handleSSUpdateAllocation(eo *events2.SSUpdateAllocationsEvent) error {
	fmt.Printf("Resource Manager Receive update allocations, time is %v\n", time.Now())
	jobAllocations := eo.NewJobAllocations
	schedulerType := rm.config.SchedulersConfiguration.PartitionID2SchedulerConfiguration[rm.clusterManager.GetPratitionID()].SchedulerType.String()
	//save to file
	dltJobs := rm.jobSimulator.GetDLTJobs()
	utils2.SaveJobAllocations(schedulerType, jobAllocations, dltJobs, rm.clusterManager.GetAllAccelerators(), rm.jobSimulator.GetSubmitTime())
	//  sort job allocations by start time
	sorter := utils.Sorter{
		LenFunc: func() int {
			return len(jobAllocations)
		},
		LessFunc: func(i, j int) bool {
			return jobAllocations[i].GetTaskAllocations()[0].GetStartExecutionTimeNanoSecond().GetValue() < jobAllocations[j].GetTaskAllocations()[0].GetStartExecutionTimeNanoSecond().GetValue()
		},
		SwapFunc: func(i, j int) {
			t := jobAllocations[i]
			jobAllocations[i] = jobAllocations[j]
			jobAllocations[j] = t
		},
	}
	sort.Sort(sorter)
	for _, jobAllocation := range jobAllocations {
		rm.jobManager.AddJobAllocation(jobAllocation)
	}
	//update job allocation event
	//ev := &events2.RMUpdateAllocationsEvent{UpdatedJobAllocations: jobAllocations}
	//rm.pushUpdateAllocations(ev)
	return nil
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
//			fmt.Printf("simulator ignores allocation of jobID = %s since it is already allocated", nonPlaceholderAllocation.GetJobID())
//			continue nextNonPlaceholderAlloc
//		}
//		// check resource is free
//		ok, err := rm.GetClusterManager().CheckJobResources(nonPlaceholderAllocation)
//		if err != nil {
//			fmt.Printf("%v", err)
//		}
//		if ok {
//			//go rm.StartJob(nonPlaceholderAllocation, now)
//			filteredAllocations = append(filteredAllocations, nonPlaceholderAllocation)
//		} else {
//			fmt.Printf("resources for job %s's alllocation is not free\n", nonPlaceholderAllocation.GetJobID())
//		}
//	}
//nextPlaceholderAlloc:
//	//todo placeholder??????????????????
//	for _, placeholderAllocation := range placeholders {
//		if rm.GetJobManager().CheckJobRunning(placeholderAllocation.GetJobID()) {
//			continue nextPlaceholderAlloc
//		}
//		// check resource
//		ok, err := rm.GetClusterManager().CheckJobResources(placeholderAllocation)
//		if err != nil {
//			fmt.Printf("")
//		}
//		if ok {
//			//rm.StartJob(placeholderAllocation, time.Now())
//			filteredAllocations = append(filteredAllocations, placeholderAllocation)
//		} else {
//			fmt.Printf("resources for job %s's alllocation is not free\n", placeholderAllocation.GetJobID())
//		}
//	}
//	if len(filteredAllocations) > 0 {
//		for _, jobAC := range filteredAllocations {
//			for _, taskAC := range jobAC.TaskAllocations {
//				taskAC.StartExecutionTimeNanoSecond = &wrappers.Int64Value{Value: int64(now.UnixNano())}
//			}
//			rm.jobManager.AddJobAllocation(jobAC)
//		}
//		//push UpdateAllocations Event
//		ev := &events2.RMUpdateAllocationsEvent{UpdatedJobAllocations: filteredAllocations}
//
//	}
//}

////??????????????????allocations????????????????????????????????????
//func (rm *ResourceManager) checkNotStartedJob() {
//	for {
//		//time.Sleep(CheckNotStartedJobInterval)
//		now := time.Now()
//		notStartedJobIDS := rm.jobManager.GetNewAllocationIDs()
//		for _, jobID := range notStartedJobIDS {
//			jobAllocation, err := rm.jobManager.GetJobAllocation(jobID)
//			if err != nil {
//				continue
//			}
//			ok, err := rm.clusterManager.CheckJobResources(jobAllocation)
//			if err != nil {
//				continue
//			}
//			if ok {
//				// remove from newAllocationID
//				rm.jobManager.DeleteNewAllocationID(jobID)
//				//placeholder
//				if jobAllocation.GetTaskAllocations()[0].GetPlaceholder() {
//					rm.jobManager.AddNewStartedJobIDs(jobID)
//					//update startTime
//					rm.jobManager.UpdateJobStartExecutionTime(jobID, now)
//				}
//				go rm.StartJob(jobAllocation, now)
//			}
//		}
//	}
//}

func (rm *ResourceManager) StartJob(jobAllocation *objects.JobAllocation, now time.Time) {
	fmt.Printf("Resource Manager: start job %s at time %v \n", jobAllocation.GetJobID(), now)
	// acqurie resources
	err := rm.GetClusterManager().AllocJobResources(jobAllocation)
	if err != nil {
		errMsg := fmt.Sprintf("Alloc Resource for job %s error, err = %v\n", jobAllocation.GetJobID(), err)
		fmt.Printf(errMsg)
		//todo panic
		return
	}
	rm.jobManager.UpdateJobStartExecutionTime(jobAllocation.JobID, now)
	//start task
	for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
		err := rm.StartTask(taskAllocation)
		if err != nil {
			errMsg := fmt.Sprintf("start pod for task %s in job %s error, err=[%v]\n", taskAllocation.TaskID, jobAllocation.JobID, err)
			fmt.Printf(errMsg)
			return
		}
	}
}

func (rm *ResourceManager) StartTask(taskAllocation *objects.TaskAllocation) error {
	accelerator := rm.clusterManager.GetAccelerator(taskAllocation.GetAcceleratorAllocation().GetAcceleratorID())
	// start Pod
	sleepTime := strconv.FormatInt(rm.jobSimulator.GetRuningTime(taskAllocation.JobID, accelerator.GetAcceleratorMetaInfo().GetBriefType()), 10)
	err := rm.k8sManager.GetPodManager().StartPod(map[string]string{
		"namespace": rm.k8sManager.GetNamespace(),
		"nodeID":    taskAllocation.NodeID,
		"jobID":     taskAllocation.JobID,
		"taskID":    taskAllocation.TaskID,
		"sleepTime": sleepTime,
	})
	return err
}

func (rm *ResourceManager) checkFinishedTasks() {
	for {
		annotations := rm.k8sManager.GetPodManager().GetDeletePodAnnoatations()
		go rm.handleTaskFinish(annotations)
	}
}

//podmanager???????????????????????????channel, checkFinishedJobs?????????channel????????????????????????
func (rm *ResourceManager) handleTaskFinish(annotations map[string]string) {
	finishTime, _ := strconv.ParseInt(annotations["finishTime"], 10, 64)
	fmt.Printf("Resource Manager: task %s in job %s finished, time is %v\n", annotations["jobID"], annotations["taskID"], time.UnixMicro(finishTime/1000))
	//job manager
	err := rm.jobManager.HandleFinishedTask(annotations)
	if err != nil {
		fmt.Printf("handleTaskFinish error, err=[%v]\n", err)
	}
	// relearse resources
	taskAlloc, err := rm.jobManager.GetTaskAllocation(annotations["jobID"], annotations["taskID"])
	if err != nil {
		fmt.Printf("handleTaskFinish error, err=[%v]\n", err)
	}
	jobAlloc, err := rm.jobManager.GetJobAllocation(annotations["jobID"])
	if err != nil {
		fmt.Printf("handleTaskFinish error, err=[%v]\n", err)
	}
	err = rm.clusterManager.FreeTaskResources(taskAlloc, jobAlloc.GetPartitionID())
	if err != nil {
		fmt.Printf("handleTaskFinish error, err=[%v]\n", err)
	}
}

func (rm *ResourceManager) pushNewJobs(newJobs ...*objects.Job) {
	rm.push(&events.Event{
		Data: &events2.RMUpdateJobsEvent{
			NewJobs: newJobs,
		},
	})
	jobIDs := make([]string, 0, len(newJobs))
	for _, newJob := range newJobs {
		jobIDs = append(jobIDs, newJob.GetJobID())
	}
	fmt.Printf("ResourceManager pushNewJobs newJobs, time is %v\n", time.Now())
}

func (rm *ResourceManager) push(event *events.Event) {
	inst := schedulers.GetServiceInstance()
	inst.Push(rm.GetResourceManagerID(), rm.clusterManager.GetPratitionID(), event)
}

func (rm *ResourceManager) pushUpdateAllocations(event *events2.RMUpdateAllocationsEvent) {
	rm.push(&events.Event{
		Data: event,
	})
	allocations := event.GetUpdatedJobAllocations()
	jobIDs := make([]string, 0, len(allocations))
	for _, allocation := range allocations {
		jobIDs = append(jobIDs, allocation.GetJobID())
	}
	fmt.Printf("resource manager pushUpdateAllocations jobIDs = %+v\n", jobIDs)
}
