package naive

import (
	"UNSAdapter/pb_gen"
	"UNSAdapter/pb_gen/configs"
	eventsobjs "UNSAdapter/pb_gen/events"
	"UNSAdapter/pb_gen/objects"
	"UNSAdapter/predictor"
	interfaces2 "UNSAdapter/predictor/interfaces"
	base2 "UNSAdapter/schedulers/impls/DLT/base"
	"UNSAdapter/schedulers/interfaces"
	"log"
	"sort"
)

type Scheduler struct {
	*base2.DLTSchedulerTemplate

	Config    *configs.NaiveSchedulerConfiguration
	Predictor interfaces2.Predictor
}

func (s *Scheduler) GetSchedulerID() string {
	return s.Config.GetSchedulerID()
}

func Build(configuration interface{}, pusher base2.EventPusher, partitionContextAware base2.PartitionContextAware) (interfaces.Scheduler, error) {
	c := configuration.(*configs.NaiveSchedulerConfiguration)
	sche := &Scheduler{
		Config: c,
	}
	sche.DLTSchedulerTemplate = base2.NewIntervalSchedulerTemplate(sche, c.GetIntervalNano(), partitionContextAware, c.GetSyncMode(), pusher)
	sche.Predictor = predictor.BuildPredictor(c.GetPredictorConfiguration())
	return sche, nil
}

func (s *Scheduler) DoSchedule() *eventsobjs.SSUpdateAllocationsEvent {
	partitionContext := s.GetPartitionContext().Clone(false)
	unscheduledJobs := make([]*objects.Job, 0)
	for _, job := range partitionContext.UnfinishedJobs {
		if _, ok := partitionContext.Allocations[job.GetJobID()]; !ok {
			unscheduledJobs = append(unscheduledJobs, job)
		}
	}
	if len(unscheduledJobs) == 0 {
		return nil
	}
	nodesMap := make(map[string]*objects.Node)
	for _, node := range partitionContext.Meta.GetNodes() {
		nodesMap[node.GetNodeID()] = node
	}
	acceleratorID2NodeID := make(map[string]string)
	for nodeID, accelerators := range partitionContext.MetalViews.NodeID2Accelerators {
		for _, accelerator := range accelerators {
			acceleratorID2NodeID[accelerator.GetAcceleratorID()] = nodeID
		}
	}

	acceleratorIDs := make([]string, 0, len(acceleratorID2NodeID))
	for acceleratorID := range partitionContext.MetalViews.AcceleratorID2Accelerator {
		acceleratorIDs = append(acceleratorIDs, acceleratorID)
	}
	sort.Strings(acceleratorIDs)

	unoccupiedAcceleratorIDsMap := make(map[string]bool)
	for acceleratorID := range acceleratorID2NodeID {
		unoccupiedAcceleratorIDsMap[acceleratorID] = true
	}
	for _, pendingAllocation := range partitionContext.Allocations {
		for _, taskAllocation := range pendingAllocation.GetTaskAllocations() {
			acceleratorAllocation := taskAllocation.GetAcceleratorAllocation()
			delete(unoccupiedAcceleratorIDsMap, acceleratorAllocation.GetAcceleratorID())
		}
	}

	if len(unoccupiedAcceleratorIDsMap) == 0 {
		return nil
	}

	newAllocations := make([]*pb_gen.JobAllocation, 0)
	for _, job := range unscheduledJobs {
		taskGroup := job.GetTaskGroup()
		switch taskGroup.GetTaskGroupType() {
		case objects.TaskGroupType_taskGroupTypeSingle, objects.TaskGroupType_taskGroupTypeGang:
		default:
			log.Printf("Naive Scheduler support task groups [%v, %v], but received task groupd of [%v]", objects.TaskGroupType_taskGroupTypeSingle, objects.TaskGroupType_taskGroupTypeGang, taskGroup.GetTaskGroupType())
			continue
		}
		taskGroupLen := len(taskGroup.GetTasks())
		if len(unoccupiedAcceleratorIDsMap) < taskGroupLen {
			break
		}
		chosenAcceleratorIDs := make([]string, 0, taskGroupLen)
		for _, acceleratorID := range acceleratorIDs {
			if unoccupiedAcceleratorIDsMap[acceleratorID] {
				chosenAcceleratorIDs = append(chosenAcceleratorIDs, acceleratorID)
				if len(chosenAcceleratorIDs) == taskGroupLen {
					break
				}
			}
		}
		for _, acceleratorID := range chosenAcceleratorIDs {
			delete(unoccupiedAcceleratorIDsMap, acceleratorID)
		}
		taskAllocations := make([]*objects.TaskAllocation, 0, taskGroupLen)
		for i, task := range taskGroup.GetTasks() {
			accID := chosenAcceleratorIDs[i]
			acceleratorAllocation := &objects.AcceleratorAllocation{
				AcceleratorID: accID,
			}
			taskAllocation := &objects.TaskAllocation{
				NodeID:                acceleratorID2NodeID[accID],
				JobID:                 job.GetJobID(),
				TaskID:                task.GetTaskID(),
				Placeholder:           true,
				AcceleratorAllocation: acceleratorAllocation,
			}
			taskAllocations = append(taskAllocations, taskAllocation)
		}
		newAllocation := &pb_gen.JobAllocation{
			JobAllocation: &objects.JobAllocation{
				JobID:             job.GetJobID(),
				ResourceManagerID: s.Config.GetResourceManagerID(),
				PartitionID:       s.Config.GetPartitionID(),
				TaskAllocations:   taskAllocations,
			},
		}
		newAllocations = append(newAllocations, newAllocation)
	}
	return &eventsobjs.SSUpdateAllocationsEvent{NewJobAllocations: pb_gen.UnwrapJobAllocations(newAllocations)}
}

func (s *Scheduler) GetPredictor() interfaces2.Predictor {
	return s.Predictor
}
