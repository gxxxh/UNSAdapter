package queue_based

import (
	"UNSAdapter/pb_gen/configs"
	eventobjs "UNSAdapter/pb_gen/events"
	"UNSAdapter/pb_gen/objects"
	base2 "UNSAdapter/schedulers/impls/DLT/base"
	"UNSAdapter/schedulers/interfaces"
	"UNSAdapter/schedulers/partition"
	"math"
	"sort"
)

type EDFScheduler struct {
	*QueueBasedSchedulerTemplate

	Config *configs.EDFSchedulerConfiguration
}

func (s *EDFScheduler) GetSchedulerID() string {
	return s.Config.GetSchedulerID()
}

func BuildEDF(configuration interface{}, pusher base2.EventPusher, partitionContextAware base2.PartitionContextAware) (interfaces.Scheduler, error) {
	c := configuration.(*configs.EDFSchedulerConfiguration)
	sche := &EDFScheduler{
		Config: c,
	}
	var err error
	provideMode := base2.ProvideTypeDefault | base2.ProvideTypeOnlyNonSpaceSharing
	sche.QueueBasedSchedulerTemplate, err = BuildTemplate(&QueueBasedSchedulerParam{
		Impl:                         sche,
		PredictorConfiguration:       c.PredictorConfiguration,
		Pusher:                       pusher,
		PartitionContextAware:        partitionContextAware,
		IntervalNano:                 c.GetIntervalNano(),
		SyncMode:                     c.GetSyncMode(),
		AllocationProvideMode:        provideMode,
		ReturnAllSchedulingDecisions: c.ReturnAllScheduleDecisions,
		//AllocationsProvider:          &base2.AllocationsProviderImpl{RandomMode: true},
		//MaxPossibleAllocationsCount:  10,
	})
	if err != nil {
		return nil, err
	}
	return sche, nil
}

func (s *EDFScheduler) PrioritySort(pc *partition.Context, jobs map[string]*objects.Job) []*objects.Job {
	result := make([]*objects.Job, 0, len(jobs))
	for _, job := range jobs {
		result = append(result, job)
	}
	type jobWithExecutionTime struct {
		Job           *objects.Job
		ExecutionTime int64
	}
	jobWithETs := make(map[string]*jobWithExecutionTime)
	for _, job := range jobs {
		jobWithETs[job.GetJobID()] = &jobWithExecutionTime{
			Job:           job,
			ExecutionTime: s.Predictor.PredictSolelyFastestExecutionTime(job),
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].GetDeadline() == math.MaxInt64 && result[j].GetDeadline() == math.MaxInt64 {
			//return result[i].GetSubmitTimeNanoSecond() < result[j].GetSubmitTimeNanoSecond()
			return jobWithETs[result[i].GetJobID()].ExecutionTime < jobWithETs[result[j].GetJobID()].ExecutionTime
		}
		if result[i].GetDeadline() != math.MaxInt64 && result[j].GetDeadline() != math.MaxInt64 {
			return result[i].GetDeadline() < result[j].GetDeadline()
		}
		if result[i].GetDeadline() != math.MaxInt64 {
			return true
		} else {
			return false
		}
	})
	return result
}

func (s *EDFScheduler) GetJobAllocationScore(param *JobAllocationScorerParam) JobAllocationScore {
	possibleAllocation := param.JobAllocation
	pr := param.PredictResult
	//pc := param.PC
	//if possibleAllocation.GetTaskAllocations()[0].GetAllocationTimeNanoSecond() != pc.FixedNow() {
	//	return JobAllocationScore(math.Inf(-1))
	//}
	r := pr.GetResult(possibleAllocation.GetTaskAllocations()[0])
	//job := pc.GetJob(possibleAllocation.GetJobID())
	finishTime := *r.GetFinishNanoTime()
	//startTime := *r.GetStartExecutionNanoTime()
	return -JobAllocationScore(finishTime)
	//if job.GetDeadline() == math.MaxInt64 {
	//	当没有deadline时，结束时间越早，分越高
	//return -JobAllocationScore(finishTime)
	//}
	//vioDeadline := finishTime - job.GetDeadline()
	//return -JobAllocationScore(vioDeadline)
}

func (s *EDFScheduler) DoSchedule() *eventobjs.SSUpdateAllocationsEvent {
	return s.QueueBasedSchedulerTemplate.DoScheduleTemplate()
}
