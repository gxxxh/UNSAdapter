package makespan

import (
	"UNSAdapter/pb_gen/objects"
	predictorinterfaces "UNSAdapter/predictor/interfaces"
	"UNSAdapter/schedulers/impls/DLT/UNS/benefits/base"
	interfaces2 "UNSAdapter/schedulers/impls/DLT/UNS/benefits/interfaces"
	"UNSAdapter/schedulers/partition"
	"sort"
)

type Calculator struct {
	*base.CalculatorCommon
}

func NewCalculator() *Calculator {
	common := &base.CalculatorCommon{}
	c := &Calculator{}
	common.Impl = c
	c.CalculatorCommon = common
	return c
}

type Stub struct {
	JobID2JCT map[string]int64
}

func (c *Calculator) ByHistory(pc *partition.Context, histories map[string]*objects.JobExecutionHistory) (benefit interfaces2.Benefit, stub interface{}) {
	return 0, nil
}

func (c *Calculator) CloneStub(stub interface{}) interface{} {
	s := &Stub{JobID2JCT: make(map[string]int64)}
	oStub := stub.(*Stub)
	for jobID, JCT := range oStub.JobID2JCT {
		s.JobID2JCT[jobID] = JCT
	}
	return s
}

func (c *Calculator) UpdateStub(pc *partition.Context, contexts map[string]*base.BenefitCalculationContext, stub interface{}) {
	s := stub.(*Stub)
	for _, ctx := range contexts {
		job := ctx.Job
		submitTime := job.GetSubmitTimeNanoSecond()
		finishTime := ctx.FinishTime
		s.JobID2JCT[job.GetJobID()] = finishTime - submitTime
	}
}

func (c *Calculator) calculateMakeSpan(stub *Stub) int64 {
	maximumJCT := int64(0)
	for _, JCT := range stub.JobID2JCT {
		if JCT > maximumJCT {
			maximumJCT = JCT
		}
	}
	return maximumJCT
}

func (c *Calculator) avgJCT2Benefit(avgJCT float64) interfaces2.Benefit {
	return interfaces2.Benefit(-avgJCT)
}

func (c *Calculator) NewStub() interface{} {
	return &Stub{JobID2JCT: make(map[string]int64)}
}

func (c *Calculator) Calculate(prevStub interface{}) interfaces2.Benefit {
	makeSpan := c.calculateMakeSpan(prevStub.(*Stub))
	return interfaces2.Benefit(-makeSpan)
}

func (c *Calculator) PrioritySort(pc *partition.Context, jobs map[string]*objects.Job, predictor predictorinterfaces.Predictor) map[string]int {
	type JobAndTime struct {
		Job  *objects.Job
		Time int64
	}
	jobAndTimes := make([]*JobAndTime, 0, len(jobs))
	for _, job := range jobs {
		et := predictor.PredictSolelyFastestExecutionTime(job)
		jobAndTimes = append(jobAndTimes, &JobAndTime{
			Job:  job,
			Time: et,
		})
	}
	sort.Slice(jobAndTimes, func(i, j int) bool {
		return jobAndTimes[i].Time < jobAndTimes[j].Time
	})
	result := make(map[string]int)
	for i, jat := range jobAndTimes {
		result[jat.Job.GetJobID()] = i
	}
	return result
}
