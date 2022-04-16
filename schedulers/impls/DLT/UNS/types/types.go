package types

import (
	"UNSAdapter/pb_gen"
	"UNSAdapter/pb_gen/objects"
	"UNSAdapter/predictor/interfaces"
	benefitsinterfaces "UNSAdapter/schedulers/impls/DLT/UNS/benefits/interfaces"
	"UNSAdapter/schedulers/impls/DLT/UNS/score"
	"UNSAdapter/schedulers/partition"
)

type AllocContext struct {
	PC                           *partition.Context
	Job                          *objects.Job
	JobAllocation                *pb_gen.JobAllocation
	NewJobAllocations            []*pb_gen.JobAllocation
	NewJobAllocationsFingerPrint string
	Benefit                      benefitsinterfaces.Benefit
	BenefitStub                  interface{}
	PredictResult                interfaces.PredictResult
	Score                        score.JobAllocationsScore
}

func (j *AllocContext) GetBenefit() benefitsinterfaces.Benefit {
	return j.Benefit
}

func (j *AllocContext) GetScore() score.JobAllocationsScore {
	return j.Score
}
