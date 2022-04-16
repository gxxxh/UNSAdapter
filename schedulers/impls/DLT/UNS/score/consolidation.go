package score

import (
	"UNSAdapter/pb_gen"
	"UNSAdapter/schedulers/partition"
	"UNSAdapter/utils"
)

type JobAllocationsScore float64

type Calculator interface {
	GetScore(pc *partition.Context, jobAllocations []*pb_gen.JobAllocation) (JobAllocationsScore, interface{})
	GetScoreIncrementally(pc *partition.Context, jobAllocations []*pb_gen.JobAllocation, stub interface{}) (JobAllocationsScore, interface{})
	NewStub() interface{}
}

type ConsolidationScoreCalculator struct {
}

func NewConsolidationScoreCalculator() *ConsolidationScoreCalculator {
	return &ConsolidationScoreCalculator{}
}

type ConsolidationScoreStub struct {
	NodeIDs   map[string]bool
	SocketIDs map[string]bool
}

func (c *ConsolidationScoreCalculator) NewStub() interface{} {
	return &ConsolidationScoreStub{
		NodeIDs:   make(map[string]bool),
		SocketIDs: make(map[string]bool),
	}
}

func (c *ConsolidationScoreCalculator) GetScore(pc *partition.Context, jobAllocations []*pb_gen.JobAllocation) (JobAllocationsScore, interface{}) {
	stub := &ConsolidationScoreStub{
		NodeIDs:   make(map[string]bool),
		SocketIDs: make(map[string]bool),
	}
	c.updateStub(pc, jobAllocations, stub)
	return c.getScore(stub), stub
}

func (c *ConsolidationScoreCalculator) GetScoreIncrementally(pc *partition.Context, jobAllocations []*pb_gen.JobAllocation, stub interface{}) (JobAllocationsScore, interface{}) {
	s := stub.(*ConsolidationScoreStub)
	s = c.cloneStub(s)
	c.updateStub(pc, jobAllocations, s)
	return c.getScore(s), s
}

func (c *ConsolidationScoreCalculator) cloneStub(stub *ConsolidationScoreStub) *ConsolidationScoreStub {
	return &ConsolidationScoreStub{
		NodeIDs:   utils.CloneStringSet(stub.NodeIDs),
		SocketIDs: utils.CloneStringSet(make(map[string]bool)),
	}

}

func (c *ConsolidationScoreCalculator) updateStub(pc *partition.Context, jobAllocations []*pb_gen.JobAllocation, stub *ConsolidationScoreStub) {
	for _, jobAllocation := range jobAllocations {
		for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
			accID := taskAllocation.GetAcceleratorAllocation().GetAcceleratorID()
			stub.NodeIDs[pc.MetalViews.AcceleratorID2NodeID[accID]] = true
			stub.SocketIDs[pc.MetalViews.AcceleratorID2SocketID[accID]] = true
		}
	}
}

func (c *ConsolidationScoreCalculator) getScore(stub *ConsolidationScoreStub) JobAllocationsScore {
	s := JobAllocationsScore(0.)
	s += JobAllocationsScore(-10 * len(stub.NodeIDs))
	s += JobAllocationsScore(-1 * len(stub.SocketIDs))
	return s
}
