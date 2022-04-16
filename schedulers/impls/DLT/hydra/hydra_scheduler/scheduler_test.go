package hydra_scheduler

import (
	interfaces2 "UNSAdapter/predictor/interfaces"
	"UNSAdapter/schedulers/impls/DLT/hydra/adapter"
	"UNSAdapter/schedulers/impls/DLT/hydra/hydra_scheduler/cost"
	"UNSAdapter/schedulers/impls/DLT/hydra/types"
	"UNSAdapter/schedulers/partition"
	"testing"
	"time"
)

func TestName(t *testing.T) {
	ctx := &partition.Context{}
	var predictor interfaces2.Predictor
	scheduleContext := adapter.BuildScheduleContext(ctx, predictor)
	s := initHydraBABHeuristicScheduler(10000 * time.Millisecond)
	s.SetCluster(scheduleContext.Cluster)
	s.OnScheduleEvent(types.NewScheduleEventJobsArrived(scheduleContext.JobMetas))
}

func initHydraBABHeuristicScheduler(latency time.Duration) types.Scheduler {
	return New(
		WithScheme(NewBasicScheduleScheme(true, false, -1, true)),
		WithDistanceAlgo(NewMinCostDistanceAlgo(
			//cost.NewBranchAndBoundAlgoWithLatency(cost.BranchAndBoundLCStandardPredictCost, cost.BranchAndBoundAlgoTypeFixNonDDL, time.Duration(latencySec)*time.Second, cost.NewSwapHeuristic()),
			cost.NewBranchAndBoundAlgoWithLatency(cost.BranchAndBoundLCStandardPredictCost, cost.BranchAndBoundAlgoTypeFixNonDDL, latency, cost.NewSwapHeuristic()),
			cost.NewSimpleAddCostSolverMaker(cost.DDLCostTypeStrict, 1e20))),
	)
}

//
//import (
//	"DES-go/schedulers/kmeans_scheduler/cost"
//	"DES-go/schedulers/types"
//	"DES-go/simulator"
//	"encoding/json"
//	"testing"
//)
//
//func Test_scheduler(t *testing.T) {
//	scheduler := New(WithScheme(
//		NewBasicScheduleScheme(false, false, -1, false)),
//		WithDistanceAlgo(NewMinCostDistanceAlgo(cost.NewBranchAndBoundAlgo(cost.BranchAndBoundLCStandardPartialCost, cost.BranchAndBoundAlgoTypeAllPermutation), cost.NewSimpleAddCostSolverMaker(cost.DDLCostTypeStrict, 1e20))),
//	)
//	simu := simulator.NewSimulator(scheduler,
//		simulator.WithOptionLogPrintLevel(simulator.ShortMsgPrint),
//		simulator.WithOptionLogPath("/Users/purchaser/go/src/DES-go/logs"),
//		simulator.WithOptionGPUType2Count(map[types.GPUType]int{
//			"V100": 1,
//			"T4":   1,
//		}))
//	simulator.SetDataSource([]*simulator.JobMeta{
//		simulator.NewJobMeta("job1", 0, 12, map[types.GPUType]types.Duration{"V100": 5, "T4": 10}),
//		simulator.NewJobMeta("job2", 0, 7, map[types.GPUType]types.Duration{"V100": 6, "T4": 12}),
//		simulator.NewJobMeta("job3", 0, 6, map[types.GPUType]types.Duration{"V100": 3, "T4": 5}),
//	})
//	simu.Run()
//}
//
//func Test2(t *testing.T) {
//	a := float64(1) / float64(0)
//	print(a)
//}
//
//type IA interface {
//	A() string
//}
//type A struct {
//}
//
//func (A) A() string {
//	return "AAA"
//}
//
//func TestJson(t *testing.T) {
//	a := A{}
//	r, e := json.Marshal(a)
//	t.Log(e)
//	t.Log(r)
//}
