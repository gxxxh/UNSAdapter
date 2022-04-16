package simulator

import (
	"UNSAdapter/pb_gen/configs"
	"UNSAdapter/pb_gen/objects"
	"UNSAdapter/utils"
	"io/ioutil"
	"math"
	"sort"
)

// iterJobsBySubmitTime 获取根据submitTime排序的下一波任务。(submitTime int64, jobs []*objects.Job, next func())
func iteratorJobsBySubmitTime(jobs []*objects.Job) func() (int64, []*objects.Job, func() bool) {
	//根据提交时间进行排序
	sorter := &utils.Sorter{
		LenFunc: func() int {
			return len(jobs)
		},
		LessFunc: func(i, j int) bool {
			return jobs[i].GetSubmitTimeNanoSecond() < jobs[j].GetSubmitTimeNanoSecond()
		},
		SwapFunc: func(i, j int) {
			o := jobs[i]
			jobs[i] = jobs[j]
			jobs[j] = o
		},
	}
	sort.Sort(sorter)
	//排序后遍历
	currIndex := 0
	next := func(batchJobsSize int) bool {
		currIndex += batchJobsSize
		if currIndex >= len(jobs) {
			return false
		}
		return true
	}
	return func() (int64, []*objects.Job, func() bool) {
		if currIndex >= len(jobs) {
			return math.MaxInt64, nil, func() bool {
				return false
			}
		}
		submitTime := jobs[currIndex].GetSubmitTimeNanoSecond()
		nextBatchJobs := make([]*objects.Job, 0, 1)
		for i := currIndex; i < len(jobs); i++ {
			if jobs[i].GetSubmitTimeNanoSecond() == submitTime {
				nextBatchJobs = append(nextBatchJobs, jobs[i])
			}
		}
		return submitTime, nextBatchJobs, func() bool {
			return next(len(nextBatchJobs))
		}
	}
}

func loadDLTJobData(filePath string)(map[string]*configs.DLTJobData){
	bytes, err := ioutil.ReadFile(filePath)
	if err!=nil{
		panic(err)
	}
	data := &configs.DLTPredictorDataOrientedDataFormat{}
	err = utils.Unmarshal(string(bytes), data)
	if err!=nil{
		panic(err)
	}
	return data.GetJobID2DLTJobData()
}