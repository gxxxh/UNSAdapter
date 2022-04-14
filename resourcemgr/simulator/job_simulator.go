package simulator

import (
	"UNSAdapter/pb_gen/objects"
	"log"
	"time"
)

//用于产生job任务
var CheckJobSubmitInterval = 1000 * time.Millisecond
type JobSimulator struct {
	startSimulationTime time.Time//job的提交时间是相对于开始模拟的时间的
	jobs []*objects.Job
	newJobChan chan []*objects.Job //保存新产生的Job
}

func NewJobSimulator(jobs []*objects.Job)(*JobSimulator){
	return &JobSimulator{
		startSimulationTime: time.Now(),
		jobs:                jobs,
		newJobChan:          make(chan []*objects.Job, 1024),
	}
}

func (js *JobSimulator) Run(){
	iter := iteratorJobsBySubmitTime(js.jobs)
	hasNext:= true
	var now time.Time
	for hasNext{
		normSubmitTime, batchJobs, hasNextFunc := iter()
		hasNext = hasNextFunc()
		submitTime := js.startSimulationTime.Add(time.Duration(normSubmitTime)*time.Nanosecond)
		for now = time.Now(); submitTime.After(now); now = time.Now(){
			log.Printf("check submit jobs, now %s, newt submit time %s\n", now.String(), submitTime.String())
			time.Sleep(CheckJobSubmitInterval)
		}
		log.Printf("submit jobs, now %s, submit time %s\n",now.String(), submitTime.String() )
		js.newJobChan <- batchJobs
	}
	close(js.newJobChan)
}

func (js *JobSimulator)GetNewJob()([]*objects.Job, bool){
	newJob, isClose := <- js.newJobChan

	return newJob, isClose
}

