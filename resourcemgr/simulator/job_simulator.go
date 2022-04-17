package simulator

import (
	"UNSAdapter/pb_gen/configs"
	"UNSAdapter/pb_gen/objects"
	"fmt"
	"log"
	"time"
)

//用于产生job任务
var CheckJobSubmitInterval = 1000 * time.Millisecond
type JobSimulator struct {
	startSimulationTime time.Time//job的提交时间是相对于开始模拟的时间的
	jobID2DLTData map[string]*configs.DLTJobData
	//jobs []*objects.Job
	newJobChan chan []*objects.Job //保存新产生的Job
}

func NewJobSimulator()(*JobSimulator){

	return &JobSimulator{
		startSimulationTime: time.Now(),
		jobID2DLTData:              make(map[string]*configs.DLTJobData,0),
		newJobChan:          make(chan []*objects.Job, 1024),
	}
}

func (js *JobSimulator)AddJobs(filePath string){
	fmt.Printf("Job simulator add jobs...")
	js.jobID2DLTData = loadDLTJobData(filePath)
	jobs := js.GetJobs()
	//reset submit time
	now := time.Now()
	//minTime := int64(now.UnixNano())
	//for _, job := range(jobs){
	//	if(job.SubmitTimeNanoSecond<minTime){
	//		minTime = job.SubmitTimeNanoSecond
	//	}
	//}
	for _, job := range(jobs){
	//	job.SubmitTimeNanoSecond = int64(now.UnixNano())+(job.SubmitTimeNanoSecond-minTime)
		job.SubmitTimeNanoSecond = now.UnixNano()
	}
	js.newJobChan <- js.GetJobs()
	close(js.newJobChan)
}


func (js *JobSimulator)GetJobs()[]*objects.Job{
	jobs := make([]*objects.Job, 0, len(js.jobID2DLTData))
	for _, DLTJob := range js.jobID2DLTData{
		jobs = append(jobs, DLTJob.GetJob())
	}
	return jobs
}
func (js *JobSimulator)GetRuningTime(jobID string, acceleratorType string)int64{
	DLTJob := js.jobID2DLTData[jobID]
 	runTime := DLTJob.GetAcceleratorType2MiniBatchDuration().GetAccType2Duration()[acceleratorType]
	 return runTime*DLTJob.GetTotalMiniBatches()
}

func (js *JobSimulator) Run(){
	iter := iteratorJobsBySubmitTime(js.GetJobs())
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
	for _, job := range newJob{
		fmt.Printf("Accept new job: %v\n", job)
	}
	return newJob, isClose
}

