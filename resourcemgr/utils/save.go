package utils

import (
	"UNSAdapter/utils"
	"encoding/json"
	"fmt"
	"github.com/MLSched/UNS/pb_gen/configs"
	"github.com/MLSched/UNS/pb_gen/objects"
	"sort"

	"io/ioutil"
	"path/filepath"
	"strconv"
)

type AcceleratorUsingInfo struct {
	JobID     string `json:"JobID"`
	TaskID    string `json:"TaskID"`
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"EndTime"`
	Duration  int64  `json:"duration"`
	DDLViolated bool `json:"DDLViolated"`
}
type ScheduleResult struct {
	SchedulerType         string  `json:"SchedulerType"`
	TotalRunningTime      string  `json:"TotalRunningTime"`
	TotalJobNum				int `json:"TotalJobNum"`
	DDLViolation          int  `json:"DDLViolationJobNums"`
	TotalViolationTime    int64 `json:"TotalViolationTime"`
	AcceleratorUsingInfos map[string][]AcceleratorUsingInfo
}

var savePath = "D:\\GolangProjects\\src\\UNSAdapter\\result\\"
func SaveJobAllocations(schedulerType string, jobAllocations []*objects.JobAllocation, dltJobs map[string]*configs.DLTJobData, accelerators map[string]string,submitTime int64){

	info := make(map[string][]AcceleratorUsingInfo)
	for ID, _ := range accelerators {
		info[ID] = make([]AcceleratorUsingInfo, 0)
	}
	var totalRunnintTime int64
	totalRunnintTime = 0
	ddlViolationNum := 0
	var totalViolationTime int64
	totalViolationTime = 0
	for _, jobAlloc := range(jobAllocations){
		for _, taskAlloc := range(jobAlloc.TaskAllocations){
			acceleratorType :=accelerators[taskAlloc.AcceleratorAllocation.AcceleratorID]
			duration := dltJobs[taskAlloc.JobID].GetAcceleratorType2MiniBatchDuration().GetAccType2Duration()[acceleratorType]
			accID := taskAlloc.AcceleratorAllocation.AcceleratorID
			info[accID] = append(info[accID], AcceleratorUsingInfo{
				JobID:       taskAlloc.JobID,
				TaskID:      taskAlloc.TaskID,
				StartTime:   taskAlloc.StartExecutionTimeNanoSecond.Value,
				EndTime:     taskAlloc.StartExecutionTimeNanoSecond.Value+duration,
				Duration:    duration,
				DDLViolated: taskAlloc.StartExecutionTimeNanoSecond.Value+duration>dltJobs[taskAlloc.JobID].Job.Deadline,
			})
			totalRunnintTime += duration
			if(duration>dltJobs[taskAlloc.JobID].Job.Deadline){
				fmt.Printf("job %s violated ddl\n", jobAlloc.JobID)
				ddlViolationNum += 1
				totalViolationTime += (duration-dltJobs[taskAlloc.JobID].Job.Deadline)
			}
		}
	}
	for accID,_:= range accelerators{
		usingInfos := info[accID]
		sorter := utils.Sorter{
			LenFunc:  func() int {
				return len(usingInfos)
			},
			LessFunc:func(i,j int)bool{
				return usingInfos[i].StartTime<usingInfos[j].StartTime
			},
			SwapFunc: func(i, j int){
				t := usingInfos[i]
				usingInfos[i] = usingInfos[j]
				usingInfos[j] = t
			},
		}
		sort.Sort(sorter)
	}

	result := ScheduleResult{
		SchedulerType:         schedulerType,
		TotalRunningTime: strconv.FormatInt(totalRunnintTime, 10),
		TotalJobNum: len(jobAllocations),
		DDLViolation:         ddlViolationNum,
		TotalViolationTime: totalRunnintTime,
		AcceleratorUsingInfos: info,
	}
	infoJson, err := json.Marshal(result)
	if err != nil {
		fmt.Println("SaveFinishedJobInfo:Marshal failed: ", err)
	}
	err = ioutil.WriteFile(filepath.Join(savePath, fmt.Sprintf("%s_allocation.json", schedulerType)), []byte(string(infoJson)), 0666)
	if err != nil {
		fmt.Println("SaveFinishedJobInfo:Marshal failed: ", err)
	}
}

func SaveFinishedJobInfo(schedulerType string, jobExecutionHistories []*objects.JobExecutionHistory,dltJobs map[string]*configs.DLTJobData, accelerators map[string]string, submitTime int64) {
	info := make(map[string][]AcceleratorUsingInfo)
	for ID, _ := range accelerators {
		info[ID] = make([]AcceleratorUsingInfo, 0)
	}
	var totalRunnintTime int64
	totalRunnintTime = 0
	ddlViolationNum := 0
	var totalViolationTime int64
	totalViolationTime = 0
	for _, jobExecutionHistory := range jobExecutionHistories {
		for _, taskExecutionHistory := range jobExecutionHistory.TaskExecutionHistories {
			accID := taskExecutionHistory.AcceleratorAllocation.AcceleratorID
			info[accID] = append(info[accID], AcceleratorUsingInfo{
				JobID:     taskExecutionHistory.JobID,
				TaskID:    taskExecutionHistory.JobID,
				StartTime: taskExecutionHistory.StartExecutionTimeNanoSecond,
				Duration:  taskExecutionHistory.DurationNanoSecond,
				EndTime:   taskExecutionHistory.StartExecutionTimeNanoSecond + taskExecutionHistory.DurationNanoSecond,
				DDLViolated: taskExecutionHistory.DurationNanoSecond +taskExecutionHistory.StartExecutionTimeNanoSecond> dltJobs[taskExecutionHistory.JobID].Job.Deadline,
			})

			totalRunnintTime += taskExecutionHistory.DurationNanoSecond
			if(taskExecutionHistory.DurationNanoSecond>dltJobs[taskExecutionHistory.JobID].Job.Deadline){
				fmt.Printf("job %s violated dll\n", taskExecutionHistory.JobID)
				ddlViolationNum = ddlViolationNum+1
				totalViolationTime += (taskExecutionHistory.DurationNanoSecond-dltJobs[taskExecutionHistory.JobID].Job.Deadline)
			}
		}
	}
	result := ScheduleResult{
		SchedulerType:         schedulerType,
		TotalRunningTime: strconv.FormatInt(totalRunnintTime, 10),
		TotalJobNum: len(jobExecutionHistories),
		DDLViolation:         ddlViolationNum,
		TotalViolationTime: totalRunnintTime,
		AcceleratorUsingInfos: info,
	}
	infoJson, err := json.Marshal(result)
	if err != nil {
		fmt.Println("SaveFinishedJobInfo:Marshal failed: ", err)
	}
	err = ioutil.WriteFile(filepath.Join(savePath, fmt.Sprintf("%s_result.json", schedulerType)), []byte(string(infoJson)), 0666)
	if err != nil {
		fmt.Println("SaveFinishedJobInfo:Marshal failed: ", err)
	}
}
