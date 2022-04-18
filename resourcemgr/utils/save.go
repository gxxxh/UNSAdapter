package utils

import (
	"UNSAdapter/pb_gen/configs"
	"UNSAdapter/pb_gen/objects"
	"encoding/json"
	"fmt"

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

var savePath = "D:\\GolangProjects\\src\\UNSAdapter\\config\\"

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
				StartTime: taskExecutionHistory.StartExecutionTimeNanoSecond-submitTime,
				Duration:  taskExecutionHistory.DurationNanoSecond,
				EndTime:   taskExecutionHistory.StartExecutionTimeNanoSecond + taskExecutionHistory.DurationNanoSecond-submitTime,
				DDLViolated: taskExecutionHistory.DurationNanoSecond > dltJobs[taskExecutionHistory.JobID].Job.Deadline,
			})

			totalRunnintTime += taskExecutionHistory.DurationNanoSecond
			if(taskExecutionHistory.DurationNanoSecond>dltJobs[taskExecutionHistory.JobID].Job.Deadline){
				fmt.Printf("job %s violated dll\n", taskExecutionHistory.JobID)
				ddlViolationNum = ddlViolationNum+1
				totalViolationTime += (taskExecutionHistory.StartExecutionTimeNanoSecond+taskExecutionHistory.DurationNanoSecond-submitTime)
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
