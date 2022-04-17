package utils

import (
	"UNSAdapter/pb_gen/objects"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
)

type AcceleratorUsingInfo struct {
	JobID     string `json:"JobID"`
	TaskID    string `json:"TaskID"`
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"EndTime"`
	Duration  int64  `json:"duration"`
}

var savePath = "D:\\GolangProjects\\src\\UNSAdapter\\config\\"

func SaveFinishedJobInfo(schedulerType string, jobExecutionHistories []*objects.JobExecutionHistory, accelerators map[string]string) {
	info := make(map[string][]AcceleratorUsingInfo)
	for ID, _ := range accelerators {
		info[ID] = make([]AcceleratorUsingInfo, 0)
	}
	for _, jobExecutionHistory := range jobExecutionHistories {
		for _, taskExecutionHistory := range jobExecutionHistory.TaskExecutionHistories {
			accID := taskExecutionHistory.AcceleratorAllocation.AcceleratorID
			info[accID] = append(info[accID], AcceleratorUsingInfo{
				JobID:     taskExecutionHistory.JobID,
				TaskID:    taskExecutionHistory.JobID,
				StartTime: taskExecutionHistory.StartExecutionTimeNanoSecond,
				Duration:  taskExecutionHistory.DurationNanoSecond,
				EndTime: taskExecutionHistory.StartExecutionTimeNanoSecond+taskExecutionHistory.DurationNanoSecond,
			})
		}
	}
	infoJson, err := json.Marshal(info)
	if err != nil {
		fmt.Println("SaveFinishedJobInfo:Marshal failed: ", err)
	}
	err = ioutil.WriteFile(filepath.Join(savePath, fmt.Sprintf("%s_result.json", schedulerType)), []byte(string(infoJson)), 0666)
	if err != nil {
		fmt.Println("SaveFinishedJobInfo:Marshal failed: ", err)
	}
}
