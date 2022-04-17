package utils

import (
	"encoding/json"
	"io/ioutil"
	"testing"
)

var testPath = "D:\\GolangProjects\\src\\UNSAdapter\\config\\schedulerTypeHydra_result.json"
func TestSaveFinishedJobInfo(t *testing.T) {
	info := make(map[string][]AcceleratorUsingInfo)
	tmp := AcceleratorUsingInfo{
		JobID:     "testJob",
		TaskID:    "testTask",
		StartTime: 1,
		Duration:  2,
	}
	info["testGPU"] = []AcceleratorUsingInfo{
		tmp,
	}
	infoJson, err := json.Marshal(tmp)
	if err!=nil{
		t.Error(err)
	}
	err = ioutil.WriteFile(testPath, infoJson, 0666)
	if err!=nil{
		t.Error(err)
	}
}