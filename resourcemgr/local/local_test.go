package local

import (
	"UNSAdapter/config"
	"testing"
)

var dataSource = "D:\\GolangProjects\\src\\UNSAdapter\\config\\async_predictor_data.json"

func TestResourceManager(t *testing.T) {
	simulatorConfig := config.ReadSimulatorConfig()
	//simulatorConfig.RmConfiguration.SchedulersConfiguration.GetPartitionID2SchedulerConfiguration()
	rm := NewResourceManager(simulatorConfig)
	go 	rm.jobSimulator.AddJobs(dataSource)
	rm.Run()


}
