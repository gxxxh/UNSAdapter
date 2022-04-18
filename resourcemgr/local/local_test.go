package local

import (
	"UNSAdapter/config"
	"testing"
)

var dataSource = "D:\\GolangProjects\\src\\UNSAdapter\\config\\async_predictor_data.json"
var configPath = "D:\\GolangProjects\\src\\UNSAdapter\\config\\async_simulator_configuration.json"
//var configPath = "D:\\GolangProjects\\src\\UNSAdapter\\config\\async_simulator_configuration_edfFast.json"

func TestResourceManager(t *testing.T) {
	simulatorConfig := config.ReadSimulatorConfig(configPath)
	//simulatorConfig.RmConfiguration.SchedulersConfiguration.GetPartitionID2SchedulerConfiguration()
	rm := NewResourceManager(simulatorConfig)
	go 	rm.jobSimulator.AddJobs(dataSource)
	rm.Run()
}
