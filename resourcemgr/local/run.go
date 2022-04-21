package local

import "UNSAdapter/config"
//withPod， 不启动pod为false, 启动pod为true
func RMRun(configPath string, predictorDataPath string, withPod bool){
	simulatroConfig := config.ReadSimulatorConfig(configPath)
	rm := NewResourceManager(simulatroConfig, withPod)
	go rm.jobSimulator.AddJobs(predictorDataPath)
	rm.Run()
}

