package main

import (
	"UNSAdapter/config"
	"UNSAdapter/resourcemgr/k8s_manager"
	"UNSAdapter/resourcemgr/local"
	"UNSAdapter/resourcemgr/simulator"
)
func main(){
	simulatorConfig := config.ReadSimulatorConfig()
	//build resource manager from config
	k8sManager := k8s_manager.NewK8sManager()
	rm := local.NewResourceManager(simulatorConfig.GetResourceManagerID(), k8sManager)
	rm.BuildClusterManager("cluster-ID", simulatorConfig.GetRmConfiguration().GetCluster())

	// build job simulator to get job
	js := simulator.NewJobSimulator(simulatorConfig.GetJobs())
	rm.SetJobSimulator(js)
	js.Run()

	//
	for{

	}
}

