package config

import (
	"UNSAdapter/pb_gen/configs"
	"UNSAdapter/resourcemgr/k8s_manager"
	"UNSAdapter/resourcemgr/local"
	"UNSAdapter/utils"
	"io/ioutil"
)

var configPath = "D:\\GolangProjects\\src\\UNSAdapter\\config\\async_simulator_configuration.json"

func ReadSimulatorConfig() *configs.DLTSimulatorConfiguration {
	config := &configs.DLTSimulatorConfiguration{}

	bytes, err := ioutil.ReadFile(configPath)
	if err != nil {
		panic(err)
	}
	err = utils.Unmarshal(string(bytes), config)
	if err != nil {
		panic(err)
	}
	return config
}

func InitResourceManager() *local.ResourceManager {
	simulatorConfig := ReadSimulatorConfig()
	k8sManager := k8s_manager.NewK8sManager()
	rm := local.NewResourceManager(simulatorConfig.ResourceManagerID, k8sManager)
	rm.BuildClusterManager("cluster-ID", simulatorConfig.GetRmConfiguration().GetCluster())
	return rm
}
