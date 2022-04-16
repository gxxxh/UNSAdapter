package config

import (
	"UNSAdapter/pb_gen/configs"
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
