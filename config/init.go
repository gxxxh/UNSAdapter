package config

import (
	"github.com/MLSched/UNS/pb_gen/configs"
	"UNSAdapter/utils"
	"io/ioutil"
)


func ReadSimulatorConfig(configPath string) *configs.DLTSimulatorConfiguration {
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
