package main

import (
	"UNSAdapter/config"
	"UNSAdapter/resourcemgr/local"
)

func main() {
	simulatorConfig := config.ReadSimulatorConfig()

	rm := local.NewResourceManager(simulatorConfig)
	rm.Run()
	//任务到来
	//
	for {

	}
}
