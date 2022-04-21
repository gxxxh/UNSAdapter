package local

import (
	"testing"
)

var predictorDataPath = "D:\\GolangProjects\\src\\UNSAdapter\\config\\async_predictor_data.json"
var configPathHydra = "D:\\GolangProjects\\src\\UNSAdapter\\config\\async_simulator_configuration.json"
var configPathEdf = "D:\\GolangProjects\\src\\UNSAdapter\\config\\async_simulator_configuration_edfFast.json"

func TestResourceManager(t *testing.T) {
	RMRun(configPathEdf, predictorDataPath, false)
	RMRun(configPathHydra,predictorDataPath, true)
}
