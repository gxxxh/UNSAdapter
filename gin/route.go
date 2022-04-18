package gin

import (
	"UNSAdapter/config"
	"UNSAdapter/resourcemgr/local"
	"fmt"
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"net/http"
)
var hydraConfigPath = "D:\\GolangProjects\\src\\UNSAdapter\\config\\async_simulator_configuration.json"
var edfFastConfigPath  = "D:\\GolangProjects\\src\\UNSAdapter\\config\\async_simulator_configuration_edfFast.json"
var hydraResultPath = "D:\\GolangProjects\\src\\UNSAdapter\\config\\schedulerTypeHydra_result.json"
var edfFastResultPath = "D:\\GolangProjects\\src\\UNSAdapter\\config\\schedulerTypeEDFFast_result.json"
func GetHydraResult(c *gin.Context){
	bytes, err := ioutil.ReadFile(hydraResultPath)
	if err!=nil{
		errMsg := fmt.Sprintf("no result file, err=%v\n", err)
		c.String(http.StatusNotFound, errMsg)
	}
	c.String(http.StatusOK, string(bytes))
}

func GetEDFFastResult(c *gin.Context){
	bytes, err := ioutil.ReadFile(edfFastResultPath)
	if err!=nil{
		errMsg := fmt.Sprintf("no result file, err=%v\n", err)
		c.String(http.StatusNotFound, errMsg)
	}
	c.String(http.StatusOK, string(bytes))
}


func PostSubmitJobs(c *gin.Context){
	var dataSource = "D:\\GolangProjects\\src\\UNSAdapter\\config\\async_predictor_data.json"
	//hydra run
	hydraConfig := config.ReadSimulatorConfig(hydraConfigPath)
	rm := local.NewResourceManager(hydraConfig)
	js := rm.GetJobSimulator()
	go js.AddJobs(dataSource)
	rm.Run()
	//edfFast run
	edfFastConfig := config.ReadSimulatorConfig(edfFastConfigPath)
	rm_edf := local.NewResourceManager(edfFastConfig)
	js_edf := rm.GetJobSimulator()
	go js_edf.AddJobs(dataSource)
	rm_edf.Run()
}