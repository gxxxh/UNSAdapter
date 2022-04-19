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
var hydraResultPath = "D:\\GolangProjects\\src\\UNSAdapter\\result\\schedulerTypeHydra_result.json"
var hydraAllocationPath = "D:\\GolangProjects\\src\\UNSAdapter\\result\\schedulerTypeHydra_allocation.json"
var edfFastResultPath = "D:\\GolangProjects\\src\\UNSAdapter\\result\\schedulerTypeEDFFast_result.json"
var edfFastAllocationPath = "D:\\GolangProjects\\src\\UNSAdapter\\result\\schedulerTypeEDFFast_allocation.json"



func GetHydraAllocationPath(c *gin.Context){
	exist, _ := PathExists(hydraAllocationPath)
	if !exist{
		c.String(http.StatusOK, "still scheduling")
	}
	bytes, err := ioutil.ReadFile(hydraAllocationPath)
	if err!=nil{
		errMsg := fmt.Sprintf("no result file, err=%v\n", err)
		c.String(http.StatusNotFound, errMsg)
	}
	c.String(http.StatusOK, string(bytes))
}


func GetEDFFastAllocation(c *gin.Context){
	exist, _ := PathExists(edfFastAllocationPath)
	if !exist{
		c.String(http.StatusOK, "still scheduling")
	}
	bytes, err := ioutil.ReadFile(edfFastAllocationPath)
	if err!=nil{
		errMsg := fmt.Sprintf("no result file, err=%v\n", err)
		c.String(http.StatusNotFound, errMsg)
	}
	c.String(http.StatusOK, string(bytes))
}

func GetHydraResult(c *gin.Context){
	exist, _ := PathExists(hydraResultPath)
	if !exist{
		c.String(http.StatusOK, "still scheduling")
	}
	bytes, err := ioutil.ReadFile(hydraResultPath)
	if err!=nil{
		errMsg := fmt.Sprintf("no result file, err=%v\n", err)
		c.String(http.StatusNotFound, errMsg)
	}
	c.String(http.StatusOK, string(bytes))
}

func GetEDFFastResult(c *gin.Context){
	exist, _ := PathExists(edfFastAllocationPath)
	if !exist{
		c.String(http.StatusOK, "still scheduling")
	}
	bytes, err := ioutil.ReadFile(edfFastResultPath)
	if err!=nil{
		errMsg := fmt.Sprintf("no result file, err=%v\n", err)
		c.String(http.StatusNotFound, errMsg)
	}
	c.String(http.StatusOK, string(bytes))
}


func PostSubmitJobs(c *gin.Context){
	var dataSource = "D:\\GolangProjects\\src\\UNSAdapter\\config\\async_predictor_data.json"
	//delete last time result files
	err := RemoveFiles([]string{hydraResultPath, hydraAllocationPath, edfFastAllocationPath, edfFastResultPath})
	if err!=nil{
		c.String(http.StatusServiceUnavailable, fmt.Sprintf("delete files error, err=[%v]", err))
	}
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