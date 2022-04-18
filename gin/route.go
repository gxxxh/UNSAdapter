package gin

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"net/http"
)

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

}