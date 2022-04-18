package gin

import "github.com/gin-gonic/gin"

func initGin()*gin.Engine{

	r := gin.Default()
	rg := r.Group("api/v1/uns_scheduler")
	rg.GET("/hydra_result", GetHydraResult)
	rg.GET("/edffast_result",GetEDFFastResult)
	rg.POST("/submit_jobs", PostSubmitJobs)
	//运行任务结果
	//rg.POST()
	return r
}