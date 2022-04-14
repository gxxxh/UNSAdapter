package k8s_manager

import (
	"log"
	"testing"
)

func TestDeployment_manager_StartDeployment(t *testing.T) {
	//Initk8sAdapter()
	//var dm Deployment_manager
	//err := dm.StartDeployment(map[string]string{
	//	"name":"test-dp",
	//	"namespace": "default",
	//	"nodeID": "1",
	//	"jobID": "1",
	//	"taskID": "1",
	//	"sleepTime": "1000000000",
	//})
	//if err!=nil{
	//	t.Errorf("%v", err)
	//}
	c := make (chan int, 10)
	c <- 1
	c <- 2
	c <- 3
	close(c)

	for{
		i, isClose := <-c
		if !isClose{
			log.Println(i, isClose)
			break
		}
		log.Println(i, isClose)
	}
}