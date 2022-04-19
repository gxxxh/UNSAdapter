package k8s_manager

import (
	"testing"
)

func TestDeployment_manager_StartDeployment(t *testing.T) {
	k8sManager := NewK8sManager()
	go k8sManager.Run()
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

}