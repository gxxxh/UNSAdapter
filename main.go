package main

import "UNSAdapter/resourcemgr/k8s_manager"

func main(){
	//config.InitConfig()
	//grpc start run
	k8s_manager.Initk8sAdapter()
	k8s_manager.StartPod(1)
	for{

	}
}

