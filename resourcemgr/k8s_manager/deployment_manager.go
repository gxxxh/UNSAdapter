package k8s_manager

type Deployment_manager struct {

}
////todo deployment总是在重启，没法监测
//func (m *Deployment_manager)StartDeployment(info map[string]string)error{
//	dp := appsv1.Deployment{
//		TypeMeta: metav1.TypeMeta{
//			Kind:       "Deployment",
//			APIVersion: "v1",
//		},
//		ObjectMeta: metav1.ObjectMeta{
//			Name:      info["name"],
//			Namespace: info["namespace"],
//			//Labels:                     nil,
//			Annotations: map[string]string{
//				"nodeID": info["nodeID"],
//				"jobID":  info["jobID"],
//				"taskID": info["taskID"],
//			},
//		},
//		Spec: appsv1.DeploymentSpec{
//			Replicas: pointer.Int32Ptr(1),
//			Selector: &metav1.LabelSelector{
//				MatchLabels: map[string]string{
//					"id": info["jobID"] + "." + info["taskID"],
//				},
//			},
//			Template: apiv1.PodTemplateSpec{
//				ObjectMeta: metav1.ObjectMeta{
//					Labels: map[string]string{
//						"id": info["jobID"] + "." + info["taskID"],
//					},
//				},
//				Spec: apiv1.PodSpec{
//					Containers: []apiv1.Container{
//						apiv1.Container{
//							Name:  "test-container",
//							Image: "sleeptask:latest",
//							Env: []apiv1.EnvVar{
//								{
//									Name:  "SLEEP_TIME",
//									Value: info["sleepTime"],
//								},
//							},
//
//							ImagePullPolicy: "Never",
//						},
//					},
//				},
//			},
//		},
//	}
//	resp, err := k8sAdapterVar.clientSet.AppsV1().Deployments(info["namespace"]).
//		Create(context.Background(), &dp, metav1.CreateOptions{})
//	if(err!=nil){
//		log.Println("Create Deployment Error, ",err)
//		return err
//	}
//	log.Printf("Start Task %s from Job %s on Node %s, Deployment status: %v \n",info["taskID"], info["jobID"], info["nodeID"], resp.Status)
//	return nil
//}
//func (m *Deployment_manager) AddContainer(dp *appsv1.Deployment) {
//
//}
