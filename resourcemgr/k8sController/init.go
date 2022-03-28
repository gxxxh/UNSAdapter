package k8sController

import (
	"context"
	"flag"
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"log"
	"path/filepath"
)

type K8sAdapter struct {
	clientSet     *kubernetes.Clientset
	namespace     string
	podController *PodController
}

var k8sAdapter K8sAdapter

func Initk8sAdapter() {
	//init client set
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}
	k8sAdapter.clientSet, err = kubernetes.NewForConfig(config)

	if err != nil {
		panic(err.Error())
	}
	initNamespace()
	//init pod controller
	k8sAdapter.podController = NewPodController(k8sAdapter.clientSet, k8sAdapter.namespace)
	k8sAdapter.podController.Run(make(chan struct{}))
}

func initNamespace() {
	k8sAdapter.namespace = "uns"
	//find namespace
	np := func(name string) bool {
		nps, err := k8sAdapter.clientSet.CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{})
		if err != nil {
			panic(err.Error())
		}
		for _, np := range nps.Items {
			if np.Name == name {
				return true
			}
		}
		return false
	}(k8sAdapter.namespace)
	//create namespace
	if np == false {
		//create task namespace
		nsSpec := &v1.Namespace{
			TypeMeta:   metav1.TypeMeta{},
			ObjectMeta: metav1.ObjectMeta{Name: "uns"},
			Spec:       v1.NamespaceSpec{},
			Status:     v1.NamespaceStatus{},
		}
		_, err := k8sAdapter.clientSet.CoreV1().Namespaces().Create(context.Background(), nsSpec, metav1.CreateOptions{})
		if err != nil {
			panic(err.Error())
		}
	}

}

func StartPod(timeLength int64) {
	//pod spec
	pdSpec := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pod",
			Namespace:   "uns",
			Labels:      nil,
			Annotations: nil,
		},
		Spec: v1.PodSpec{
			//Volumes:                       nil,
			//InitContainers:                nil,
			Containers:    nil,
			RestartPolicy: "Never",
			NodeName:      "",
		},
		Status: v1.PodStatus{},
	}
	pdSpec.Spec.Containers = append(pdSpec.Spec.Containers, v1.Container{
		Name:  "test-container",
		Image: "sleeptask:latest",
		Env: []v1.EnvVar{
			{
				Name:  "SLEEP_TIME",
				Value: "4000000000",
			},
		},
		ImagePullPolicy: "Never",
	})
	resp, err := k8sAdapter.clientSet.CoreV1().Pods(pdSpec.Namespace).Create(context.Background(), pdSpec, metav1.CreateOptions{})
	if err != nil {
		fmt.Println("create pod error, err=[%v]", err)
	}
	log.Println("status： %v", resp.Status)

}

func deletePod(podName string){
	err := k8sAdapter.clientSet.CoreV1().Pods(k8sAdapter.namespace).Delete(context.Background(), podName, metav1.DeleteOptions{})
	if err!=nil{
		log.Println("Delete Pod error, err=", err)
	}

}
