package k8s_manager

import (
	"context"
	"flag"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"path/filepath"
)

type K8sAdapter struct {
	clientSet     *kubernetes.Clientset
	namespace     string
	podController *PodManager
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
	k8sAdapter.podController = NewPodManager(k8sAdapter.clientSet, k8sAdapter.namespace)
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
