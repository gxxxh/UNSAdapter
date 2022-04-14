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

type K8sManager struct {
	clientSet  *kubernetes.Clientset
	namespace  string
	podManager *PodManager
}

func NewK8sManager() *K8sManager {
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
	var k8sAdapterVar K8sManager
	k8sAdapterVar.clientSet, err = kubernetes.NewForConfig(config)

	if err != nil {
		panic(err.Error())
	}
	k8sAdapterVar.initNamespace()
	//init pod controller
	k8sAdapterVar.podManager = NewPodManager(k8sAdapterVar.clientSet, k8sAdapterVar.namespace)
	k8sAdapterVar.podManager.Run(make(chan struct{}))
	return &k8sAdapterVar
}

func (k8sAdapterVar *K8sManager) initNamespace() {
	k8sAdapterVar.namespace = "uns"
	//find namespace
	np := func(name string) bool {
		nps, err := k8sAdapterVar.clientSet.CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{})
		if err != nil {
			panic(err.Error())
		}
		for _, np := range nps.Items {
			if np.Name == name {
				return true
			}
		}
		return false
	}(k8sAdapterVar.namespace)
	//create namespace
	if np == false {
		//create task namespace
		nsSpec := &v1.Namespace{
			TypeMeta:   metav1.TypeMeta{},
			ObjectMeta: metav1.ObjectMeta{Name: "uns"},
			Spec:       v1.NamespaceSpec{},
			Status:     v1.NamespaceStatus{},
		}
		_, err := k8sAdapterVar.clientSet.CoreV1().Namespaces().Create(context.Background(), nsSpec, metav1.CreateOptions{})
		if err != nil {
			panic(err.Error())
		}
	}
}

func (k8sAdapter *K8sManager) GetPodManager() *PodManager {
	return k8sAdapter.podManager
}

func (k8sMg *K8sManager) GetNamespace() string {
	return k8sMg.namespace
}

func (k8sMg *K8sManager) GetClientSet() *kubernetes.Clientset {
	return k8sMg.clientSet
}
