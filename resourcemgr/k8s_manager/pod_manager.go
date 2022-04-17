package k8s_manager

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	coreinformer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"strconv"
	"strings"
	"time"
)

//https://github.com/feiskyer/kubernetes-handbook/blob/master/examples/client/informer/informer.go
//https://pkg.go.dev/k8s.io/client-go/informers
type PodManager struct {
	clientSet *kubernetes.Clientset
	informerFactory informers.SharedInformerFactory
	podInformer     coreinformer.PodInformer
	podDeleteChan   chan map[string]string//delete pod info
	//resourceManager *local.ResourceManager
}

func NewPodManager(clientset *kubernetes.Clientset, namesapce string) *PodManager {
	informerFactory := informers.NewSharedInformerFactoryWithOptions(clientset, time.Hour*24, informers.WithNamespace(namesapce)) //todo: resynctime ?
	podInfomer := informerFactory.Core().V1().Pods()
	c := &PodManager{
		clientSet: clientset,
		informerFactory: informerFactory,
		podInformer:     podInfomer,
		podDeleteChan: make(chan map[string]string, 1024),//todo chan size
	}
	podInfomer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    c.handlePodAddEvent,
			UpdateFunc: c.handlePodUpdateEvent,
			DeleteFunc: c.handlePodDeleteEvent,
		},
	)
	return c
}


func (c *PodManager) Run(stopCh chan struct{}) error {
	//start all infomer created by informerfactory
	c.informerFactory.Start(stopCh) //run in background
	//wait for the initial synchronization of the local cache
	if !cache.WaitForCacheSync(stopCh, c.podInformer.Informer().HasSynced) {
		return fmt.Errorf("failed to sync")
	}
	return nil
}

func (c *PodManager) handlePodAddEvent(obj interface{}) {
	pod := obj.(*v1.Pod)
	fmt.Println("PodManager: pod created:", pod.Name)
}

func (c *PodManager) handlePodUpdateEvent(old, new interface{}) {
	oldPod := old.(*v1.Pod)
	newPod := new.(*v1.Pod)
	//fmt.Printf("PodManager: pod updated:Name: %v, PodPhase: %v", oldPod.Name, newPod.Status.Phase, newPod.Status)
	//fmt.Printf("old Pod: %v", oldPod.Status.Phase)
	//fmt.Printf("new Pod: %v", newPod.Status.Phase)

	if newPod.Status.Phase == v1.PodSucceeded && oldPod.Status.Phase == v1.PodSucceeded {
		//delete pod
		fmt.Printf("handlePodUpdateEvent: pod %s finished\n", newPod.GetName())
		annotations := newPod.GetAnnotations()
		annotations["finishTime"] = strconv.FormatInt(time.Now().UnixNano(), 10)
		c.DeletePod(newPod.GetName(), newPod.GetNamespace(), annotations)
	}
}

func (c *PodManager) handlePodDeleteEvent(obj interface{}) {
	pod := obj.(*v1.Pod)
	fmt.Printf("handlePodDeleteEvent: pod %v deleted successfully\n", pod.Name)
}

func (pc *PodManager) StartPod(info map[string]string) error{
	//pod spec

	pdSpec := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:     strings.ToLower(info["jobID"] + "-" + strings.Replace(info["taskID"], "_", "-", -1)),
			Namespace: info["namespace"],
			Labels:    nil,
			Annotations: map[string]string{
				"nodeID": info["nodeID"],
				"jobID":  info["jobID"],
				"taskID": info["taskID"],
			},
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
				Value: info["sleepTime"],
			},
		},
		ImagePullPolicy: "Never",
	})
	resp, err := pc.clientSet.CoreV1().Pods(pdSpec.Namespace).
		Create(context.Background(), pdSpec, metav1.CreateOptions{})
	if err != nil {
		fmt.Println("create pod error, err=", err)
		return err
	}
	fmt.Printf("Start Task %s from Job %s on Node %s,sleepTime %s,  Pod status: %s \n",
		info["taskID"], info["jobID"], info["nodeID"],info["sleepTime"] ,resp.Status.Phase)
	return nil
}

func (pc *PodManager) checkPodExist(podName string, namespace string) (bool) {
	_, err := pc.clientSet.CoreV1().Pods(namespace).Get(context.Background(), podName, metav1.GetOptions{})
	if err!=nil{
		return false
	}
	return true
}
func (pc *PodManager) DeletePod(podName string, namespace string, annotations map[string]string) {
	if(pc.checkPodExist(podName, namespace)==false){
		return
	}
	err := pc.clientSet.CoreV1().Pods(namespace).Delete(context.Background(), podName, metav1.DeleteOptions{})
	if err != nil {
		fmt.Println("Delete Pod error, err=", err)
		return
	}
	// 通知resourcemgr处理pod对应内容
	//不指明大小的话，发送者会阻塞到消息被接收。
	//指明大小后，只要当前channel里元素总数不大于这个可缓冲容量，就不会被阻塞
	pc.podDeleteChan<- annotations

}

func (pc *PodManager)GetDeletePodAnnoatations()map[string]string{
	annotations := <- pc.podDeleteChan
	return annotations
}