package k8s_manager

import (
	"UNSAdapter/pb_gen/objects"

	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	coreinformer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"log"
	"time"
)

//https://github.com/feiskyer/kubernetes-handbook/blob/master/examples/client/informer/informer.go
//https://pkg.go.dev/k8s.io/client-go/informers
type PodManager struct {
	informerFactory informers.SharedInformerFactory
	podInformer     coreinformer.PodInformer
	podDeleteChan   chan map[string]string//delete pod info
	//resourceManager *local.ResourceManager
}

func NewPodManager(clientset *kubernetes.Clientset, namesapce string) *PodManager {
	informerFactory := informers.NewSharedInformerFactoryWithOptions(clientset, time.Hour*24, informers.WithNamespace(namesapce)) //todo: resynctime ?
	podInfomer := informerFactory.Core().V1().Pods()
	c := &PodManager{
		informerFactory: informerFactory,
		podInformer:     podInfomer,
		podDeleteChan: make(chan map[string]string),//todo chan size
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
	log.Println("PodManager: pod created:", pod.Name)
}

func (c *PodManager) handlePodUpdateEvent(old, new interface{}) {
	oldPod := old.(*v1.Pod)
	newPod := new.(*v1.Pod)
	//log.Printf("PodManager: pod updated:Name: %v, PodPhase: %v", oldPod.Name, newPod.Status.Phase, newPod.Status)
	log.Printf("old Pod: %v", oldPod.Status.Phase)
	log.Printf("new Pod: %v", newPod.Status.Phase)
	//猜测，只有两次事件，old和new都是succeeded时删除
	if newPod.Status.Phase == v1.PodSucceeded && oldPod.Status.Phase == v1.PodSucceeded {
		//delete pod
		c.DeletePod(newPod)
	}
	//check status delete pod
}

func (c *PodManager) handlePodDeleteEvent(obj interface{}) {
	pod := obj.(*v1.Pod)
	log.Printf("PodManager: pod %v deleted\n", pod.Name)
}

func (pc *PodManager) StartPod(taskAllocation *objects.TaskAllocation) {
	//pod spec
	pdSpec := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskAllocation.NodeID + "-" + taskAllocation.JobID + "-" + taskAllocation.TaskID,
			Namespace: k8sAdapter.namespace,
			Labels:    nil,
			Annotations: map[string]string{
				"nodeID": taskAllocation.NodeID,
				"jobID":  taskAllocation.JobID,
				"taskID": taskAllocation.TaskID,
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
				Value: string(taskAllocation.AllocationTimeNanoSecond), //todo 运行时间多少?
			},
		},
		ImagePullPolicy: "Never",
	})
	resp, err := k8sAdapter.clientSet.CoreV1().Pods(pdSpec.Namespace).
		Create(context.Background(), pdSpec, metav1.CreateOptions{})
	if err != nil {
		fmt.Println("create pod error, err=[%v]", err)
	}
	log.Printf("Start Task %s from Job %s on Node %s, Pod status: %s \n",
		taskAllocation.TaskID, taskAllocation.JobID, taskAllocation.NodeID, resp.Status.Phase)

}

func (pc *PodManager) PodExist(pod *v1.Pod) bool {
	k8sAdapter.clientSet.CoreV1().Pods(k8sAdapter.namespace).Get(context.Background(), pod.GetName(), metav1.GetOptions{})
	return true
}
func (pc *PodManager) DeletePod(pod *v1.Pod) {
	//todo check exist
	err := k8sAdapter.clientSet.CoreV1().Pods(k8sAdapter.namespace).Delete(context.Background(), pod.GetName(), metav1.DeleteOptions{})
	if err != nil {
		log.Println("Delete Pod error, err=", err)
	}
	//todo  如何通知resourceManager处理任务结束
	//pc.resourceManager.HandleTaskFinish(pod.Annotations)
	go func(){
		pc.podDeleteChan<-pod.Annotations//todo go func for this or for delete pod
	}()
}
