package local

import (
	"UNSAdapter/events"
	"UNSAdapter/pb_gen/objects"
)
type ResourceManager struct{
	RMID string
	clusterInfo *objects.Cluster//一个rm对应一个cluster
}

func NewResourceManager() *ResourceManager{
	return nil
}

func (rm *ResourceManager) GetResourceManagerID() string{
	return rm.RMID
}

func(rm *ResourceManager) InitClusterInfo(){

}

func Push(rmID string, partitionID string, event *events.Event){

}