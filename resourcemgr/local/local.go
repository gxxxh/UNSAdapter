package local

import "UNSAdapter/pb_gen/objects"
type ResourceManager struct{
	RMID string
	clusterInfo *objects.Cluster//一个rm对应一个cluster
}

func NewResourceManager() *ResourceManager{

}

func (rm *ResourceManager) GetResourceManagerID() string{
	return rm.RMID
}

func(rm *ResourceManager) InitClusterInfo(){

}

