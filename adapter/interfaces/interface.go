package interfaces
import (
	"UNSAdapter/events"
	eventsobjs "UNSAdapter/pb_gen/events"
	"UNSAdapter/resourcemgr/interfaces"
)
//可以删除，不需要
type Service interface {
	StartService()
	RegisterRM(event *eventsobjs.RMRegisterResourceManagerEvent, resourceMgr interfaces.RMinterface) *events.Result
	Push(rmID string, partitionID string, event *events.Event)
}
