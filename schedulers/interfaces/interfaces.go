package interfaces

import (
	"UNSAdapter/events"
	eventsobjs "UNSAdapter/pb_gen/events"
	"UNSAdapter/resourcemgr"
)

type Scheduler interface {
	GetSchedulerID() string
	StartService()
	events.EventHandler
}

type Service interface {
	StartService()
	RegisterRM(event *eventsobjs.RMRegisterResourceManagerEvent, resourceMgr resourcemgr.Interface) *events.Result
	Push(rmID string, partitionID string, event *events.Event)
}
