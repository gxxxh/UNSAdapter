package resourcemgr

import "UNSAdapter/events"

type Interface interface{
	GetResourceManagerID() string
	events.EventHandler
}
