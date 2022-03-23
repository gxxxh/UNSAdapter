package interfaces

import "UNSAdapter/events"
//resource manager service
//一个资源管理器管理多个cluster，负责和scheduler进行通信。
type RMinterface interface{
	GetResourceManagerID() string
	events.EventHandler
}