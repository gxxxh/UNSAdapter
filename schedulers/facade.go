package schedulers

import (
	"UNSAdapter/schedulers/interfaces"
	"UNSAdapter/schedulers/service"
)

func InitLocalSchedulersService() {
	service.InitSchedulersService(service.NewLocalSchedulerService())
}

func GetServiceInstance() interfaces.Service {
	return service.GetSchedulersServiceInstance()
}
