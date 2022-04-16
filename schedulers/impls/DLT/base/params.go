package base

import (
	"UNSAdapter/events"
	"UNSAdapter/pb_gen/configs"
	"UNSAdapter/schedulers/partition"
)

type EventPusher func(SchedulerID string, event *events.Event)
type PartitionContextAware func() *partition.Context
type SchedulerBuildParams struct {
	SchedulerConfiguration *configs.SchedulerConfiguration
	EventPusher            EventPusher
	PartitionContextAware  PartitionContextAware
}
