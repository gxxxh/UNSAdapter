package events

type EventHandler interface {
	HandleEvent(*Event)
}

type Event struct{
	Data interface{}
	ResultChan chan *Result
	Replied bool
}

type Result struct {
	Succeeded bool
	Reason string
}

func Push(rmID string, partitionID string, event *Event){

}

func Reply(event *Event, result *Result){
	if event.Replied{
		panic("event reply was called twice")
	}
	if event.ResultChan !=nil{
		event.ResultChan <- result
	}
}

func ReplySucceeded(event *Event){
	Reply(event, &Result{
		Succeeded: true,
	})
}