package util


const (
	EVENT_JOIN int = 0
	EVENT_OFFLINE int = 1
)

// channel for notifying leader election service
var LeaderElectionMembershipEventChan = make(chan *MembershipListEvent, MEMBERSHIP_LIST_EVENT_CHANEL_SIZE)

// channel for notifying Maple Juice Job Manager
var MRJobManagerMembershipEventChan = make(chan *MembershipListEvent, MEMBERSHIP_LIST_EVENT_CHANEL_SIZE)

type MembershipListEvent struct {
	eventType int
	NodeId string
}


func (this *MembershipListEvent) IsNewJoin() bool {
	return this.eventType == EVENT_JOIN;
}

func (this *MembershipListEvent) IsOffline() bool {
	return this.eventType == EVENT_OFFLINE;
}

func NotifyOffline(e *MemberListEntry){
	event := &MembershipListEvent{
		eventType: EVENT_OFFLINE,
		NodeId: e.NodeId(),
	}
	LeaderElectionMembershipEventChan <- event
	MRJobManagerMembershipEventChan <- event
}

func NotifyJoin(e *MemberListEntry){
	event := &MembershipListEvent{
		eventType: EVENT_JOIN,
		NodeId: e.NodeId(),
	}
	LeaderElectionMembershipEventChan <- event
	MRJobManagerMembershipEventChan <- event
}