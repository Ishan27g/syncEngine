package peer

type State struct {
	Self       Peer `json:"Self"`
	RaftLeader Peer `json:"RaftLeader"`
	SyncLeader Peer `json:"SyncLeader"`
}

func GetState(self, raft, sync Peer) *State {
	return &State{
		Self:       self,
		RaftLeader: raft,
		SyncLeader: sync,
	}
}
