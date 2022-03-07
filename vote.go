package main

import (
	"context"

	"github.com/Ishan27g/syncEngine/peer"
	"github.com/Ishan27g/syncEngine/proto"
)

type voteManager struct {
	self  func() *peer.Peer
	voted chan peer.Peer
}

func (v *voteManager) RequestVotes(ctx context.Context, term *proto.Term) (*proto.Vote, error) {
	vote := &proto.Vote{Elected: false}
	if term.TermCount == -100 { // sync leader election
		v.voted <- peer.Peer{
			Zone:     int(term.Zone),
			HostName: term.LeaderHostname,
			HttpPort: term.LeaderHttpPort,
			GrpcPort: term.LeaderGrpcPort,
			UdpPort:  term.LeaderUdpPort,
			Mode:     term.Mode,
			RaftTerm: -1,
			SyncTerm: int(term.TermCount),
		}
		vote.Elected = true
	} else {
		if term.TermCount > int32(v.self().RaftTerm) {
			v.voted <- peer.Peer{
				Zone:     int(term.Zone),
				HostName: term.LeaderHostname,
				HttpPort: term.LeaderHttpPort,
				GrpcPort: term.LeaderGrpcPort,
				UdpPort:  term.LeaderUdpPort,
				Mode:     term.Mode,
				RaftTerm: int(term.TermCount),
				SyncTerm: -1,
			}
			vote.Elected = true
		}
	}

	return vote, nil
}
