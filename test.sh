#!/usr/bin/env sh



if go test --v ./... -run Test_Round_AtLeader;then
#  go test --v ./... -run Test_Round_AtFollowers
#  go test --v ./... -run Test_Round_AtRandom
#  go test --v ./... -run Test_Multiple_Rounds
#
#  go test --v ./data
#  go test --v ./engine
#  go test --v ./snapshot
#  go test --v ./transport
fi


