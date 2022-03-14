package _package

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

type Client interface {
	// Register self at this address/zone with registry
	Register(address string, meta MetaData) PeerResponse
	// GetZoneIds returns the zoneIds
	GetZoneIds() []int
	// GetZonePeers returns the addresses of zone peers
	GetZonePeers(zone int) PeerResponse

	ping(address string) bool
}

func (c *client) Register(address string, meta MetaData) PeerResponse {
	if meta == nil {
		return nil
	}
	type reqJson struct {
		Address  string   `json:"address"`
		MetaData MetaData `json:"metaData"`
	}
	var r reqJson
	r.Address = address
	r.MetaData = meta
	json, err := json.Marshal(r)
	if err != nil {
		return nil
	}
	req, err := http.NewRequest("POST", c.serverAddress+RegUrl, bytes.NewBuffer(json))
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	return parseRegRsp(sendReq(req))
}

func (c *client) GetZoneIds() []int {
	url := c.serverAddress + ZoneIdsUrl
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil
	}
	var rsp []int
	if b := sendReq(req); b != nil {
		err := json.Unmarshal(b, &rsp)
		if err != nil {
			return nil
		}
	}
	return rsp
}

func (c *client) GetZonePeers(zone int) PeerResponse {
	url := c.serverAddress + ZoneUrl + "/" + strconv.Itoa(zone)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil
	}
	return parseRegRsp(sendReq(req))
}

func (c *client) ping(address string) bool {
	//TODO implement me
	panic("implement me")
}

type client registryClient

func NewClient(serverAddress string) Client {
	return &client{serverAddress: serverAddress}
}
