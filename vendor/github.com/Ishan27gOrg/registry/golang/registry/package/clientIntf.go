package _package

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type RegisterRequest struct {
	RegisterAt time.Time `json:"registered_at"`
	Address    string    `json:"address"` // full Address `
	Zone       int       `json:"zone"`    // todo
	MetaData   MetaData  `json:"meta_data"`
}
type PeerResponse []RegisterRequest

func (pr *PeerResponse) GetPeerAddr(exclude string) []string {
	var p []string
	for _, p2 := range *pr {
		if exclude != "" && strings.Contains(exclude, p2.Address) {
			continue
		}
		p = append(p, p2.Address)
	}
	return p
}
func (pr *PeerResponse) GetPeerMeta() []MetaData {
	var p []MetaData
	for _, p2 := range *pr {
		p = append(p, p2.MetaData)
	}
	return p
}

type RegistryClientI interface {
	// Register self at this address/zone with registry
	Register(zone int, address string, meta MetaData) PeerResponse
	// GetZoneIds returns the zoneIds
	GetZoneIds() []int
	// GetZonePeers returns the addresses of zone peers
	GetZonePeers(zone int) PeerResponse
	// GetDetails returns all registered peers details
	GetDetails() []string

	ping(address string) bool
}
type registryClient struct {
	serverAddress string
}

func (r *registryClient) ping(address string) bool {
	req, err := http.NewRequest("GET", address+"/engine/ping", nil)
	if err != nil {
		return false
	}
	client := &http.Client{Timeout: time.Second * 1}

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	return resp.StatusCode == http.StatusOK
}
func (r *registryClient) GetDetails() []string {
	url := r.serverAddress + DetailsUrlJson
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil
	}
	var rsp map[int]peers
	var addrs []string
	if b := sendReq(req); b != nil {
		err := json.Unmarshal(b, &rsp)
		if err != nil {
			return nil
		}
	}
	for _, peers := range rsp {
		for _, peer := range peers {
			addrs = append(addrs, peer.MetaData.(string))
		}
	}
	return addrs
}
func (r *registryClient) GetZoneIds() []int {
	url := r.serverAddress + ZoneIdsUrl
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil
	}
	var rsp map[string][]int
	if b := sendReq(req); b != nil {
		err := json.Unmarshal(b, &rsp)
		if err != nil {
			return nil
		}
	}
	return rsp["zoneIds"]
}

func (r *registryClient) Register(zone int, address string, meta MetaData) PeerResponse {
	body := registerReqBody(newPeer(address, zone, meta))
	req, err := http.NewRequest("POST", r.serverAddress+RegUrl, bytes.NewBuffer(body))
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	return parseRegRsp(sendReq(req))
}

func (r registryClient) GetZonePeers(zone int) PeerResponse {
	url := r.serverAddress + ZoneUrl + "?id=" + strconv.Itoa(zone)
	//url := r.serverAddress + ZoneUrl + "/" + strconv.Itoa(zone)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil
	}
	return parseRegRsp(sendReq(req))
}

func RegistryClient(serverAddress string) RegistryClientI {
	return &registryClient{serverAddress: serverAddress}
}
