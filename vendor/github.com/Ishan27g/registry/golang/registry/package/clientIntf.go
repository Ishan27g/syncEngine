package _package

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	GetPingMeta() map[string]PingMeta
	ping(address string) (bool, *PingMeta)
}
type registryClient struct {
	serverAddress string
}

func (r *registryClient) ping(address string) (bool, *PingMeta) {
	req, err := http.NewRequest("GET", address+"/engine/ping", nil)
	if err != nil {
		return false, nil
	}
	client := &http.Client{Timeout: time.Second * 1}

	resp, err := client.Do(req)
	if err != nil {
		return false, nil
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("ERROR reading body. " + err.Error())
		return false, nil
	}
	defer resp.Body.Close()
	var pr PingMeta
	err = json.Unmarshal(body, &pr)
	if err != nil {
		fmt.Println("e" + err.Error())
		return false, nil
	}
	return resp.StatusCode == http.StatusOK, &pr
}

func (r *registryClient) details() *map[int]details {
	var rsp map[int]details
	url := r.serverAddress + DetailsUrlJson
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil
	}
	if b := sendReq(req); b != nil {
		err := json.Unmarshal(b, &rsp)
		if err != nil {
			return nil
		}
	}
	return &rsp
}
func (r *registryClient) GetPingMeta() map[string]PingMeta {
	var pm = make(map[string]PingMeta)
	rsp := r.details()
	if rsp == nil {
		return nil
	}
	for _, peers := range *r.details() {
		for s, meta := range peers.Pm {
			pm[s] = meta
		}
	}
	return pm
}

func (r *registryClient) GetDetails() []string {
	rsp := r.details()
	if rsp == nil {
		return nil
	}
	var addrs []string
	for _, peers := range *rsp {
		for _, peer := range peers.Peers {
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
