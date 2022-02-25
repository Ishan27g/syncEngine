package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"time"

	"github.com/Ishan27g/go-utils/mLogger"
	"github.com/Ishan27g/syncEngine/peer"
	"github.com/hashicorp/go-hclog"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"
)

var baseUrl = "/engine"

var logger = mLogger.Get("http-client")

type traceData interface{}
type HttpClient struct {
	hclog.Logger
	tr trace.Tracer
}

func NewHttpClient(id string, tr trace.Tracer) HttpClient {
	return HttpClient{
		Logger: mLogger.Get("http-client" + id),
		tr:     tr,
	}
}
func stringJson(js interface{}) string {
	data, err := json.MarshalIndent(js, "", " ")
	if err != nil {
		fmt.Println("error:", err)
	}
	return string(data)
}
func (hc *HttpClient) sendFollowPing(peerHost string, self peer.Peer) *peer.State {
	url := peerHost + baseUrl + "/whoAmI"
	fmt.Println("sending sendFollowPing to " + url)
	b, e := json.Marshal(&self)
	if e != nil {
		logger.Trace("Bad payload  " + e.Error())
	}
	return hc.sendHttp(url, "sendFollowPing", b)
}

func parseRsp(j []byte) *peer.State {
	if j != nil {
		var h peer.State
		err := json.Unmarshal(j, &h)
		if err != nil {
			logger.Error("Error unmarshalling body. " + err.Error())
		}
		return &h
	}
	return nil
}
func (hc *HttpClient) sendHttp(url string, spanName string, b []byte) *peer.State {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(b))
	if err != nil {
		logger.Trace("Bad request  " + err.Error())
		return nil
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	if rsp := hc.SendHttp(req, spanName, traceData(nil)); rsp != nil {
		return parseRsp(rsp)
	}
	return nil
}
func (hc *HttpClient) FindAndFollowRaftLeader(raftPeers *[]peer.Peer, self peer.Peer) *peer.State {
	for _, peer := range *raftPeers {
		if peerRsp := hc.sendFollowPing(peer.HttpAddr(), self); peerRsp != nil {
			// if rsp from leader
			if peerRsp.Self.HttpAddr() == peerRsp.RaftLeader.HttpAddr() {
				return peerRsp
			}
		}
	}
	return nil
}
func (hc *HttpClient) FindAndFollowSyncLeader(raftLeaders *[]peer.Peer, self peer.Peer) *peer.State {
	for _, peer := range *raftLeaders {
		if peerRsp := hc.sendFollowPing(peer.HttpAddr(), self); peerRsp != nil {
			return peerRsp
		}
	}
	return nil
}
func (hc *HttpClient) SendHttp(req *http.Request, spanName string, data traceData) []byte {

	ctx, cancel := context.WithCancel(req.Context())
	span := trace.SpanFromContext(req.Context())
	if !span.IsRecording() {
		ctx, span = hc.tr.Start(ctx, spanName, trace.WithAttributes(semconv.MessagingDestinationKey.String(req.URL.Path)))
		defer span.End()
	}
	now := time.Now()

	var resp *http.Response

	defer func() {
		cancel()
		span.SetAttributes(attribute.String("took time", time.Since(now).String()))
		//span.SetStatus(codes.Code(resp.StatusCode), resp.Status)
		//span.End()
	}()
	span.AddEvent(stringJson(data))
	// add baggage to span
	bag, err := baggage.Parse("username=goku")
	if err != nil {
		logger.Trace("ERROR parsing baggage" + err.Error())
		return nil
	}
	ctx = baggage.ContextWithBaggage(ctx, bag)

	client := http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport), Timeout: time.Second * 3}

	outReq, _ := http.NewRequestWithContext(ctx, req.Method, req.URL.String(), req.Body)
	for key, value := range req.Header {
		for _, v := range value {
			outReq.Header.Add(key, v)
		}
	}
	resp, err = client.Do(outReq)
	if err != nil {
		logger.Trace("ERROR reading response " + err.Error())
		return nil
	}
	span = trace.SpanFromContext(outReq.Context())
	// on error return nil
	if resp.StatusCode < 200 || resp.StatusCode > 205 {
		span.AddEvent("Client response code", trace.WithAttributes(attribute.String("Success", resp.Status)))
		span.SetAttributes(attribute.String("Client response code", resp.Status))
		return nil
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		span.AddEvent("Client response code", trace.WithAttributes(attribute.String("Error", resp.Status)))
		span.SetAttributes(attribute.String("Client response code", resp.Status))
		logger.Trace("ERROR reading body. " + err.Error())
		return nil
	}
	defer resp.Body.Close()
	return body

}
func (hc *HttpClient) SendPing(p string) *peer.State {
	url := p + baseUrl + "/whoAmI"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		logger.Trace("Bad request  " + err.Error())
		return nil
	}
	if rsp := hc.SendHttp(req, "sendPing", traceData("Send Ping")); rsp != nil {
		var h peer.State
		err := json.Unmarshal(rsp, &h)
		if err != nil {
			return nil
		}
		return &h
	}
	logger.Trace("nill ping from " + url)
	return nil
}

func (hc *HttpClient) SendSyncLeaderHb(from peer.Peer, to ...string) {
	rand.Seed(time.Now().Unix())
	tkr := time.Tick(250 * time.Millisecond)
	b, e := json.Marshal(&from)
	if e != nil {
		logger.Trace("Bad payload  " + e.Error())
	}
	for _, s := range to {
		<-tkr
		url := s + baseUrl + "/leader/syncLeader/HB"
		hc.sendHttp(url, "SendSyncLeaderHb", b)
	}
}

func (hc *HttpClient) SendZoneHeartBeat(from peer.Peer, to ...peer.Peer) []*peer.State {
	b, e := json.Marshal(&from)
	if e != nil {
		logger.Trace("Bad payload  " + e.Error())
	}
	var followers []*peer.State
	for _, p := range to {
		url := p.HttpAddr() + baseUrl + "/follower/receiveHeartBeat"
		logger.Debug("sending heartbeat to " + url)
		followers = append(followers, hc.sendHttp(url, "SendZoneHeartBeat", b))
	}

	return followers
}
