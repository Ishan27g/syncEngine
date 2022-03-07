package transport

import (
	"context"
	"fmt"
	"net/http"
	"time"

	gossip "github.com/Ishan27g/gossipProtocol"
	"github.com/Ishan27g/syncEngine/peer"
	"github.com/Ishan27g/syncEngine/snapshot"
	"github.com/Ishan27g/syncEngine/utils"
	"github.com/Ishan27g/vClock"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel/trace"
)

type HTTPCbs func(*HttpSrv)
type SyncRsp struct {
	OrderedEvents []vClock.Event
	Entries       []snapshot.Entry
	SyncLeader    peer.Peer
}
type HttpSrv struct {
	httpSrv          *http.Server
	dataFile         string
	State            func() *peer.State
	NewZoneFollower  func(peer peer.Peer)
	NewSyncFollower  func(peer peer.Peer)
	HbFromZoneLeader func(peer peer.Peer)
	HbFromSyncLeader func(peer peer.Peer)
	SyncInitialOrder func() SyncRsp
	GetPacket        func(id string) *gossip.Packet
	SendGossip       func(data string)
}

func (h *HttpSrv) handlePing(c *gin.Context) {
	state := *h.State()
	span := trace.SpanFromContext(c.Request.Context())
	span.AddEvent(utils.PrintJson(state))
	c.JSON(200, state)
}
func (h *HttpSrv) handleSyncFollowPing(c *gin.Context) {
	var from peer.Peer
	err := c.BindJSON(&from)
	if err != nil {
		c.AbortWithStatus(400)
		return
	}
	// add follower
	// add gossip peer
	h.NewSyncFollower(from)
	// send state as response
	state := *h.State()
	span := trace.SpanFromContext(c.Request.Context())
	span.AddEvent(utils.PrintJson(state))
	c.JSON(200, state)
}

func (h *HttpSrv) handleZoneFollowPing(c *gin.Context) {
	var from peer.Peer
	err := c.BindJSON(&from)
	if err != nil {
		c.AbortWithStatus(400)
		return
	}
	// add follower
	// add gossip peer
	h.NewZoneFollower(from)
	// send state as response
	state := *h.State()
	span := trace.SpanFromContext(c.Request.Context())
	span.AddEvent(utils.PrintJson(state))
	c.JSON(200, state)
}

func (h *HttpSrv) hbFromRaftLeader(c *gin.Context) {
	state := *h.State()
	utils.PrintJson(state)
	// send to channel
	var from peer.Peer
	err := c.BindJSON(&from)
	if err != nil {
		c.AbortWithStatus(400)
		return
	}
	h.HbFromZoneLeader(from)
	// send state as response

	span := trace.SpanFromContext(c.Request.Context())
	span.AddEvent(utils.PrintJson(state))
	c.JSON(200, state)
}

func (h *HttpSrv) syncEventsOrder(c *gin.Context) {
	sync := h.SyncInitialOrder()
	span := trace.SpanFromContext(c.Request.Context())
	span.AddEvent(utils.PrintJson(sync))
	c.JSON(http.StatusOK, sync)
}

func (h *HttpSrv) sendSnapshot(c *gin.Context) {
	snap := snapshot.FromFile(h.dataFile).Get()
	span := trace.SpanFromContext(c.Request.Context())
	span.AddEvent(utils.PrintJson(snap))
	c.JSON(http.StatusOK, snap)
}

func (h *HttpSrv) sendPacket(c *gin.Context) {
	rsp := h.GetPacket(c.Param("id"))
	if rsp == nil {
		c.AbortWithStatus(400)
		return
	}
	span := trace.SpanFromContext(c.Request.Context())
	span.AddEvent(utils.PrintJson(*rsp))
	c.JSON(http.StatusOK, *rsp)
}
func (h *HttpSrv) syncLeaderHb(c *gin.Context) {
	// send to channel
	var from peer.Peer
	err := c.BindJSON(&from)
	if err != nil {
		c.AbortWithStatus(400)
		return
	}
	h.HbFromSyncLeader(from)
	// send state as response
	state := *h.State()
	c.JSON(200, state)
}

func (h *HttpSrv) Start(ctx context.Context) {
	go func() {
		go func() {
			fmt.Println("HTTP started on " + h.httpSrv.Addr)
			if err := h.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				fmt.Println("HTTP", err.Error())
			}
		}()
		<-ctx.Done()
		cx, can := context.WithTimeout(ctx, 2*time.Second)
		defer can()
		if err := h.httpSrv.Shutdown(cx); err != nil {
			fmt.Println("Http-Shutdown " + err.Error())
		}
	}()

}

func (h *HttpSrv) sendGossip(c *gin.Context) {
	h.SendGossip(c.Param("data"))
	c.Status(200)
}

func WithStateCb(cb func() *peer.State) HTTPCbs {
	return func(srv *HttpSrv) {
		srv.State = cb
	}
}
func WithRaftFollowerCb(cb func(peer peer.Peer)) HTTPCbs {
	return func(srv *HttpSrv) {
		srv.NewZoneFollower = cb
	}
}
func WithSyncFollowerCb(cb func(peer peer.Peer)) HTTPCbs {
	return func(srv *HttpSrv) {
		srv.NewSyncFollower = cb
	}
}
func WithZoneHbCb(cb func(peer peer.Peer)) HTTPCbs {
	return func(srv *HttpSrv) {
		srv.HbFromZoneLeader = cb
	}
}
func WithSyncHbCb(cb func(peer peer.Peer)) HTTPCbs {
	return func(srv *HttpSrv) {
		srv.HbFromSyncLeader = cb
	}
}
func WithSyncInitialOrderCb(cb func() SyncRsp) HTTPCbs {
	return func(srv *HttpSrv) {
		srv.SyncInitialOrder = cb
	}
}
func WithPacketCb(cb func(id string) *gossip.Packet) HTTPCbs {
	return func(srv *HttpSrv) {
		srv.GetPacket = cb
	}
}
func WithSnapshotFile(datafile string) HTTPCbs {
	return func(srv *HttpSrv) {
		srv.dataFile = datafile
	}
}
func WithGossipSend(cb func(data string)) HTTPCbs {
	return func(srv *HttpSrv) {
		srv.SendGossip = cb
	}
}
func NewHttpSrv(port string, tracerId string, cbs ...HTTPCbs) *HttpSrv {
	h := new(HttpSrv)
	for _, cb := range cbs {
		cb(h)
	}
	httpSrv := &http.Server{
		Addr:    port,
		Handler: nil,
	}
	gin.SetMode(gin.ReleaseMode)
	g := gin.New()
	g.Use(gin.Recovery())

	g.Use(otelgin.Middleware(tracerId))

	g.Handle("GET", "/engine/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, nil)
	})
	g.Handle("GET", "/engine/whoAmI", h.handlePing)
	g.Handle("POST", "/engine/whoAmI", h.handleZoneFollowPing)
	g.Handle("POST", "/engine/sync/whoAmI", h.handleSyncFollowPing)
	g.Handle("POST", "/engine/follower/receiveHeartBeat", h.hbFromRaftLeader)
	g.Handle("POST", "/engine/leader/syncLeader/HB", h.syncLeaderHb)
	g.Handle("POST", "/engine/leader/syncEventsOrder", h.syncEventsOrder)
	g.Handle("GET", "/engine/packet/data", h.sendSnapshot)
	g.Handle("GET", "/engine/packet/:id", h.sendPacket)
	g.Handle("GET", "/engine/gossip/:data", h.sendGossip)
	httpSrv.Handler = g
	h.httpSrv = httpSrv
	return h

}
