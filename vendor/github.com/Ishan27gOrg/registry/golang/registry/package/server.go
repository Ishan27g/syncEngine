package _package

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"time"

	"github.com/Ishan27g/go-utils/mLogger"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/go-hclog"
)

const (
	RegUrl         = "/register"
	ZoneIdsUrl     = "/zones"
	ZoneUrl        = "/zone"
	DetailsUrl     = "/details"
	DetailsUrlJson = "/details/json"
	ShutdownUrl    = "/shutdown"
	ResetUrl       = "/reset"
)

var sh *serverHandler

func ShutDown() {
	if sh != nil {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		sh.shutdown(c)
	}
}
func newPeer(address string, zone int, meta MetaData) peer {
	return peer{
		RegisterAt: time.Now().UTC(),
		Address:    address,
		Zone:       zone,
		MetaData:   meta,
	}
}
func registerReqBody(p peer) []byte {
	json, err := json.Marshal(p)
	if err != nil {
		return nil
	}
	return json
}
func sendReq(req *http.Request) []byte {
	client := &http.Client{Timeout: time.Second * 10}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("ERROR reading response " + err.Error())
		return nil
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("ERROR reading body. " + err.Error())
		return nil
	}
	defer resp.Body.Close()
	return body
}
func parseRegRsp(j []byte) []RegisterRequest {
	if j != nil {
		var rr []RegisterRequest
		err := json.Unmarshal(j, &rr)
		if err != nil {
			fmt.Println("e" + err.Error())
		}
		return rr
	}
	return nil
}

type serverHandler struct {
	stop   context.CancelFunc
	logger hclog.Logger
	gin    *gin.Engine
	reg    *registry
}

// registerPeer registers a peer to a zone, returns list of peers for that zone
func (sh *serverHandler) registerPeer(c *gin.Context) {
	var p peer
	if err := c.ShouldBindJSON(&p); err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}

	if sh.reg.addPeer(p) {
		var r []RegisterRequest
		for _, p2 := range sh.reg.getPeers(p.Zone) {
			r = append(r, RegisterRequest{
				RegisterAt: p2.RegisterAt,
				Address:    p2.Address,
				Zone:       p2.Zone,
				MetaData:   p2.MetaData,
			})
		}
		sh.logger.Debug("Registered - " + p.Address)
		sh.logger.Debug("registerPeer - " + fmt.Sprintf("%v", r))
		c.JSON(http.StatusAccepted, r)
	} else {
		c.AbortWithStatus(http.StatusExpectationFailed)
	}

}

// getZonePeers returns list of peers for requested zone
func (sh *serverHandler) getZonePeers(c *gin.Context) {
	zoneId, err := strconv.Atoi(c.Query("id"))
	if err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	var rsp []RegisterRequest
	for _, p2 := range sh.reg.getPeers(zoneId) {
		rsp = append(rsp, RegisterRequest{
			RegisterAt: p2.RegisterAt,
			Address:    p2.Address,
			Zone:       p2.Zone,
			MetaData:   p2.MetaData,
		})
	}
	sh.logger.Debug("getZonePeers - " + fmt.Sprintf("%v", rsp))
	c.JSON(http.StatusAccepted, rsp)
}

// runServer till context is done/cancelled
func (sh *serverHandler) runServer(addr string, ctx context.Context) {
	httpSrv := &http.Server{
		Addr:    ":" + addr,
		Handler: sh.gin,
	}
	go func() {
		sh.logger.Info("Started on " + addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			sh.logger.Error(err.Error())
		}
	}()
	<-ctx.Done()
	cx, can := context.WithTimeout(ctx, 2*time.Second)
	defer can()
	if err := httpSrv.Shutdown(cx); err != nil {
		sh.logger.Error("Shutdown " + err.Error())
	}
	sh.logger.Info("Quitting")
}

// close registry
func (sh *serverHandler) shutdown(c *gin.Context) {
	defer sh.stop()
	c.String(http.StatusOK, "Ok")
}

// all details
func (sh *serverHandler) details(c *gin.Context) {
	r := sh.reg.allDetails(false).(string)
	sh.logger.Debug("details - " + fmt.Sprintf("%v", r))
	c.String(http.StatusOK, r)
}

func (sh *serverHandler) detailsJson(c *gin.Context) {
	r := sh.reg.allDetails(true)
	sh.logger.Debug("detailsJson - " + fmt.Sprintf("%v", r))
	c.JSON(http.StatusOK, r)
}

// zone Ids
func (sh *serverHandler) getZones(c *gin.Context) {
	r := &gin.H{"zoneIds": sh.reg.zoneIds()}
	sh.logger.Debug("detailsJson - " + fmt.Sprintf("%v", r))
	c.JSON(http.StatusOK, r)
}

// reset everything
func (sh *serverHandler) reset(c *gin.Context) {
	sh.reg.clear()
	c.String(http.StatusOK, "Ok")
}

// Run configures the http Run and handlers
func Run(addr string, reg *registry) {
//	gin.SetMode(gin.ReleaseMode)

	sh = &serverHandler{
		stop:   nil,
		logger: mLogger.Get("http"),
		gin:    gin.New(),
		reg:    reg,
	}

	sh.gin.Use(gin.Recovery())

	// get full details
	sh.gin.Handle("GET", DetailsUrl, sh.details)
	sh.gin.Handle("GET", DetailsUrlJson, sh.detailsJson)
	// get list of zone id
	sh.gin.Handle("GET", ZoneIdsUrl, sh.getZones)
	// shutdown registry
	sh.gin.Handle("GET", ShutdownUrl, sh.shutdown)
	// shutdown registry
	sh.gin.Handle("GET", ResetUrl, sh.reset)
	// get list of peers for a zone
	sh.gin.Handle("GET", ZoneUrl, sh.getZonePeers)
	// add a peer to a zone, return list of peers for that zone
	sh.gin.Handle("POST", RegUrl, sh.registerPeer)

	ctx, cancel := context.WithCancel(context.Background())

	sh.stop = cancel
	// listen and serve http
	sh.runServer(addr, ctx) // blocking
}
