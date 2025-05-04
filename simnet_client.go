package rpc25519

import (
	//"context"
	//"crypto/tls"
	//"crypto/x509"
	//"fmt"
	//"log"
	//"net"
	//"os"
	//"strings"
	"time"
	//"os"
)

var _ = time.Time{}

func (c *Client) runSimNetClient(localHostPort string) {

	netAddr := &SimNetAddr{network: "cli simnet@" + localHostPort}

	// how does client pass this to us?/if we need it at all?
	//simNetConfig := &SimNetConfig{}

	// prevent race read vs simnet_server.go:68 setting simnet
	c.cfg.simnetRendezvous.mut.Lock()
	c.simnet = c.cfg.simnetRendezvous.simnet
	c.cfg.simnetRendezvous.mut.Unlock()

	conn := &simnetConn{
		isCli:   true,
		simnet:  c.simnet,
		netAddr: netAddr,
	}
	c.conn = conn
	//c.isTLS = false

	//c.setLocalAddr(conn)
	c.connected <- nil
	//defer conn.Close() // let server do if needed (prob not)

	cpair := &cliPairState{}
	c.cpair = cpair
	go c.runSendLoop(conn, cpair)
	select {
	case c.simnet.cliReady <- c:
	case <-c.simnet.halt.ReqStop.Chan:
		return
	case <-c.halt.ReqStop.Chan:
		return
	}
	c.runReadLoop(conn, cpair)
}
