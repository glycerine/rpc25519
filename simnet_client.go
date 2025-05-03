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

	netAddr := &SimNetAddr{network: "simnet@" + localHostPort}

	// how does client pass this to us?/if we need it at all?
	//simNetConfig := &SimNetConfig{}

	c.simnet = c.cfg.simnetRendezvous.simnet

	conn := &simnetConn{
		isCli:   true,
		simnet:  c.simnet,
		netAddr: netAddr,
	}
	c.conn = conn
	//c.isTLS = false

	//c.setLocalAddr(conn)
	c.connected <- nil
	defer conn.Close()

	cpair := &cliPairState{}
	c.cpair = cpair
	go c.runSendLoop(conn, cpair)
	c.runReadLoop(conn, cpair)
}
