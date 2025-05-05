package rpc25519

func (c *Client) runSimNetClient(localHostPort string) {

	netAddr := &SimNetAddr{network: "cli simnet@" + localHostPort}

	// how does client pass this to us?/if we need it at all?
	//simNetConfig := &SimNetConfig{}

	c.cfg.simnetRendezvous.mut.Lock()

	c.simnet = c.cfg.simnetRendezvous.simnet
	conn := c.cfg.simnetRendezvous.c2s
	conn.netAddr = netAddr
	c.simnode = conn.local
	c.simconn = conn
	c.conn = conn

	c.cfg.simnetRendezvous.mut.Unlock()

	c.setLocalAddr(conn)
	c.connected <- nil

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
