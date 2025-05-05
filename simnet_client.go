package rpc25519

func (c *Client) runSimNetClient(localHostPort string) {

	//netAddr := &SimNetAddr{network: "cli simnet@" + localHostPort}

	// how does client pass this to us?/if we need it at all?
	//simNetConfig := &SimNetConfig{}

	c.cfg.simnetRendezvous.mut.Lock()
	c.simnet = c.cfg.simnetRendezvous.simnet
	c.cfg.simnetRendezvous.mut.Unlock()

	registration := c.simnet.newClientRegistration(c)

	select {
	case c.simnet.cliRegisterCh <- registration:
	case <-c.simnet.halt.ReqStop.Chan:
		return
	case <-c.halt.ReqStop.Chan:
		return
	}

	select {
	case <-registration.done:
	case <-c.simnet.halt.ReqStop.Chan:
		return
	case <-c.halt.ReqStop.Chan:
		return
	}

	//conn := c.cfg.simnetRendezvous.c2s
	conn := registration.conn
	c.simnode = registration.simnode // conn.local
	c.simconn = conn
	c.conn = conn

	c.setLocalAddr(conn)
	// tell user level client code we are ready
	c.connected <- nil

	cpair := &cliPairState{}
	c.cpair = cpair
	go c.runSendLoop(conn, cpair)
	c.runReadLoop(conn, cpair)
}
