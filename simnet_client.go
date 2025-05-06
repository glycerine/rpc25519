package rpc25519

func (c *Client) runSimNetClient(localHostPort, serverAddr string) {

	//netAddr := &SimNetAddr{network: "cli simnet@" + localHostPort}

	// how does client pass this to us?/if we need it at all?
	//simNetConfig := &SimNetConfig{}

	singleSimnetMut.Lock()
	c.simnet = singleSimnet
	singleSimnetMut.Unlock()

	vv("runSimNetClient c.simnet = %p, '%v', goro = %v", c.simnet, c.name, GoroNumber()) // only 'auto-cli-srv_grid_node_1'

	// ignore serverAddr in favor of cfg.ClientDialToHostPort
	// which tests actually set.

	if c.cfg.ClientDialToHostPort == "" && serverAddr == "" {
		panic("gotta have a server address of some kind")
	}
	registration := c.simnet.newClientRegistration(c, localHostPort, serverAddr, c.cfg.ClientDialToHostPort)

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
