package rpc25519

// doLoops is for traditional rpc/peer where we need the
// readLoop and the sendLoop going. The Dial/net.Conn
// stuff does not want these loops, so sets doLoops false.
func (c *Client) runSimNetClient(localHostPort, serverAddr string, doLoops bool) (err error) {

	//defer func() {
	//	vv("runSimNetClient defer on exit running client = %p", c)
	//}()

	//netAddr := &SimNetAddr{network: "cli simnet@" + localHostPort}

	// how does client pass this to us?/if we need it at all?
	//simNetConfig := &SimNetConfig{}

	c.cfg.simnetRendezvous.singleSimnetMut.Lock()
	c.simnet = c.cfg.simnetRendezvous.singleSimnet
	c.cfg.simnetRendezvous.singleSimnetMut.Unlock()

	if c.simnet == nil {
		panic("arg. client could not find cfg.simnetRendezvous.singleSimnet")
	}

	//vv("runSimNetClient c.simnet = %p, '%v', goro = %v", c.simnet, c.name, GoroNumber())

	// ignore serverAddr in favor of cfg.ClientDialToHostPort
	// which tests actually set.

	if c.cfg.ClientDialToHostPort == "" && serverAddr == "" {
		panic("gotta have a server address of some kind")
	}
	registration := c.simnet.newClientRegistration(c, localHostPort, serverAddr, c.cfg.ClientDialToHostPort, c.cfg.serverBaseID)

	select {
	case c.simnet.cliRegisterCh <- registration:
	case <-c.simnet.halt.ReqStop.Chan:
		return
	case <-c.halt.ReqStop.Chan:
		return
	}

	select {
	case <-registration.proceed:
		//vv("client registration.proceed")
	case <-c.simnet.halt.ReqStop.Chan:
		return
	case <-c.halt.ReqStop.Chan:
		return
	}

	//conn := c.cfg.simnetRendezvous.c2s
	conn := registration.conn
	c.mut.Lock()
	c.simnode = registration.simnode // conn.local
	c.simconn = conn
	c.conn = conn
	c.mut.Unlock()

	c.setLocalAddr(conn)
	// tell user level client code we are ready
	select {
	case c.connected <- nil:
	case <-c.halt.ReqStop.Chan:
		return
	}
	if doLoops {
		cpair := &cliPairState{}
		c.cpair = cpair
		go c.runSendLoop(conn, cpair)
		// does not return until client is stopped.
		c.runReadLoop(conn, cpair)
	}
	return
}
