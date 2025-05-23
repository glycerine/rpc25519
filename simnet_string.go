package rpc25519

import (
	"fmt"
	"sort"
	"time"

	cristalbase64 "github.com/cristalhq/base64"
	"github.com/glycerine/blake3"
)

func (s *simnet) showDNS() {
	alwaysPrintf("simnet dns: %v", s.stringDNS)
}

func (s *simnet) stringDNS() (r string) {

	// sort names first for easier reading
	var names []string
	for nm := range s.dns {
		names = append(names, nm)
	}
	sort.Strings(names)

	i := 0
	r = "map[string]*simckt = {\n"
	for _, name := range names {
		node := s.dns[name]
		r += fmt.Sprintf("[%2d] showDNS dns[%v] = %p\n", i, name, node)
		i++
	}
	r += "}"
	return
}

func (node *simnode) String() (r string) {
	if node == nil {
		return "(nil *simnode)"
	}
	r += fmt.Sprintf("%v (powerOff: %v) in %v state, Q summary:\n", node.name, node.powerOff, node.state)
	r += node.readQ.String()
	r += node.preArrQ.String()
	r += node.timerQ.String()
	r += node.deafReadQ.String()
	r += node.droppedSendQ.String()
	return
}

func (node *simnode) StringNoPQ() (r string) {
	if node == nil {
		return "(nil *simnode)"
	}
	r += fmt.Sprintf("%v in %v state, Q summary:\n", node.name, node.state)
	return
}

func (alt Alteration) String() string {
	switch alt {
	case UNDEFINED:
		return "UNDEFINED"
	case SHUTDOWN:
		return "SHUTDOWN"
	case POWERON:
		return "POWERON"
	case ISOLATE:
		return "ISOLATE"
	case UNISOLATE:
		return "UNISOLATE"
	}
	panic(fmt.Sprintf("unknown Alteration %v", int(alt)))
	return "unknown Alteration"
}

func (pq *pq) String() (r string) {
	if pq == nil {
		return "(nil *pq)"
	}
	i := 0
	r = fmt.Sprintf(" ------- %v %v PQ --------\n", pq.Owner, pq.Orderby)
	for it := pq.Tree.Min(); it != pq.Tree.Limit(); it = it.Next() {

		item := it.Item() // interface{}
		if isNil(item) {
			panic("do not put nil into the pq")
		}
		op := item.(*mop)
		r += fmt.Sprintf("pq[%2d] = %v\n", i, op)
		i++
	}
	if i == 0 {
		r += fmt.Sprintf("empty PQ\n")
	}
	return
}

func (state Faultstate) String() string {
	switch state {
	case HEALTHY:
		return "HEALTHY"
	case ISOLATED:
		return "ISOLATED"
	case FAULTY:
		return "FAULTY"
	case FAULTY_ISOLATED:
		return "FAULTY_ISOLATED"
	}
	panic(fmt.Sprintf("unknown Faultstate '%v'", int(state)))
	return "unknown Faultstate"
}

func (k mopkind) String() string {
	switch k {
	case TIMER:
		return "TIMER"
	case TIMER_DISCARD:
		return "TIMER_DISCARD"
	case SEND:
		return "SEND"
	case READ:
		return "READ"
	case SNAPSHOT:
		return "SNAPSHOT"
	case CLIENT_REG:
		return "CLIENT_REG"
	case SERVER_REG:
		return "SERVER_REG"
	case FAULT_CKT:
		return "FAULT_CKT"
	case FAULT_HOST:
		return "FAULT_HOST"
	case REPAIR_CKT:
		return "REPAIR_CKT"
	case REPAIR_HOST:
		return "REPAIR_HOST"
	case SCENARIO:
		return "SCENARIO"
	case ALTER_HOST:
		return "ALTER_HOST"
	case ALTER_NODE:
		return "ALTER_NODE"

	default:
		return fmt.Sprintf("unknown mopkind %v", int(k))
	}
}

func (op *mop) String() string {
	if op == nil {
		return "(nil *mop)"
	}
	var msgSerial int64
	if op.msg != nil {
		msgSerial = op.msg.HDR.Serial
	}
	who := "SERVER"
	if op.originCli {
		who = "CLIENT"
	}
	now := time.Now()
	var ini, arr, complete string
	if op.initTm.IsZero() {
		ini = "unk"
	} else {
		ini = fmt.Sprintf("%v", op.initTm.Sub(now))
	}
	if op.arrivalTm.IsZero() {
		arr = "unk"
	} else {
		arr = fmt.Sprintf("%v", op.arrivalTm.Sub(now))
	}
	if op.completeTm.IsZero() {
		complete = "unk"
	} else {
		complete = fmt.Sprintf("%v", op.completeTm.Sub(now))
	}
	extra := ""
	switch op.kind {
	case TIMER:
		extra = " timer set at " + op.timerFileLine
	case TIMER_DISCARD:
		extra = " timer discarded at " + op.timerFileLine

	case SEND:
		extra = fmt.Sprintf(" FROM %v TO %v (eof:%v)", op.origin.name, op.target.name, op.isEOF_RST)

	case READ:
		extra = fmt.Sprintf(" AT %v FROM %v (eof:%v)", op.origin.name, op.target.name, op.isEOF_RST)

	}
	return fmt.Sprintf("mop{%v %v init:%v, arr:%v, complete:%v op.sn:%v, msg.sn:%v%v}", who, op.kind, ini, arr, complete, op.sn, msgSerial, extra)
}

func (z *circuitFault) String() (r string) {
	r = "&circuitFault{\n"
	r += fmt.Sprintf("  originName: %v\n", z.originName)
	r += fmt.Sprintf("  targetName: %v\n", z.targetName)
	r += fmt.Sprintf("DropDeafSpec: %v\n", z.DropDeafSpec.String())
	r += fmt.Sprintf("          sn: %v\n", z.sn)
	r += fmt.Sprintf("         err: %v\n", z.err)
	r += "}\n"
	return
}

func (z *circuitRepair) String() (r string) {
	r = "&circuitRepair{\n"
	r += fmt.Sprintf("              sn: %v\n", z.sn)
	r += fmt.Sprintf("      originName: %v\n", z.originName)
	r += fmt.Sprintf("      targetName: %v\n", z.targetName)
	r += fmt.Sprintf("       unIsolate: %v\n", z.unIsolate)
	r += fmt.Sprintf("    powerOnIfOff: %v\n", z.powerOnIfOff)
	r += fmt.Sprintf("justOriginHealed: %v\n", z.justOriginHealed)
	r += fmt.Sprintf("             err: %v\n", z.err)
	r += "}\n"
	return
}
func (z *DropDeafSpec) String() (r string) {
	r = "&DropDeafSpec{\n"
	r += fmt.Sprintf(" UpdateDeafReads: %v\n", z.UpdateDeafReads)
	r += fmt.Sprintf(" UpdateDropSends: %v\n", z.UpdateDropSends)
	r += fmt.Sprintf("DeafReadsNewProb: %v\n", z.DeafReadsNewProb)
	r += fmt.Sprintf("DropSendsNewProb: %v\n", z.DropSendsNewProb)
	r += "}\n"
	return
}

func (z *hostFault) String() (r string) {
	r = "&hostFault{\n"
	r += fmt.Sprintf("    hostName: %v\n", z.hostName)
	r += fmt.Sprintf("DropDeafSpec: %v\n", z.DropDeafSpec)
	r += fmt.Sprintf("          sn: %v\n", z.sn)
	r += fmt.Sprintf("         err: %v\n", z.err)
	r += "}\n"
	return
}

func (z *hostRepair) String() (r string) {
	r = "&hostRepair{\n"
	r += fmt.Sprintf("    hostName: %v\n", z.hostName)
	r += fmt.Sprintf("         err: %v\n", z.err)
	r += fmt.Sprintf("powerOnIfOff: %v\n", z.powerOnIfOff)
	r += fmt.Sprintf("   unIsolate: %v\n", z.unIsolate)
	r += fmt.Sprintf("    allHosts: %v\n", z.allHosts)
	r += fmt.Sprintf("          sn: %v\n", z.sn)
	r += "}\n"
	return
}

// traditionally, a string form of address
// (for example, "192.0.2.1:25", "[2001:db8::1]:80").
// In simnet, we just use the name. Easier diagnostics,
// simpler "dns" lookup keys.
func (s *SimNetAddr) String() (str string) {
	if s == nil {
		return "(nil *SimNetAddr)"
	}
	// keep it simple, as it is our simnet.dns lookup key.
	//str = s.addr + "/" + s.name
	str = s.name
	//vv("SimNetAddr.String() returning '%v'", str) // recursive... locks
	return
}

func (s *simnet) String() (r string) {
	if s == nil {
		return "(nil *simnet)"
	}
	r = "&simnet{\n"
	r += fmt.Sprintf("   faketime: %v\n", faketime)
	r += fmt.Sprintf("    barrier: %v\n", s.barrier)
	r += fmt.Sprintf("   scenario: %v\n", s.scenario)
	r += fmt.Sprintf("        cfg: %v\n", s.cfg)
	//r += fmt.Sprintf("simNetCfg: %v\n", s.simNetCfg)
	if s.srv != nil {
		r += fmt.Sprintf("   srv.name: %v\n", s.srv.name)
	} else {
		r += fmt.Sprintf("        srv: %v\n", s.srv)
	}
	if s.cli != nil {
		r += fmt.Sprintf("   cli.name: %v\n", s.cli.name)
	} else {
		r += fmt.Sprintf("        cli: %v\n", s.cli)
	}
	r += fmt.Sprintf("        dns: %v\n", s.stringDNS())
	//r += fmt.Sprintf("       halt: %v\n", s.halt)
	//r += fmt.Sprintf("  nextTimer: %v\n", s.nextTimer)
	r += fmt.Sprintf("  lastArmTm: %v\n", s.lastArmTm.Format(rfc3339NanoNumericTZ0pad))
	r += "}\n"
	return
}

func (s *serverRegistration) String() (r string) {
	if s == nil {
		return "(nil *serverRegistration)"
	}
	r = "&serverRegistration{\n"
	if s.server != nil {
		r += fmt.Sprintf("        server.name: %v\n", s.server.name)
	} else {
		r += fmt.Sprintf("              server: %v\n", s.server)
	}
	r += fmt.Sprintf("         srvNetAddr: %v\n", s.srvNetAddr)
	r += fmt.Sprintf("            simnode: %v\n", s.simnode.StringNoPQ())
	r += fmt.Sprintf("             simnet: %v\n", s.simnet)
	r += "}\n"
	return
}

func (s *clientRegistration) String() (r string) {
	if s == nil {
		return "(nil *clientRegistration)"
	}
	r = "&clientRegistration{\n"
	if s.client != nil {
		r += fmt.Sprintf("     client.name: %v\n", s.client.name)
	} else {
		r += fmt.Sprintf("          client: %v\n", s.client)
	}
	r += fmt.Sprintf("localHostPortStr: %v\n", s.localHostPortStr)
	r += fmt.Sprintf("          dialTo: %v\n", s.dialTo)
	r += fmt.Sprintf("   serverAddrStr: %v\n", s.serverAddrStr)
	r += fmt.Sprintf("         simnode: %v\n", s.simnode.StringNoPQ())
	r += fmt.Sprintf("            conn: %v\n", s.conn)
	r += "}\n"
	return
}

func (s *simnodeAlteration) String() (r string) {
	if s == nil {
		return "(nil *simnodeAlteration)"
	}
	r = "&simnodeAlteration{\n"
	r += fmt.Sprintf(" simnet: %v\n", s.simnet)
	r += fmt.Sprintf("simnode: %v\n", s.simnodeName)
	r += fmt.Sprintf("  alter: %v\n", s.alter)
	r += "}\n"
	return
}

func blake3OfSeed32(seed [32]byte) (b3 string, isZero bool) {
	isZero = seed == [32]byte{}
	h := blake3.New(64, nil)
	h.Write(seed[:])
	sum := h.Sum(nil)
	b3 = "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(sum[:33])
	return
}

func (s *scenario) String() (r string) {
	if s == nil {
		return "(nil *scenario)"
	}
	b3seed, isZero := blake3OfSeed32(s.seed)
	if isZero {
		b3seed = "(zero seed) " + b3seed
	} else {
		b3seed = fmt.Sprintf("(seed[0]=%v) %v", s.seed[0], b3seed)
	}
	r = "&scenario{\n"
	r += fmt.Sprintf("  seed: %v\n", b3seed)
	//r += fmt.Sprintf("   rng: %v\n", s.rng)
	r += fmt.Sprintf("  tick: %v\n", s.tick)
	r += fmt.Sprintf("minHop: %v\n", s.minHop)
	r += fmt.Sprintf("maxHop: %v\n", s.maxHop)
	r += "}\n"
	return
}

func (s *simconn) String() (r string) {

	s.mut.Lock()
	defer s.mut.Unlock()

	readDead := "nil"
	if s.readDeadlineTimer != nil {
		readDead = s.readDeadlineTimer.completeTm.Format(rfc3339MsecTz0)
	}
	sendDead := "nil"
	if s.sendDeadlineTimer != nil {
		sendDead = s.sendDeadlineTimer.completeTm.Format(rfc3339MsecTz0)
	}

	r = fmt.Sprintf(`
     simconn{
                isCli: %v
                  net: %v
              netAddr: %v
                local: %v
               remote: %v
    readDeadlineTimer: %v
    sendDeadlineTimer: %v
          localClosed: %v
         remoteClosed: %v
             deafRead: %0.2f (probability)
             dropSend: %0.2f (probability)
             nextRead: "%v"
}`, s.isCli,
		s.net.simNetCfg,
		s.netAddr,
		s.local.name,
		s.remote.name,
		readDead,
		sendDead,
		s.localClosed.IsClosed(),
		s.remoteClosed.IsClosed(),
		s.deafRead,
		s.dropSend,
		string(s.nextRead),
	)
	return
}

func (s *SimNetConfig) String() string {
	if s.BarrierOff {
		return fmt.Sprintf(`SimNetConfig{ barrier is OFF}`)
	}
	return fmt.Sprintf(`SimNetConfig{ barrier is ON}`)
}

func (z *SimnetConnSummary) String() (r string) {
	//r = "&SimnetConnSummary{\n"
	r += fmt.Sprintf("   ----  origin details  ----\n")
	r += fmt.Sprintf("        Origin: %v\n", z.Origin)
	r += fmt.Sprintf("   OriginState: %v\n", z.OriginState)
	r += fmt.Sprintf("  OriginClosed: %v\n", z.OriginConnClosed)
	r += fmt.Sprintf("OriginPoweroff: %v\n", z.OriginPoweroff)
	r += fmt.Sprintf("   OriginIsCli: %v\n", z.OriginIsCli)
	r += fmt.Sprintf("   ----  target details  ----\n")
	r += fmt.Sprintf("        Target: %v\n", z.Target)
	r += fmt.Sprintf("   TargetState: %v\n", z.TargetState)
	r += fmt.Sprintf("  TargetClosed: %v\n", z.TargetConnClosed)
	r += fmt.Sprintf("TargetPoweroff: %v\n", z.TargetPoweroff)
	r += fmt.Sprintf(" ----  origin side fault settings  ----\n")
	drop, deaf := "(healthy)", "(healthy)"
	alldrop, alldeaf := false, false
	if z.DropSendProb > 0 {
		drop = "(send dropped with probability)"
		if z.DropSendProb >= 1 {
			drop = "(all sends dropped)"
			alldrop = true
		}
	}
	if z.DeafReadProb > 0 {
		deaf = "(read deaf with probability)"
		if z.DeafReadProb >= 1 {
			deaf = "(all reads deaf)"
			alldeaf = true
		}
	}
	if !alldrop && (z.OriginState == ISOLATED ||
		z.OriginState == FAULTY_ISOLATED) {
		drop += fmt.Sprintf(" but %v drops all -> sender's droppedSendQ", z.OriginState)
	}
	if !alldeaf && (z.OriginState == ISOLATED ||
		z.OriginState == FAULTY_ISOLATED) {
		deaf += fmt.Sprintf(" but %v deafens all reads -> reader's deafReadQ", z.OriginState)
	}
	r += fmt.Sprintf("  DropSendProb: %0.2f %v\n", z.DropSendProb, drop)
	r += fmt.Sprintf("  DeafReadProb: %0.2f %v\n", z.DeafReadProb, deaf)
	r += fmt.Sprintf(" ----  origin priority queues  ----\n%v\n", z.Qs)
	//r += "}\n"
	return
}

func (z *SimnetPeerStatus) String() (r string) {
	//r = "&SimnetPeerStatus{\n"
	r += fmt.Sprintf("        Name: %v\n", z.Name)
	r += fmt.Sprintf(" ServerState: %v\n", z.ServerState)
	r += fmt.Sprintf("    Poweroff: %v\n", z.Poweroff)
	r += fmt.Sprintf("          LC: %v\n", z.LC)
	if z.IsLoneCli {
		r += fmt.Sprintf("   IsLoneCli: %v\n", z.IsLoneCli)
	}
	// distracting, but might need it later.
	//r += fmt.Sprintf("ServerBaseID: %v\n", z.ServerBaseID)
	if !z.IsLoneCli {
		r += fmt.Sprintf("    Conn[%v: peer has %v dialed client + 1 listening server]:\n", len(z.Conn), len(z.Conn)-1)
	}
	for i, conn := range z.Conn {
		if z.IsLoneCli {
			r += fmt.Sprintf("=========  the conn[lone cli] %v -> %v\n",
				conn.Origin, conn.Target)
		} else {
			r += fmt.Sprintf("=========  conn[%v on peer] %v -> %v\n",
				i, conn.Origin, conn.Target)
		}
		r += fmt.Sprintf("%v\n", conn.String())
	}

	//r += "}\n"
	return
}

func (z *SimnetSnapshot) notAllHealthy() (long bool) {
	for _, srv := range z.Peer {
		if srv.ServerState != HEALTHY {
			long = true
			return
		}
	}
	if !long {
		for _, cli := range z.LoneCli {
			if cli.ServerState != HEALTHY {
				long = true
				return
			}
		}
	}
	return
}

// ShortString: if everything is healthy, just give a short
// summary. Otherwise give the full snapshot.
func (z *SimnetSnapshot) ShortString() (r string) {
	faults := z.notAllHealthy()
	if faults {
		r += "SimnetSnapshot{some faults}:"
	} else {
		r += "SimnetSnapshot{all HEALTHY}:"
	}
	np := len(z.Peer)
	if np > 0 {
		r += fmt.Sprintf(" Peers[%v][%v conn]{", np, z.PeerConnCount)
		for i, peer := range z.Peer {
			if i > 0 {
				r += ", "
			}
			r += fmt.Sprintf("%v", peer.Name)
			if faults {
				r += fmt.Sprintf("(%v)", peer.ServerState)
			}
		}
		r += "}"
	}
	ncli := len(z.LoneCli)
	if ncli > 0 {
		r += fmt.Sprintf(" LoneCli[%v][%v conn]{", ncli, z.LoneCliConnCount)
		i := 0
		for _, cli := range z.LoneCli {
			if i > 0 {
				r += ", "
			}
			r += fmt.Sprintf("%v", cli.Name)
			if faults {
				r += fmt.Sprintf("(%v)", cli.ServerState)
			}
			i++
		}
		r += "}"
	}
	r += "}\n"
	return
}

// String: if everything is healthy, just give a short
// summary. Otherwise give the full snapshot.
func (z *SimnetSnapshot) String() (r string) {
	if z.notAllHealthy() {
		return z.LongString()
	}
	return z.ShortString()
}

func (z *SimnetSnapshot) LongString() (r string) {
	r = "&SimnetSnapshot{\n"

	r += fmt.Sprintf("              Asof: %v\n",
		z.Asof.Format(rfc3339NanoNumericTZ0pad))
	r += fmt.Sprintf("         NetClosed: %v\n", z.NetClosed)
	r += fmt.Sprintf("               Cfg: %v\n", z.Cfg.String())
	r += fmt.Sprintf("             Loopi: %v\n", z.Loopi)
	r += fmt.Sprintf("       ScenarioNum: %v\n", z.ScenarioNum)
	r += fmt.Sprintf("GetSimnetStatusErr: %v\n", z.GetSimnetStatusErr)

	b3seed, isZero := blake3OfSeed32(z.ScenarioSeed)
	if isZero {
		b3seed = "(zero seed) " + b3seed
	} else {
		b3seed = fmt.Sprintf("(seed[0]=%v) %v", z.ScenarioSeed[0], b3seed)
	}
	r += fmt.Sprintf("  ScenarioSeed: %v\n", b3seed)
	r += fmt.Sprintf("  ScenarioTick: %v\n", z.ScenarioTick)
	r += fmt.Sprintf("ScenarioMinHop: %v\n", z.ScenarioMinHop)
	r += fmt.Sprintf("ScenarioMaxHop: %v\n", z.ScenarioMaxHop)

	r += fmt.Sprintf("peer count(%v) total connection count(%v):\n", len(z.Peer), z.PeerConnCount)
	for i, srv := range z.Peer {
		r += fmt.Sprintf(" ===============================\n")
		r += fmt.Sprintf(" =======  SimnetPeerStatus[%v]  %v\n", i, srv.Name)
		r += fmt.Sprintf("%v\n", srv.String())
	}

	r += fmt.Sprintf("lone cli count(%v); not with a peer. conns(%v)\n", len(z.LoneCli), z.LoneCliConnCount)
	for i, cli := range z.LoneCli {
		r += fmt.Sprintf(" ===============================\n")
		r += fmt.Sprintf(" =======  Simnet LoneCli Status[%v]  %v\n", i, cli.Name)
		r += fmt.Sprintf("%v\n", cli.String())
	}
	//r += "}\n"
	return
}
