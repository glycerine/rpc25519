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
	r = "map[string]*simnode = {\n"
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
	r += fmt.Sprintf("%v in %v state, Q summary:\n", node.name, node.state)
	r += node.readQ.String()
	r += node.preArrQ.String()
	r += node.timerQ.String()
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
	case SHUTDOWN:
		return "SHUTDOWN"
	case ISOLATE:
		return "ISOLATE"
	case UNISOLATE:
		return "UNISOLATE"
	case RESTART:
		return "RESTART"
	}
	panic(fmt.Sprintf("unknown Alteration %v", int(alt)))
	return "unknown Alteration"
}

func (pq *pq) String() (r string) {
	if pq == nil {
		return "(nil *pq)"
	}
	i := 0
	r = fmt.Sprintf("\n ------- %v %v PQ --------\n", pq.owner, pq.orderby)
	for it := pq.tree.Min(); it != pq.tree.Limit(); it = it.Next() {

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

func (state nodestate) String() string {
	switch state {
	case HEALTHY:
		return "HEALTHY"
	case HALTED:
		return "HALTED"
	case ISOLATED:
		return "ISOLATED"
	}
	panic(fmt.Sprintf("unknown nodestate '%v'", int(state)))
	return "unknown nodestate"
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
	case DEAFDROP:
		return "DEAFDROP"
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
		dropped := ""
		if op.sendIsDropped {
			dropped = " DROPPED SEND"
		}
		extra = fmt.Sprintf(" FROM %v TO %v (eof:%v)%v", op.origin.name, op.target.name, op.isEOF_RST, dropped)

	case READ:
		deaf := ""
		if op.readIsDeaf {
			deaf = " DEAF READ"
		}
		extra = fmt.Sprintf(" AT %v FROM %v (eof:%v)%v", op.origin.name, op.target.name, op.isEOF_RST, deaf)

	case DEAFDROP:
		return fmt.Sprintf(`
mop{%v %v op.sn:%v
          originName: %v,
          targetName: %v,
     updateDeafReads: %v,
    deafReadsNewProb: %v,
     updateDropSends: %v,
    dropSendsNewProb: %v,
                 err: %v,
}`, who, op.kind, op.sn,
			op.originName,
			op.targetName,
			op.updateDeafReads,
			op.deafReadsNewProb,
			op.updateDropSends,
			op.dropSendsNewProb,
			op.err,
		)
	}
	return fmt.Sprintf("mop{%v %v init:%v, arr:%v, complete:%v op.sn:%v, msg.sn:%v%v}", who, op.kind, ini, arr, complete, op.sn, msgSerial, extra)
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
	r += fmt.Sprintf("   faketime: %v,\n", faketime)
	r += fmt.Sprintf("    barrier: %v,\n", s.barrier)
	r += fmt.Sprintf("   scenario: %v,\n", s.scenario)
	r += fmt.Sprintf("        cfg: %v,\n", s.cfg)
	//r += fmt.Sprintf("simNetCfg: %v,\n", s.simNetCfg)
	if s.srv != nil {
		r += fmt.Sprintf("   srv.name: %v,\n", s.srv.name)
	} else {
		r += fmt.Sprintf("        srv: %v,\n", s.srv)
	}
	if s.cli != nil {
		r += fmt.Sprintf("   cli.name: %v,\n", s.cli.name)
	} else {
		r += fmt.Sprintf("        cli: %v,\n", s.cli)
	}
	r += fmt.Sprintf("        dns: %v,\n", s.stringDNS())
	//r += fmt.Sprintf("       halt: %v,\n", s.halt)
	//r += fmt.Sprintf("  nextTimer: %v,\n", s.nextTimer)
	r += fmt.Sprintf("  lastArmTm: %v,\n", s.lastArmTm)
	r += "}\n"
	return
}

func (s *serverRegistration) String() (r string) {
	if s == nil {
		return "(nil *serverRegistration)"
	}
	r = "&serverRegistration{\n"
	if s.server != nil {
		r += fmt.Sprintf("        server.name: %v,\n", s.server.name)
	} else {
		r += fmt.Sprintf("              server: %v,\n", s.server)
	}
	r += fmt.Sprintf("         srvNetAddr: %v,\n", s.srvNetAddr)
	r += fmt.Sprintf("            simnode: %v,\n", s.simnode.StringNoPQ())
	r += fmt.Sprintf("             simnet: %v,\n", s.simnet)
	r += "}\n"
	return
}

func (s *clientRegistration) String() (r string) {
	if s == nil {
		return "(nil *clientRegistration)"
	}
	r = "&clientRegistration{\n"
	if s.client != nil {
		r += fmt.Sprintf("     client.name: %v,\n", s.client.name)
	} else {
		r += fmt.Sprintf("          client: %v,\n", s.client)
	}
	r += fmt.Sprintf("localHostPortStr: \"%v\",\n", s.localHostPortStr)
	r += fmt.Sprintf("          dialTo: \"%v\",\n", s.dialTo)
	r += fmt.Sprintf("   serverAddrStr: \"%v\",\n", s.serverAddrStr)
	r += fmt.Sprintf("         simnode: %v,\n", s.simnode.StringNoPQ())
	r += fmt.Sprintf("            conn: %v,\n", s.conn)
	r += "}\n"
	return
}

func (s *nodeAlteration) String() (r string) {
	if s == nil {
		return "(nil *nodeAlteration)"
	}
	r = "&nodeAlteration{\n"
	r += fmt.Sprintf(" simnet: %v,\n", s.simnet)
	r += fmt.Sprintf("simnode: %v,\n", s.simnode)
	r += fmt.Sprintf("  alter: %v,\n", s.alter)
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
	r += fmt.Sprintf("  seed: %v,\n", b3seed)
	//r += fmt.Sprintf("   rng: %v,\n", s.rng)
	r += fmt.Sprintf("  tick: %v,\n", s.tick)
	r += fmt.Sprintf("minHop: %v,\n", s.minHop)
	r += fmt.Sprintf("maxHop: %v,\n", s.maxHop)
	r += "}\n"
	return
}

func (s *simnetConn) String() (r string) {

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
     simnetConn{
                isCli: %v,
                  net: %v,
              netAddr: %v,
                local: %v,
               remote: %v,
    readDeadlineTimer: %v,
    sendDeadlineTimer: %v,
          localClosed: %v,
         remoteClosed: %v,
             deafRead: %0.3f (probability),
             dropSend: %0.3f (probability),
             nextRead: "%v",
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
	return fmt.Sprintf(`SimNetConfig{BarrierOff: %v}`, s.BarrierOff)
}
