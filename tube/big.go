package tube

import (
	"fmt"

	"github.com/glycerine/greenpack/msgp"
)

// print how much size each big field in Ticket / Inspection takes.

func (z *Ticket) Footprint() (r string) {
	r = "&Ticket{\n"
	// just for context
	r += fmt.Sprintf("                        Errs: \"%v\",\n", z.Errs)
	r += fmt.Sprintf("                    TicketID: \"%v\",\n", z.TicketID)
	r += fmt.Sprintf("                          Op: %v,\n", z.Op)
	r += fmt.Sprintf("                    LogIndex: %v,\n", z.LogIndex)
	r += fmt.Sprintf("                        Term: %v,\n", z.Term)
	r += fmt.Sprintf("                        Desc: \"%v\",\n", z.Desc)

	r += fmt.Sprintf("                         Key: %v,\n", z.Key.Msgsize())
	r += fmt.Sprintf("                         Val: %v,\n", z.Val.Msgsize())
	r += fmt.Sprintf("                      OldVal: %v,\n", z.OldVal.Msgsize())
	r += fmt.Sprintf("                       Stage: \"%v\",\n\n", msgp.GuessSize(z.Stage))
	if z.Insp != nil {
		r += fmt.Sprintf("                --- begin Insp ---\n%v\n", z.Insp.Footprint())
	}
	r += fmt.Sprintf("                ---  end Insp ---\n\n")
	if z.MC != nil {
		r += fmt.Sprintf("                          MC: %v,\n", z.MC.Msgsize())
	}
	if z.NewSessReq != nil {
		r += fmt.Sprintf("                  NewSessReq: %v,\n", z.NewSessReq.Msgsize())
	}
	if z.NewSessReply != nil {
		r += fmt.Sprintf("                NewSessReply: %v,\n", z.NewSessReply.Msgsize())
	}
	if z.StateSnapshot != nil {
		r += fmt.Sprintf("               StateSnapshot: %v,\n", z.StateSnapshot.Msgsize())
	}
	if z.KeyValRangeScan != nil {
		r += fmt.Sprintf("             KeyValRangeScan: %v,\n", z.KeyValRangeScan.Msgsize())
	}
	if len(z.Batch) > 0 {
		sz := 0
		for _, b := range z.Batch {
			sz += b.Msgsize()
		}
		r += fmt.Sprintf("                       Batch: %v,\n", sz)
		r += "  --- batch breakdown ---\n"
		for _, b := range z.Batch {
			if b != nil {
				r += b.Footprint()
			}
		}
		r += "  --- end batch breakdown ---\n\n}\n"
	}
	return
}

func (z *Inspection) Footprint() (r string) {
	r = "&Inspection{\n"
	r += fmt.Sprintf("                  CktReplica: %v,\n", msgp.GuessSize(z.CktReplica))
	r += fmt.Sprintf("            CktReplicaByName: %v,\n", msgp.GuessSize(z.CktReplicaByName))
	r += fmt.Sprintf("                      CktAll: %v,\n", msgp.GuessSize(z.CktAll))
	r += fmt.Sprintf("                CktAllByName: %v,\n", msgp.GuessSize(z.CktAllByName))
	sz := 0
	for nm, info := range z.Peers {
		sz += msgp.GuessSize(nm) + info.Msgsize()
	}
	r += fmt.Sprintf("                       Peers: %v,\n", sz)
	sz = 0
	for id, tkt := range z.WaitingAtLeader {
		sz += msgp.GuessSize(id) + tkt.Msgsize()
	}
	r += fmt.Sprintf("             WaitingAtLeader(len %v): %v,\n", len(z.WaitingAtLeader), sz)
	sz = 0
	for id, tkt := range z.WaitingAtFollow {
		sz += msgp.GuessSize(id) + tkt.Msgsize()
	}
	r += fmt.Sprintf("             WaitingAtFollow(len %v): %v,\n", len(z.WaitingAtFollow), sz)
	if z.State != nil {
		r += fmt.Sprintf("                       State: %v,\n", z.State.Msgsize())
	}
	r += fmt.Sprintf("                         Cfg: %v,\n", z.Cfg.Msgsize())
	if z.MC != nil {
		r += fmt.Sprintf("                          MC: %v,\n", z.MC.Msgsize())
	}
	if z.ShadowReplicas != nil {
		r += fmt.Sprintf("              ShadowReplicas: %v,\n", z.ShadowReplicas.Msgsize())
	}
	if z.Known != nil {
		r += fmt.Sprintf("                       Known: %v,\n", msgp.GuessSize(z.Known))
	}
	r += "}\n"
	return
}

func (z *RaftState) Footprint() (r string) {
	r = "&RaftState{\n"
	r += fmt.Sprintf("                KVstore: %v\n", z.KVstore.Msgsize())
	if z.MC != nil {
		r += fmt.Sprintf("                     MC: %v\n", z.MC.Msgsize())
	}
	r += fmt.Sprintf("                  Known: %v\n", z.Known.Msgsize())
	r += fmt.Sprintf("              Observers: %v\n", z.Observers.Msgsize())
	r += fmt.Sprintf("         ShadowReplicas: %v\n", z.ShadowReplicas.Msgsize())
	sz := 0
	for key, ste := range z.SessTable {
		sz += msgp.GuessSize(key) + ste.Msgsize()
	}
	r += fmt.Sprintf("              SessTable: %v\n", sz)
	r += "}\n"
	return
}
