package rpc25519

import (
	"context"
	//"fmt"
	//"strings"
	//"sync"
	//"sync/atomic"
	//"time"
	"github.com/glycerine/idem"
)

// peer_test.go uses this syncer test object.

func newSyncer(name string) *syncer {
	return &syncer{
		name:                   name,
		PushToPeerURL:          make(chan string),
		halt:                   idem.NewHalter(),
		gotIncomingCkt:         make(chan *Circuit),
		gotOutgoingCkt:         make(chan *Circuit),
		gotIncomingCktReadFrag: make(chan *Fragment),

		gotCktHaltReq: idem.NewIdemCloseChan(),
	}
}

type syncer struct {
	name          string
	PushToPeerURL chan string
	halt          *idem.Halter

	gotOutgoingCkt chan *Circuit
	gotIncomingCkt chan *Circuit

	gotIncomingCktReadFrag chan *Fragment

	gotCktHaltReq *idem.IdemCloseChan
}

// instrument for peer_test.go
func (s *syncer) Start(
	myPeer LocalPeer,
	ctx0 context.Context,
	newPeerCh <-chan RemotePeer,

) error {

	defer func() {
		vv("%v: (%v) end of syncer.Start() inside defer, about the return/finish", s.name, myPeer.ServiceName())
		s.halt.Done.Close()
	}()

	vv("%v: syncer.Start() top.", s.name)
	vv("%v: ourID = '%v'; peerServiceName='%v';", s.name, myPeer.ID(), myPeer.ServiceName())

	aliasRegister(myPeer.ID(), myPeer.ID()+" ("+myPeer.ServiceName()+")")

	done0 := ctx0.Done()

	for {
		vv("%v: top of select", s.name) // client only seen once, since peer_test acts as cli
		select {
		// URL format: tcp://x.x.x.x:port/peerServiceName
		case pushToURL := <-s.PushToPeerURL:
			vv("%v: sees pushToURL '%v'", s.name, pushToURL)

			vv("%v: about to new up the server. pushToURL='%v'", s.name, pushToURL)
			ckt, ctx, err := myPeer.NewCircuitToPeerURL("push-cicuit", pushToURL, nil, nil)
			vv("%v: back from myPeer.NewCircuitToPeerURL(pushToURL: '%v'): err='%v'", s.name, pushToURL, err)
			panicOn(err)
			vv("%v: got ckt = '%v' back from NewCircuitToPeerURL '%v'", s.name, ckt.Name, pushToURL)
			s.gotOutgoingCkt <- ckt
			defer func() {
				vv("%v: (ckt '%v') defer running; closing ckt for pushToURL.", s.name, ckt.Name)
				ckt.Close()
				s.gotCktHaltReq.Close()
			}()

			done := ctx.Done()
			_ = done

			for {
				vv("%v: (ckt '%v'): top of select", s.name, ckt.Name)
				select {
				case frag := <-ckt.Reads:
					vv("%v: (ckt '%v') got from ckt.Reads ->  '%v'", s.name, ckt.Name, frag.String())

				case fragerr := <-ckt.Errors:
					vv("%v: (ckt '%v') got error fragerr back: '%#v'", s.name, ckt.Name, fragerr)
				case <-ckt.Halt.ReqStop.Chan:
					vv("%v: (ckt '%v') ckt halt requested.", s.name, ckt.Name)
					s.gotCktHaltReq.Close()
					return nil

				case <-done:
					vv("%v: (ckt '%v') done seen", s.name, ckt.Name)
					return nil
				case <-done0:
					vv("%v: (ckt '%v') done0 seen", s.name, ckt.Name)
					return nil
				case <-s.halt.ReqStop.Chan:
					vv("%v: (ckt '%v') topp func halt.ReqStop seen", s.name, ckt.Name)
					return nil
				}
			}

			//}(pushToURL)
		// new Circuit connection arrives
		case peer := <-newPeerCh:

			vv("%v: got from newPeerCh! service '%v' sees new peerURL: '%v'", s.name,
				peer.PeerServiceName(), peer.PeerURL())

			// talk to this peer on a separate goro if you wish:
			go func(peer RemotePeer) {

				ckt, ctx, err := peer.IncomingCircuit()
				if err != nil {
					vv("%v: RemotePeer err from IncomingCircuit: '%v'; stopping", s.name, err)
					return
				}
				vv("%v: (ckt '%v') got incoming ckt", s.name, ckt.Name)
				s.gotIncomingCkt <- ckt
				vv("%v: (ckt '%v') got past <-ckt for incoming ckt", s.name, ckt.Name)

				defer func() {
					vv("%v: (ckt '%v') defer running! finishing RemotePeer goro.", s.name, ckt.Name) // seen on server
					ckt.Close()
					s.gotCktHaltReq.Close()
				}()

				vv("%v: (ckt '%v') <- got new IncomingCircuit", s.name, ckt.Name)
				//vv("IncomingCircuit got RemoteCircuitURL = '%v'", ckt.RemoteCircuitURL())
				//vv("IncomingCircuit got LocalCircuitURL = '%v'", ckt.LocalCircuitURL())
				done := ctx.Done()

				for {
					select {
					case frag := <-ckt.Reads:
						vv("%v: (ckt %v) ckt.Reads sees frag:'%s'", s.name, ckt.Name, frag)

						s.gotIncomingCktReadFrag <- frag
						vv("%v: (ckt %v) past s.gotIncomingCktReadFrag <- frag. frag:'%s'", s.name, ckt.Name, frag)

						outFrag := NewFragment()
						outFrag.Payload = frag.Payload
						outFrag.FragSubject = "echo reply"
						outFrag.ServiceName = myPeer.ServiceName()
						vv("%v: (ckt '%v') sending 'echo reply'='%v'", s.name, ckt.Name, frag)
						err := peer.SendOneWay(ckt, outFrag, nil)
						panicOn(err)

					case fragerr := <-ckt.Errors:
						vv("%v: (ckt '%v') fragerr = '%v'", s.name, ckt.Name, fragerr)

					case <-ckt.Halt.ReqStop.Chan:
						vv("%v: (ckt '%v') ckt halt requested.", s.name, ckt.Name)
						s.gotCktHaltReq.Close()
						return

					case <-done:
						vv("%v: (ckt '%v') done!", s.name, ckt.Name)
						return
					case <-done0:
						vv("%v: (ckt '%v') done0!", s.name, ckt.Name)
						return
					case <-s.halt.ReqStop.Chan:
						vv("%v: (ckt '%v') top func halt.ReqStop seen", s.name, ckt.Name)
						return
					}
				}

			}(peer)

		case <-done0:
			vv("%v: done0!", s.name)
			return ErrContextCancelled
		case <-s.halt.ReqStop.Chan:
			vv("%v: halt.ReqStop seen", s.name)
			return ErrHaltRequested
		}
	}
	return nil
}
