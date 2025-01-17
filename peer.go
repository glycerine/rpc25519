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
	myPeer *LocalPeer,
	ctx0 context.Context,
	newPeerCh <-chan *RemotePeer,

) error {

	defer func() {
		//zz("%v: (%v) end of syncer.Start() inside defer, about the return/finish", s.name, myPeer.ServiceName())
		s.halt.Done.Close()
	}()

	//zz("%v: syncer.Start() top.", s.name)
	//zz("%v: ourID = '%v'; peerServiceName='%v';", s.name, myPeer.ID(), myPeer.ServiceName())

	aliasRegister(myPeer.PeerID, myPeer.PeerID+" ("+myPeer.ServiceName()+")")

	done0 := ctx0.Done()

	for {
		//zz("%v: top of select", s.name) // client only seen once, since peer_test acts as cli
		select {
		// URL format: tcp://x.x.x.x:port/peerServiceName
		case pushToURL := <-s.PushToPeerURL:
			//zz("%v: sees pushToURL '%v'", s.name, pushToURL)

			//zz("%v: about to new up the server. pushToURL='%v'", s.name, pushToURL)
			ckt, ctx, err := myPeer.NewCircuitToPeerURL("push-cicuit", pushToURL, nil, 0)
			//zz("%v: back from myPeer.NewCircuitToPeerURL(pushToURL: '%v'): err='%v'", s.name, pushToURL, err)
			panicOn(err)
			//zz("%v: got ckt = '%v' back from NewCircuitToPeerURL '%v'", s.name, ckt.Name, pushToURL)
			s.gotOutgoingCkt <- ckt
			defer func() {
				//zz("%v: (ckt '%v') defer running; closing ckt for pushToURL.", s.name, ckt.Name)
				ckt.Close()
				s.gotCktHaltReq.Close()
			}()

			done := ctx.Done()
			_ = done

			for {
				//zz("%v: (ckt '%v'): top of select", s.name, ckt.Name)
				select {
				case frag := <-ckt.Reads:
					//zz("%v: (ckt '%v') got from ckt.Reads ->  '%v'", s.name, ckt.Name, frag.String())
					_ = frag

				case fragerr := <-ckt.Errors:
					//zz("%v: (ckt '%v') got error fragerr back: '%#v'", s.name, ckt.Name, fragerr)
					_ = fragerr
				case <-ckt.Halt.ReqStop.Chan:
					//zz("%v: (ckt '%v') ckt halt requested.", s.name, ckt.Name)
					s.gotCktHaltReq.Close()
					return nil

				case <-done:
					//zz("%v: (ckt '%v') done seen", s.name, ckt.Name)
					return nil
				case <-done0:
					//zz("%v: (ckt '%v') done0 seen", s.name, ckt.Name)
					return nil
				case <-s.halt.ReqStop.Chan:
					//zz("%v: (ckt '%v') topp func halt.ReqStop seen", s.name, ckt.Name)
					return nil
				}
			}

			//}(pushToURL)
		// new Circuit connection arrives
		case peer := <-newPeerCh:

			//zz("%v: got from newPeerCh! service '%v' sees new peerURL: '%v'", s.name, peer.PeerServiceName(), peer.PeerURL())

			// talk to this peer on a separate goro if you wish:
			go func(peer *RemotePeer) {

				ckt, ctx, err := peer.IncomingCircuit()
				if err != nil {
					//zz("%v: RemotePeer err from IncomingCircuit: '%v'; stopping", s.name, err)
					return
				}
				//zz("%v: (ckt '%v') got incoming ckt", s.name, ckt.Name)
				s.gotIncomingCkt <- ckt
				//zz("%v: (ckt '%v') got past <-ckt for incoming ckt", s.name, ckt.Name)

				defer func() {
					//zz("%v: (ckt '%v') defer running! finishing RemotePeer goro. stack = '%v'", s.name, ckt.Name, stack()) // seen on server
					ckt.Close()
					s.gotCktHaltReq.Close()
				}()

				//zz("%v: (ckt '%v') <- got new IncomingCircuit", s.name, ckt.Name)
				////zz("IncomingCircuit got RemoteCircuitURL = '%v'", ckt.RemoteCircuitURL())
				////zz("IncomingCircuit got LocalCircuitURL = '%v'", ckt.LocalCircuitURL())
				done := ctx.Done()

				for {
					select {
					case frag := <-ckt.Reads:
						//zz("%v: (ckt %v) ckt.Reads sees frag:'%s'", s.name, ckt.Name, frag)

						s.gotIncomingCktReadFrag <- frag
						//zz("%v: (ckt %v) past s.gotIncomingCktReadFrag <- frag. frag:'%s'", s.name, ckt.Name, frag)

						outFrag := NewFragment()
						outFrag.Payload = frag.Payload
						outFrag.FragSubject = "echo reply"
						outFrag.ServiceName = myPeer.ServiceName()
						//zz("%v: (ckt '%v') sending 'echo reply'='%v'", s.name, ckt.Name, frag)
						err := peer.SendOneWay(ckt, outFrag, 0)
						panicOn(err)

					case fragerr := <-ckt.Errors:
						//zz("%v: (ckt '%v') fragerr = '%v'", s.name, ckt.Name, fragerr)
						_ = fragerr

					case <-ckt.Halt.ReqStop.Chan:
						//zz("%v: (ckt '%v') ckt halt requested.", s.name, ckt.Name)
						s.gotCktHaltReq.Close()
						return

					case <-done:
						//zz("%v: (ckt '%v') done!", s.name, ckt.Name)
						return // server finishing on done!
					case <-done0:
						//zz("%v: (ckt '%v') done0!", s.name, ckt.Name)
						return
					case <-s.halt.ReqStop.Chan:
						//zz("%v: (ckt '%v') top func halt.ReqStop seen", s.name, ckt.Name)
						return
					}
				}

			}(peer)

		case <-done0:
			//zz("%v: done0!", s.name)
			return ErrContextCancelled
		case <-s.halt.ReqStop.Chan:
			//zz("%v: halt.ReqStop seen", s.name)
			return ErrHaltRequested
		}
	}
	return nil
}
