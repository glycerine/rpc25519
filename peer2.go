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

// peer2_test.go uses this syncer2 test object.

func newSyncer2(name string) *syncer2 {
	return &syncer2{
		name:                   name,
		PushToPeerURL:          make(chan string),
		halt:                   idem.NewHalter(),
		gotIncomingCkt:         make(chan *Circuit),
		gotOutgoingCkt:         make(chan *Circuit),
		gotIncomingCktReadFrag: make(chan *Fragment, 10),

		gotCktHaltReq: idem.NewIdemCloseChan(),
	}
}

type syncer2 struct {
	name          string
	PushToPeerURL chan string
	halt          *idem.Halter

	gotOutgoingCkt chan *Circuit
	gotIncomingCkt chan *Circuit

	gotIncomingCktReadFrag chan *Fragment

	gotCktHaltReq *idem.IdemCloseChan
}

// instrument for peer2_test.go
func (s *syncer2) Start(
	myPeer *LocalPeer,
	ctx0 context.Context,
	newCircuitCh <-chan *Circuit,

) error {

	defer func() {
		//zz("%v: (%v) end of syncer.Start() inside defer, about the return/finish", s.name, myPeer.ServiceName())
		s.halt.Done.Close()
	}()

	//zz("%v: syncer.Start() top.", s.name)
	//zz("%v: ourID = '%v'; peerServiceName='%v';", s.name, myPeer.ID(), myPeer.ServiceName())

	AliasRegister(myPeer.PeerID, myPeer.PeerID+" ("+myPeer.ServiceName()+")")

	done0 := ctx0.Done()

	for {
		//zz("%v: top of select", s.name) // client only seen once, since peer_test acts as cli
		select {
		// new Circuit connection arrives
		case ckt := <-newCircuitCh:

			//zz("%v: got from newCircuitCh! service '%v' sees new peerURL: '%v'", s.name, peer.PeerServiceName(), peer.PeerURL())

			// talk to this peer on a separate goro if you wish:
			go func(ckt *Circuit) (err0 error) {

				ctx := ckt.Context
				//zz("%v: (ckt '%v') got incoming ckt", s.name, ckt.Name)
				s.gotIncomingCkt <- ckt
				//zz("%v: (ckt '%v') got past <-ckt for incoming ckt", s.name, ckt.Name)

				defer func() {
					//zz("%v: (ckt '%v') defer running! finishing RemotePeer goro.", s.name, ckt.Name) // seen on server
					ckt.Close(err0)
					s.gotCktHaltReq.Close()
				}()

				//zz("%v: (ckt '%v') <- got new incoming ckt", s.name, ckt.Name)
				////zz("incoming ckt has RemoteCircuitURL = '%v'", ckt.RemoteCircuitURL())
				////zz("incoming ckt has LocalCircuitURL = '%v'", ckt.LocalCircuitURL())
				done := ctx.Done()

				for {
					select {
					case frag := <-ckt.Reads:
						//zz("%v: (ckt %v) ckt.Reads sees frag:'%s'", s.name, ckt.Name, frag)

						s.gotIncomingCktReadFrag <- frag
						//zz("%v: (ckt %v) past s.gotIncomingCktReadFrag <- frag. frag:'%s'", s.name, ckt.Name, frag)

						outFrag := myPeer.U.NewFragment()
						outFrag.Payload = frag.Payload
						outFrag.FragSubject = "echo reply"
						outFrag.ServiceName = myPeer.ServiceName()
						//zz("%v: (ckt '%v') sending 'echo reply'='%v'", s.name, ckt.Name, frag)
						err := ckt.SendOneWay(outFrag, 0)
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
						return
					case <-done0:
						//zz("%v: (ckt '%v') done0!", s.name, ckt.Name)
						return
					case <-s.halt.ReqStop.Chan:
						//zz("%v: (ckt '%v') top func halt.ReqStop seen", s.name, ckt.Name)
						return
					}
				}

			}(ckt)

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
