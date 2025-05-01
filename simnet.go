package rpc25519

/* all moved to simnet_test.go
// simulated network.
// same send call as UniversalCliSrv
// interface

import (
	"context"
	//"testing/synctest"
	"time"

	"github.com/glycerine/idem"
)

var _ UniversalCliSrv = &SimNet{}

type SimNet struct {
	ckts map[string]*Circuit
}

func NewSimNet() *SimNet {
	return &SimNet{
		ckts: make(map[string]*Circuit),
	}
}

func (s *SimNet) AddPeer(peerID string, ckt *Circuit) (err error) {
	s.ckts[peerID] = ckt
	return nil
}

func (s *SimNet) SendOneWayMessage(ctx context.Context, msg *Message, errWriteDur time.Duration) (error, chan *Message) {
	panic("TODO")
	return nil, nil
}

func (s *SimNet) GetConfig() *Config {
	panic("TODO")
	return nil
}
func (s *SimNet) RegisterPeerServiceFunc(peerServiceName string, psf PeerServiceFunc) error {
	panic("TODO")
	return nil
}

func (s *SimNet) StartLocalPeer(ctx context.Context, peerServiceName string, requestedCircuit *Message) (lpb *LocalPeer, err error) {
	panic("TODO")
	return
}

func (s *SimNet) StartRemotePeer(ctx context.Context, peerServiceName, remoteAddr string, waitUpTo time.Duration) (remotePeerURL, RemotePeerID string, err error) {
	panic("TODO")
	return
}

func (s *SimNet) StartRemotePeerAndGetCircuit(lpb *LocalPeer, circuitName string, frag *Fragment, peerServiceName, remoteAddr string, waitUpTo time.Duration) (ckt *Circuit, err error) {
	panic("TODO")
	return nil, nil
}

func (s *SimNet) GetReadsForCallID(ch chan *Message, callID string) {
	panic("TODO")
	return
}
func (s *SimNet) GetErrorsForCallID(ch chan *Message, callID string) {
	panic("TODO")
	return
}

// for Peer/Object systems; ToPeerID gets priority over CallID
// to allow such systems to implement custom message
// types. An example is the Fragment/Peer/Circuit system.
// (This priority is implemented in notifies.handleReply_to_CallID_ToPeerID).
func (s *SimNet) GetReadsForToPeerID(ch chan *Message, objID string) {
	panic("TODO")
	return
}
func (s *SimNet) GetErrorsForToPeerID(ch chan *Message, objID string) {
	panic("TODO")
	return
}

func (s *SimNet) UnregisterChannel(ID string, whichmap int) {
	panic("TODO")
	return
}
func (s *SimNet) LocalAddr() string {
	panic("TODO")
	return ""
}
func (s *SimNet) RemoteAddr() string { // client provides, server gives ""
	panic("TODO")
	return ""
}

// allow peers to find out that the host Client/Server is stopping.
func (s *SimNet) GetHostHalter() *idem.Halter {
	panic("TODO")
	return nil
}

// fragment memory recycling, to avoid heap pressure.
func (s *SimNet) NewFragment() *Fragment {
	panic("TODO")
	return nil
}
func (s *SimNet) FreeFragment(frag *Fragment) {
	panic("TODO")
	return
}
func (s *SimNet) RecycleFragLen() int {
	panic("TODO")
	return 0
}
func (s *SimNet) PingStats(remote string) *PingStat {
	panic("TODO")
	return nil
}
func (s *SimNet) AutoClients() (list []*Client, isServer bool) {
	panic("TODO")
	return
}
*/
