package jsync

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
)

//go:generate greenpack

// OwnershipAccounts is sent once, before any file metadata, and shared by all
// the file circuits of one transfer. Large tables are split into disjoint
// batches; each ID/name pair is sent only once. Last ends the logical table.
type OwnershipAccounts struct {
	Users  map[int64]string `zid:"0"`
	Groups map[int64]string `zid:"1"`
	Last   bool             `zid:"2"`
}

type OwnershipHello struct {
	Session     uint64 `zid:"0"`
	RemoteTakes bool   `zid:"1"`
	Privileged  bool   `zid:"2"`
}

const ownershipCircuitName = "jsync ownership"
const (
	opOwnershipHello    = 44
	opOwnershipAccounts = 45
	opOwnershipReady    = 46
	opOwnershipEnd      = 47
)

//msgp:ignore ownershipTransfer
type ownershipTransfer struct {
	enabled  bool
	accounts *OwnershipAccounts
}

func (s *SyncService) ownershipTransfer(session uint64) *ownershipTransfer {
	if v, ok := s.ownershipTransfers.Load(session); ok {
		return v.(*ownershipTransfer)
	}
	return nil
}

func (s *SyncService) accountMaps(session uint64) *OwnershipAccounts {
	if tr := s.ownershipTransfer(session); tr != nil {
		return tr.accounts
	}
	return nil
}

func (s *SyncService) ownershipEnabled(session uint64) bool {
	tr := s.ownershipTransfer(session)
	return tr != nil && tr.enabled
}

func (s *SyncService) limitFileOwnership(f *File, session uint64) {
	if !s.ownershipEnabled(session) {
		f.OwnershipConveyed, f.UID, f.GID = false, 0, 0
	}
}

func (s *SyncService) limitPrecisOwnership(p *FilePrecis, session uint64) {
	if p != nil && !s.ownershipEnabled(session) {
		p.OwnershipConveyed, p.UID, p.GID = false, 0, 0
	}
}

func (s *SyncService) limitRequestOwnership(req *RequestToSyncPath) {
	if !s.ownershipEnabled(req.OwnershipSession) {
		req.OwnershipConveyed, req.UID, req.GID = false, 0, 0
	}
	// A taker's own metadata is not ownership to restore on the giver.
	if req.Precis != nil {
		req.Precis.OwnershipConveyed, req.Precis.UID, req.Precis.GID = false, 0, 0
	}
}

func (s *SyncService) senderFileMetadata(fi os.FileInfo, session uint64) *File {
	f := &File{FileMode: uint32(fi.Mode())}
	if s.ownershipEnabled(session) {
		f.OwnershipConveyed, f.UID, f.GID = fileOwnership(fi)
	}
	return f
}

func (s *SyncService) collectOwnership(enabled bool) (*ownershipTransfer, error) {
	tr := &ownershipTransfer{enabled: enabled}
	if !enabled {
		return tr, nil
	}
	load := localAccounts
	if s.sourceAccounts != nil {
		load = s.sourceAccounts
	}
	db, err := load()
	if err != nil {
		return nil, err
	}
	tr.accounts = &OwnershipAccounts{Users: db.users, Groups: db.groups, Last: true}
	return tr, nil
}

func sendOwnership(ckt *rpc.Circuit, op int, payload []byte) error {
	f := ckt.LpbFrom.NewFragment()
	f.FragOp = op
	f.Payload = payload
	_, err := ckt.SendOneWay(f, 0, 0)
	return err
}

func readOwnership(ctx context.Context, ckt *rpc.Circuit) (*rpc.Fragment, error) {
	select {
	case f := <-ckt.Reads:
		if f == nil {
			return nil, fmt.Errorf("ownership circuit closed")
		}
		if f.Err != "" {
			return nil, fmt.Errorf("ownership negotiation: %s", f.Err)
		}
		return f, nil
	case f := <-ckt.Errors:
		return nil, fmt.Errorf("ownership circuit error: %v", f)
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-ckt.Context.Done():
		return nil, ckt.Context.Err()
	}
}

func expectOwnership(ctx context.Context, ckt *rpc.Circuit, op int) (*rpc.Fragment, error) {
	f, err := readOwnership(ctx, ckt)
	if err != nil {
		return nil, err
	}
	if int(f.FragOp) != op {
		return nil, fmt.Errorf("ownership protocol: got %v, want %v", f.FragOp, op)
	}
	return f, nil
}

func sendOwnershipAccounts(ckt *rpc.Circuit, accounts *OwnershipAccounts) error {
	batch := &OwnershipAccounts{Users: make(map[int64]string), Groups: make(map[int64]string)}
	flush := func(last bool) error {
		batch.Last = last
		b, err := batch.MarshalMsg(nil)
		if err != nil {
			return err
		}
		if len(b) > rpc.UserMaxPayload {
			return fmt.Errorf("ownership account batch exceeds payload limit")
		}
		if err = sendOwnership(ckt, opOwnershipAccounts, b); err != nil {
			return err
		}
		batch = &OwnershipAccounts{Users: make(map[int64]string), Groups: make(map[int64]string)}
		return nil
	}
	for id, name := range accounts.Users {
		batch.Users[id] = name
		if len(batch.Users)+len(batch.Groups) == 512 {
			if err := flush(false); err != nil {
				return err
			}
		}
	}
	for id, name := range accounts.Groups {
		batch.Groups[id] = name
		if len(batch.Users)+len(batch.Groups) == 512 {
			if err := flush(false); err != nil {
				return err
			}
		}
	}
	return flush(true)
}

func receiveOwnershipAccounts(ctx context.Context, ckt *rpc.Circuit) (*OwnershipAccounts, error) {
	accounts := &OwnershipAccounts{Users: make(map[int64]string), Groups: make(map[int64]string)}
	for {
		f, err := expectOwnership(ctx, ckt, opOwnershipAccounts)
		if err != nil {
			return nil, err
		}
		var batch OwnershipAccounts
		if _, err = batch.UnmarshalMsg(f.Payload); err != nil {
			return nil, err
		}
		for id, name := range batch.Users {
			accounts.Users[id] = name
		}
		for id, name := range batch.Groups {
			accounts.Groups[id] = name
		}
		if batch.Last {
			accounts.Last = true
			return accounts, nil
		}
	}
}

// This runs outside Start's select loop so simultaneous transfers in opposite
// directions can still accept each other's negotiation circuits.
func (s *SyncService) prepareOwnership(ctx context.Context, peer *rpc.LocalPeer, req *RequestToSyncPath) {
	var ckt *rpc.Circuit
	var err error
	defer func() {
		if err != nil {
			req.Errs = err.Error()
			s.ownershipTransfers.Delete(req.OwnershipSession)
			if ckt != nil {
				ckt.Close(err)
			}
			req.Done.Close()
		}
	}()
	if req.Done == nil {
		req.Done = idem.NewIdemCloseChan()
	}
	if !req.RemoteTakes && privilegedReceiver() {
		if _, err = localAccounts(); err != nil {
			return
		}
	}
	var id [8]byte
	if _, err = rand.Read(id[:]); err != nil {
		return
	}
	req.OwnershipSession = binary.BigEndian.Uint64(id[:])
	hello := &OwnershipHello{Session: req.OwnershipSession, RemoteTakes: req.RemoteTakes, Privileged: !req.RemoteTakes && privilegedReceiver()}
	var b []byte
	if b, err = hello.MarshalMsg(nil); err != nil {
		return
	}
	f := peer.NewFragment()
	f.FragOp = opOwnershipHello
	f.Payload = b
	ckt, _, _, _, err = s.U.StartRemotePeerAndGetCircuit(peer, ownershipCircuitName, "", f, req.ToRemotePeerServiceName, "", req.ToRemoteNetAddr, 0, false, nil, false)
	if err != nil {
		return
	}
	var tr *ownershipTransfer
	if req.RemoteTakes {
		var reply *rpc.Fragment
		if reply, err = expectOwnership(ctx, ckt, opOwnershipHello); err != nil {
			return
		}
		if _, err = hello.UnmarshalMsg(reply.Payload); err != nil {
			return
		}
		if tr, err = s.collectOwnership(hello.Privileged); err != nil {
			return
		}
		if tr.enabled {
			err = sendOwnershipAccounts(ckt, tr.accounts)
		} else {
			err = sendOwnership(ckt, opOwnershipReady, nil)
		}
		if err != nil {
			return
		}
		if _, err = expectOwnership(ctx, ckt, opOwnershipReady); err != nil {
			return
		}
	} else {
		tr = &ownershipTransfer{enabled: hello.Privileged}
		if tr.enabled {
			tr.accounts, err = receiveOwnershipAccounts(ctx, ckt)
		} else {
			_, err = expectOwnership(ctx, ckt, opOwnershipReady)
		}
		if err != nil {
			return
		}
		if err = sendOwnership(ckt, opOwnershipReady, nil); err != nil {
			return
		}
	}
	if tr.enabled {
		s.ownershipTransfers.Store(req.OwnershipSession, tr)
	} else {
		req.OwnershipSession = 0
	}
	req.accounts = tr.accounts
	go func() {
		select {
		case <-req.Done.Chan:
		case <-ctx.Done():
		}
		_ = sendOwnership(ckt, opOwnershipEnd, nil)
		ckt.Close(nil)
		s.ownershipTransfers.Delete(req.OwnershipSession)
	}()
	select {
	case s.ownershipReady <- req:
	case <-ctx.Done():
		err = ctx.Err()
	}
}

func (s *SyncService) serveOwnership(ctx context.Context, ckt *rpc.Circuit) {
	var err error
	var session uint64
	defer func() {
		if err != nil {
			f := ckt.LpbFrom.NewFragment()
			f.Err = err.Error()
			_, _ = ckt.SendOneWay(f, 0, 0)
		}
		s.ownershipTransfers.Delete(session)
		ckt.Close(err)
	}()
	var first *rpc.Fragment
	if first, err = expectOwnership(ctx, ckt, opOwnershipHello); err != nil {
		return
	}
	var hello OwnershipHello
	if _, err = hello.UnmarshalMsg(first.Payload); err != nil {
		return
	}
	session = hello.Session
	var tr *ownershipTransfer
	if hello.RemoteTakes {
		hello.Privileged = privilegedReceiver()
		if hello.Privileged {
			if _, err = localAccounts(); err != nil {
				return
			}
		}
		var b []byte
		if b, err = hello.MarshalMsg(nil); err != nil {
			return
		}
		if err = sendOwnership(ckt, opOwnershipHello, b); err != nil {
			return
		}
		tr = &ownershipTransfer{enabled: hello.Privileged}
		if tr.enabled {
			tr.accounts, err = receiveOwnershipAccounts(ctx, ckt)
		} else {
			_, err = expectOwnership(ctx, ckt, opOwnershipReady)
		}
		if err != nil {
			return
		}
		if tr.enabled {
			s.ownershipTransfers.Store(session, tr)
		}
		if err = sendOwnership(ckt, opOwnershipReady, nil); err != nil {
			return
		}
	} else {
		if tr, err = s.collectOwnership(hello.Privileged); err != nil {
			return
		}
		if tr.enabled {
			s.ownershipTransfers.Store(session, tr)
		}
		if tr.enabled {
			err = sendOwnershipAccounts(ckt, tr.accounts)
		} else {
			err = sendOwnership(ckt, opOwnershipReady, nil)
		}
		if err != nil {
			return
		}
		if _, err = expectOwnership(ctx, ckt, opOwnershipReady); err != nil {
			return
		}
	}
	_, err = expectOwnership(ctx, ckt, opOwnershipEnd)
}
