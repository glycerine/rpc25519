package rpc25519

import (
	"fmt"

	rb "github.com/glycerine/rbtree"
)

type pq struct {
	Owner   string
	Orderby string
	Tree    *rb.Tree

	// don't export so user does not
	// accidentally mess with it.
	cmp func(a, b rb.Item) int

	isDeafQ bool
}

func (s *pq) Len() int {
	return s.Tree.Len()
}

// deep meaning we clone the *mop contents as
// well as the tree. the *mop themselves are a
// shallow cloned to avoid infinite loop on cycles
// of pointers.
func (s *pq) deepclone() (c *pq) {

	c = &pq{
		Owner:   s.Owner,
		Orderby: s.Orderby,
		Tree:    rb.NewTree(s.cmp),
		// cmp is shared by simnet and out to user goro
		// without locking; it is a pure function
		// though, so this should be fine--also this saves us
		// from having to know exactly which of thee
		// three possible PQ ordering functions we have.
		cmp: s.cmp,
	}
	for it := s.Tree.Min(); !it.Limit(); it = it.Next() {
		c.Tree.Insert(it.Item().(*mop).clone())
	}
	return
}

func (s *pq) peek() *mop {
	n := s.Tree.Len()
	if n == 0 {
		return nil
	}
	it := s.Tree.Min()
	if it.Limit() {
		panic("n > 0 above, how is this possible?")
		return nil
	}
	return it.Item().(*mop)
}

func (s *pq) pop() *mop {
	n := s.Tree.Len()
	//vv("pop n = %v", n)
	if n == 0 {
		return nil
	}
	it := s.Tree.Min()
	if it.Limit() {
		panic("n > 0 above, how is this possible?") // hitting this for releasableQ
		return nil
	}
	top := it.Item().(*mop)
	s.Tree.DeleteWithIterator(it)
	return top
}

func (s *pq) add(op *mop) (added bool, it rb.Iterator) {
	if op == nil {
		panic("do not put nil into pq!")
	}
	//if s.isDeafQ && op.kind == READ { // sends go in too.
	//	panic(fmt.Sprintf("where read added to deafReadQ: '%v'", op))
	//}
	added, it = s.Tree.InsertGetIt(op) // race write vs write self
	return
}

func (s *pq) del(op *mop) (found bool) {
	if op == nil {
		panic("cannot delete nil mop!")
	}
	var it rb.Iterator
	it, found = s.Tree.FindGE_isEqual(op)
	if !found {
		return
	}
	s.Tree.DeleteWithIterator(it)
	return
}

func (s *pq) deleteAll() {
	s.Tree.DeleteAll()
	return
}

// order by arrivalTm; for the pre-arrival preArrQ.
//
// Note: must be deterministic iteration order! Don't
// use random tie breakers in here.
// Otherwise we might decide, as the dispatcher does,
// that the mop we wanted to delete on the first
// pass is not there when we look again, or vice-versa.
// We learned this the hard way.
func (s *Simnet) newPQarrivalTm(owner string) *pq {
	cmp := func(a, b rb.Item) int {
		av := a.(*mop)
		bv := b.(*mop)

		if av == bv {
			return 0 // points to same memory (or both nil)
		}
		if av == nil {
			// just a is nil; b is not. sort nils to the front
			// so they get popped and GC-ed sooner (and
			// don't become temporary memory leaks by sitting at the
			// back of the queue.
			panic("no nils in pq")
			return -1
		}
		if bv == nil {
			panic("no nils in pq")
			return 1
		}
		// INVAR: neither av nor bv is nil
		if av == bv {
			return 0 // pointer equality is immediate
		}
		if av.sn == bv.sn {
			return 0 // preserve delete of same sn
		}

		if av.arrivalTm.Before(bv.arrivalTm) {
			return -1
		}
		if av.arrivalTm.After(bv.arrivalTm) {
			return 1
		}
		apri := av.priority()
		bpri := bv.priority()
		if apri < bpri {
			return -1
		}
		if apri > bpri {
			return 1
		}
		// same priority, i.e. both reads

		// some alt from <-s.alterHostCh, newAlterHostMop(alt)) had
		// mop.origin nil, so try not to seg fault on it.
		if av.origin == nil && bv.origin != nil {
			// seen in tube tests! on client register.
			return -1
		}
		if av.origin != nil && bv.origin == nil {
			return 1
		}
		// we want to handle the both origin == nil cases
		// here with bestName to use the earlyName.
		aname := chompAnyUniqSuffix(av.bestName())
		bname := chompAnyUniqSuffix(bv.bestName())
		if aname < bname {
			return -1
		}
		if aname > bname {
			return 1
		}

		// timers might not have target...

		if av.target == nil && bv.target != nil {
			return -1
		}
		if av.target != nil && bv.target == nil {
			return 1
		}

		if av.target != nil && bv.target != nil {
			atname := chompAnyUniqSuffix(av.target.name)
			btname := chompAnyUniqSuffix(bv.target.name)
			if atname < btname {
				return -1
			}
			if atname > btname {
				return 1
			}
		}
		// same origin, same target.
		aw := av.whence()
		bw := bv.whence()
		if aw < bw {
			return -1
		}
		if aw > bw {
			return 1
		}

		// logical clocks change, maybe, avoid.
		// av.senderLC < av.senderLC

		acli := av.cliOrSrvString()
		bcli := bv.cliOrSrvString()
		if acli < bcli {
			return -1
		}
		if acli > bcli {
			return 1
		}
		// either both client or both server

		atm := av.tm()
		btm := bv.tm()
		if atm.Before(btm) {
			return -1
		}
		if atm.After(btm) {
			return 1
		}

		a0 := av.dispatchTm.IsZero()
		b0 := bv.dispatchTm.IsZero()
		if a0 && !b0 {
			return -1
		}
		if !a0 && b0 {
			return 1
		}
		if !a0 && !b0 {
			if av.dispatchTm.Before(bv.dispatchTm) {
				return -1
			}
			if av.dispatchTm.After(bv.dispatchTm) {
				return 1
			}
		}

		// sn are non-deterministic. only use as an
		// extreme last resort. tube has some srv.go:2272
		// calls that do look virtually identical...

		if av.sn < bv.sn {
			return -1
		}
		if av.sn > bv.sn {
			return 1
		}
		// must be the same if same sn.
		return 0
	}
	return &pq{
		Owner:   owner,
		Orderby: "arrivalTm",
		Tree:    rb.NewTree(cmp),
		cmp:     cmp,
	}
}

// order by mop.initTm, then mop.sn;
// for reads (readQ).
func newPQinitTm(owner string, isDeafQ bool) *pq {

	q := &pq{
		Owner:   owner,
		Orderby: "initTm",
		isDeafQ: isDeafQ,
	}
	cmp := func(a, b rb.Item) int {
		av := a.(*mop)
		bv := b.(*mop)

		if av == bv {
			return 0 // points to same memory (or both nil)
		}
		if av == nil {
			// just a is nil; b is not. sort nils to the front
			// so they get popped and GC-ed sooner (and
			// don't become temporary memory leaks by sitting at the
			// back of the queue.
			panic("no nils allowed in tree")
			return -1
		}
		if bv == nil {
			panic("no nils allowed in tree")
			return 1
		}
		// INVAR: neither av nor bv is nil
		if av.sn == bv.sn {
			return 0
		}

		if av.initTm.Before(bv.initTm) {
			return -1
		}
		if av.initTm.After(bv.initTm) {
			return 1
		}
		// INVAR initTm equal, but delivery order should not matter.
		// but go for determinism.

		apri := av.priority()
		bpri := bv.priority()
		if apri < bpri {
			return -1
		}
		if apri > bpri {
			return 1
		}
		// same priority, i.e. both reads

		// some alt from <-s.alterHostCh, newAlterHostMop(alt)) had
		// mop.origin nil, so try not to seg fault on it.
		if av.origin == nil && bv.origin != nil {
			// seen in tube tests! on client register.
			return -1
		}
		if av.origin != nil && bv.origin == nil {
			return 1
		}
		// we want to handle the both origin == nil cases
		// here with bestName to use the earlyName.
		aname := chompAnyUniqSuffix(av.bestName())
		bname := chompAnyUniqSuffix(bv.bestName())
		if aname < bname {
			return -1
		}
		if aname > bname {
			return 1
		}

		// timers might not have target...

		if av.target == nil && bv.target != nil {
			return -1
		}
		if av.target != nil && bv.target == nil {
			return 1
		}

		if av.target != nil && bv.target != nil {
			atname := chompAnyUniqSuffix(av.target.name)
			btname := chompAnyUniqSuffix(bv.target.name)
			if atname < btname {
				return -1
			}
			if atname > btname {
				return 1
			}
		}
		// same origin, same target.
		aw := av.whence()
		bw := bv.whence()
		if aw < bw {
			return -1
		}
		if aw > bw {
			return 1
		}

		// logical clocks change, maybe, avoid.
		// av.senderLC < av.senderLC

		acli := av.cliOrSrvString()
		bcli := bv.cliOrSrvString()
		if acli < bcli {
			return -1
		}
		if acli > bcli {
			return 1
		}
		// either both client or both server

		if av.sn < bv.sn {
			return -1
		}
		if av.sn > bv.sn {
			return 1
		}
		// must be the same if same sn.
		return 0
	}
	q.Tree = rb.NewTree(cmp)
	q.cmp = cmp
	return q
}

// order by mop.completeTm then mop.sn; for timers
func newPQcompleteTm(owner string) *pq {
	cmp := func(a, b rb.Item) int {
		av := a.(*mop)
		bv := b.(*mop)

		if av == bv {
			return 0 // points to same memory (or both nil)
		}
		if av == nil {
			// just a is nil; b is not. sort nils to the front
			// so they get popped and GC-ed sooner (and
			// don't become temporary memory leaks by sitting at the
			// back of the queue.
			panic("no nils in pq")
			return -1
		}
		if bv == nil {
			panic("no nils in pq")
			return 1
		}
		// INVAR: neither av nor bv is nil
		if av.sn == bv.sn {
			return 0
		}

		if av.completeTm.Before(bv.completeTm) {
			return -1
		}
		if av.completeTm.After(bv.completeTm) {
			return 1
		}
		// INVAR when equal, delivery order should not matter.
		// but strive for determinism:

		apri := av.priority()
		bpri := bv.priority()
		if apri < bpri {
			return -1
		}
		if apri > bpri {
			return 1
		}
		// same priority, i.e. both reads

		// some alt from <-s.alterHostCh, newAlterHostMop(alt)) had
		// mop.origin nil, so try not to seg fault on it.
		if av.origin == nil && bv.origin != nil {
			// seen in tube tests! on client register.
			return -1
		}
		if av.origin != nil && bv.origin == nil {
			return 1
		}
		// we want to handle the both origin == nil cases
		// here with bestName to use the earlyName.
		aname := chompAnyUniqSuffix(av.bestName())
		bname := chompAnyUniqSuffix(bv.bestName())
		if aname < bname {
			return -1
		}
		if aname > bname {
			return 1
		}

		// timers might not have target...

		if av.target == nil && bv.target != nil {
			return -1
		}
		if av.target != nil && bv.target == nil {
			return 1
		}

		if av.target != nil && bv.target != nil {
			atname := chompAnyUniqSuffix(av.target.name)
			btname := chompAnyUniqSuffix(bv.target.name)
			if atname < btname {
				return -1
			}
			if atname > btname {
				return 1
			}
		}
		// same origin, same target.
		aw := av.whence()
		bw := bv.whence()
		if aw < bw {
			return -1
		}
		if aw > bw {
			return 1
		}

		// logical clocks change, maybe, avoid.
		// av.senderLC < av.senderLC

		acli := av.cliOrSrvString()
		bcli := bv.cliOrSrvString()
		if acli < bcli {
			return -1
		}
		if acli > bcli {
			return 1
		}
		// either both client or both server

		// desparate...
		if av.sn < bv.sn {
			return -1
		}
		if av.sn > bv.sn {
			return 1
		}
		// must be the same if same sn.
		return 0
	}
	return &pq{
		Owner:   owner,
		Orderby: "completeTm",
		Tree:    rb.NewTree(cmp),
		cmp:     cmp,
	}
}

// mop.sn assignment by client code is
// non-deterministic. the client or the
// server could get their read request allocated a sn first,
// for example. so avoid using .sn to break ties
// until after looking at origin:target.
func newMasterEventQueue(owner string) *pq {

	cmp := func(a, b rb.Item) int {
		av := a.(*mop)
		bv := b.(*mop)

		if av == nil {
			panic("no nil events allowed in the meq")
			return -1
		}
		if bv == nil {
			panic("no nil events allowed in the meq")
			return 1
		}
		asn := av.sn
		bsn := bv.sn
		// be sure to keep delete by sn working
		if asn == bsn {
			return 0
		}
		atm := av.tm()
		btm := bv.tm()
		if atm.Before(btm) {
			return -1
		}
		if atm.After(btm) {
			return 1
		}

		apri := av.priority()
		bpri := bv.priority()
		if apri < bpri {
			return -1
		}
		if apri > bpri {
			return 1
		}
		// same priority, i.e. both reads

		// some alt from <-s.alterHostCh, newAlterHostMop(alt)) had
		// mop.origin nil, so try not to seg fault on it.
		if av.origin == nil && bv.origin != nil {
			return -1
		}
		if av.origin != nil && bv.origin == nil {
			return 1
		}
		// we want to handle the both origin == nil cases
		// here with bestName to use the earlyName.
		aname := chompAnyUniqSuffix(av.bestName())
		bname := chompAnyUniqSuffix(bv.bestName())
		if aname < bname {
			return -1
		}
		if aname > bname {
			return 1
		}

		// timers might not have target...

		if av.target == nil && bv.target != nil {
			return -1
		}
		if av.target != nil && bv.target == nil {
			return 1
		}

		if av.target != nil && bv.target != nil {
			atname := chompAnyUniqSuffix(av.target.name)
			btname := chompAnyUniqSuffix(bv.target.name)
			if atname < btname {
				return -1
			}
			if atname > btname {
				return 1
			}
		}

		// same origin, same target.
		aw := av.whence()
		bw := bv.whence()
		if aw < bw {
			return -1
		}
		if aw > bw {
			return 1
		}
		// possible addition:
		acli := av.cliOrSrvString()
		bcli := bv.cliOrSrvString()
		if acli < bcli {
			return -1
		}
		if acli > bcli {
			return 1
		}
		// either both client or both server

		if asn < bsn {
			return -1
		}
		if asn > bsn {
			return 1
		}
		return 0
	}
	return &pq{
		Owner:   owner,
		Orderby: "masterMEQ tm() then priority() then name...",
		Tree:    rb.NewTree(cmp),
		cmp:     cmp,
	}
}

func newOneTimeSliceQ(owner string) *pq {

	cmp := func(a, b rb.Item) int {
		av := a.(*mop)
		bv := b.(*mop)

		if av == nil {
			panic("no nil events allowed in the oneTimeSliceQ")
			return -1
		}
		if bv == nil {
			panic("no nil events allowed in the oneTimeSliceQ")
			return 1
		}
		asn := av.sn
		bsn := bv.sn
		// be sure to keep delete by sn working
		if asn == bsn {
			return 0
		}
		if av.pseudorandom < bv.pseudorandom {
			return -1
		}
		if av.pseudorandom > bv.pseudorandom {
			return 1
		}

		apri := av.priority()
		bpri := bv.priority()
		if apri < bpri {
			return -1
		}
		if apri > bpri {
			return 1
		}
		// same priority, i.e. both reads

		// chompAnyUniqSuffix(op.bestName()), op.whence())

		// some alt from <-s.alterHostCh, newAlterHostMop(alt)) had
		// mop.origin nil, so try not to seg fault on it.
		if av.origin == nil && bv.origin != nil {
			// seen in tube tests! on client register.
			return -1
		}
		if av.origin != nil && bv.origin == nil {
			return 1
		}
		// we want to handle the both origin == nil cases
		// here with bestName to use the earlyName.
		aname := chompAnyUniqSuffix(av.bestName())
		bname := chompAnyUniqSuffix(bv.bestName())
		if aname < bname {
			return -1
		}
		if aname > bname {
			return 1
		}

		// timers might not have target...

		if av.target == nil && bv.target != nil {
			return -1
		}
		if av.target != nil && bv.target == nil {
			return 1
		}

		if av.target != nil && bv.target != nil {
			atname := chompAnyUniqSuffix(av.target.name)
			btname := chompAnyUniqSuffix(bv.target.name)
			if atname < btname {
				return -1
			}
			if atname > btname {
				return 1
			}
		}
		// same origin, same target.
		aw := av.whence()
		bw := bv.whence()
		if aw < bw {
			return -1
		}
		if aw > bw {
			return 1
		}
		acli := av.cliOrSrvString()
		bcli := bv.cliOrSrvString()
		if acli < bcli {
			return -1
		}
		if acli > bcli {
			return 1
		}
		// either both client or both server

		atm := av.tm()
		btm := bv.tm()
		if atm.Before(btm) {
			return -1
		}
		if atm.After(btm) {
			return 1
		}

		// sn are non-deterministic. only use as an
		// extreme last resort. tube has some srv.go:2272
		// calls that do look virtually identical...
		// av='mop{SERVER(srv_node_4) TIMER init:0s, arr:unk, complete:5s op.sn:286, who:654, msg.sn:0 timer set at srv.go:2272}'
		// bv='mop{SERVER(srv_node_4) TIMER init:0s, arr:unk, complete:5s op.sn:283, who:656, msg.sn:0 timer set at srv.go:2272}'

		if asn < bsn {
			return -1
		}
		if asn > bsn {
			return 1
		}
		return 0
	}
	return &pq{
		Owner:   owner,
		Orderby: "oneTimeSliceQ priority() then name...",
		Tree:    rb.NewTree(cmp),
		cmp:     cmp,
	}
}

func newReleasableQueue(owner string) *pq {

	cmp := func(a, b rb.Item) int {
		av := a.(*mop)
		bv := b.(*mop)

		if av == nil {
			panic("no nil events allowed in the oneTimeSliceQ")
			return -1
		}
		if bv == nil {
			panic("no nil events allowed in the oneTimeSliceQ")
			return 1
		}
		asn := av.sn
		bsn := bv.sn
		// be sure to keep delete by sn working
		if asn == bsn {
			return 0
		}

		if av.issueBatch < bv.issueBatch {
			return -1
		}
		if av.issueBatch > bv.issueBatch {
			return 1
		}

		apri := av.priority()
		bpri := bv.priority()
		if apri < bpri {
			return -1
		}
		if apri > bpri {
			return 1
		}
		// same priority, i.e. both reads

		// chompAnyUniqSuffix(op.bestName()), op.whence())

		// some alt from <-s.alterHostCh, newAlterHostMop(alt)) had
		// mop.origin nil, so try not to seg fault on it.
		if av.origin == nil && bv.origin != nil {
			// seen in tube tests! on client register.
			return -1
		}
		if av.origin != nil && bv.origin == nil {
			return 1
		}
		// we want to handle the both origin == nil cases
		// here with bestName to use the earlyName.
		aname := chompAnyUniqSuffix(av.bestName())
		bname := chompAnyUniqSuffix(bv.bestName())
		if aname < bname {
			return -1
		}
		if aname > bname {
			return 1
		}

		// timers might not have target...

		if av.target == nil && bv.target != nil {
			return -1
		}
		if av.target != nil && bv.target == nil {
			return 1
		}

		if av.target != nil && bv.target != nil {
			atname := chompAnyUniqSuffix(av.target.name)
			btname := chompAnyUniqSuffix(bv.target.name)
			if atname < btname {
				return -1
			}
			if atname > btname {
				return 1
			}
		}
		// same origin, same target.
		aw := av.whence()
		bw := bv.whence()
		if aw < bw {
			return -1
		}
		if aw > bw {
			return 1
		}

		// logical clocks change, maybe, avoid.
		// av.senderLC < av.senderLC

		acli := av.cliOrSrvString()
		bcli := bv.cliOrSrvString()
		if acli < bcli {
			return -1
		}
		if acli > bcli {
			return 1
		}
		// either both client or both server

		// for timers, this is completeTm if available.
		atm := av.tm()
		btm := bv.tm()
		if atm.Before(btm) {
			return -1
		}
		if atm.After(btm) {
			return 1
		}
		if av.kind == TIMER && bv.kind == TIMER {
			if av.timerDur < bv.timerDur {
				return -1
			}
			if av.timerDur > bv.timerDur {
				return 1
			}
			if av.initTm.Before(bv.initTm) {
				return -1
			}
			if av.initTm.After(bv.initTm) {
				return 1
			}
		}

		a0 := av.dispatchTm.IsZero()
		b0 := bv.dispatchTm.IsZero()
		if a0 && !b0 {
			return -1
		}
		if !a0 && b0 {
			return 1
		}
		if !a0 && !b0 {
			if av.dispatchTm.Before(bv.dispatchTm) {
				return -1
			}
			if av.dispatchTm.After(bv.dispatchTm) {
				return 1
			}
		}

		// sn are non-deterministic. only use as an
		// extreme last resort. tube has some srv.go:2272
		// calls that do look virtually identical...

		if asn < bsn {
			return -1
		}
		if asn > bsn {
			return 1
		}
		return 0
	}
	return &pq{
		Owner:   owner,
		Orderby: "releasableQ: dispatch,batch,priority,tm,clisrv,kind,...",
		Tree:    rb.NewTree(cmp),
		cmp:     cmp,
	}
}

func (s *Simnet) showQ(q *pq, name string) (r string) {
	sz := q.Len()
	if sz == 0 {
		r += fmt.Sprintf("\n empty Q: %v\n", name)
		return
	}
	r += fmt.Sprintf("\n %v size %v:\n%v", name, sz, q)

	i := 0
	r = fmt.Sprintf(" ------- %v len %v --------\n", name, q.Len())
	for it := q.Tree.Min(); !it.Limit(); it = it.Next() {

		item := it.Item() // interface{}
		if isNil(item) {
			panic("do not put nil into the pq")
		}
		op := item.(*mop)
		r += fmt.Sprintf("pq[%2d] = %v   %v\n", i, nice9(op.tm()), op)
		i++
	}
	return
}
