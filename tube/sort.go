package tube

import (
	"cmp"
	"fmt"
	"iter"
	"slices"
)

// order in which tickets appear in the RaftLog.
type logOrder []*Ticket

func (lo logOrder) Len() int { return len(lo) }
func (lo logOrder) Less(i, j int) bool {
	return lo[i].LogIndex < lo[j].LogIndex
}
func (lo logOrder) Swap(i, j int) {
	lo[i], lo[j] = lo[j], lo[i]
}

// client serial number on ticket.
type ticketsInSerialOrder []*Ticket

func (lo ticketsInSerialOrder) Len() int { return len(lo) }
func (lo ticketsInSerialOrder) Less(i, j int) bool {
	return lo[i].TSN < lo[j].TSN
}
func (lo ticketsInSerialOrder) Swap(i, j int) {
	lo[i], lo[j] = lo[j], lo[i]
}

// client timestamp on ticket.
type chronoOrder []*Ticket

func (lo chronoOrder) Len() int { return len(lo) }
func (lo chronoOrder) Less(i, j int) bool {
	return lo[i].T0.Before(lo[j].T0)
}
func (lo chronoOrder) Swap(i, j int) {
	lo[i], lo[j] = lo[j], lo[i]
}

// leader stamps tickets when they first receive them.
type stampOrder []*Ticket

func (lo stampOrder) Len() int { return len(lo) }
func (lo stampOrder) Less(i, j int) bool {
	return lo[i].LeaderStampSN < lo[j].LeaderStampSN
}
func (lo stampOrder) Swap(i, j int) {
	lo[i], lo[j] = lo[j], lo[i]
}

// sort any map by its keys
func sorted[K cmp.Ordered, V any](m map[K]V) iter.Seq2[K, V] {

	return func(yield func(K, V) bool) {

		var keys []K
		for k := range m {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, k := range keys {
			v := m[k]
			if !yield(k, v) {
				return
			}
		}
	} // end seq2 definition
}

type byCircuitID []*cktPlus

func (p byCircuitID) Len() int { return len(p) }
func (p byCircuitID) Less(i, j int) bool {
	return p[i].ckt.CircuitID < p[j].ckt.CircuitID
}
func (p byCircuitID) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

type indexTermSlice []*IndexTerm

func (p indexTermSlice) Len() int { return len(p) }
func (p indexTermSlice) Less(i, j int) bool {
	// sort biggest first!
	if p[i].Term > p[j].Term {
		return true
	}
	if p[i].Term < p[j].Term {
		return false
	}
	return p[i].Index >= p[j].Index
}
func (p indexTermSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p indexTermSlice) String() (r string) {
	r = fmt.Sprintf("indexTermSlice[%v]{\n", len(p))
	for i, itn := range p {
		r += fmt.Sprintf("[%02d] %v\n", i, itn)
	}
	r += "}\n"
	return
}
