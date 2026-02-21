package tube

import (
	"fmt"
	"testing"
)

func TestRleCompactNewBeg1(t *testing.T) {
	// starting:
	// 1,2,3,4 index
	// 1,1,2,2 termsx

	// ending, after CompactNewBeg(1) (stays the same)
	// 1,2,3,4 index
	// 1,1,2,2 terms

	f := &TermsRLE{
		BaseC: 0,
		Endi:  4,
		Runs: []*TermRLE{
			{Term: 1, Count: 2},
			{Term: 2, Count: 2},
		},
	}
	var ti IndexTerm
	f.CompactNewBeg(1, &ti)
	if f.BaseC != 0 {
		panic(fmt.Sprintf("wanted f.BaseC == 0, got %v", f.BaseC))
	}
	if len(f.Runs) != 2 {
		panic(fmt.Sprintf("wanted len(f.Runs) == 2, got %v", len(f.Runs)))
	}
	for i, run := range f.Runs {
		if run.Count != 2 {
			panic(fmt.Sprintf("wanted f.Runs[i=%v].Count == 2, got %v", i, run.Count))
		}
	}
}

func TestRleCompactNewBeg2(t *testing.T) {
	// starting:
	// 1,2,3,4 index
	// 1,1,2,2 termsx

	// ending, after CompactNewBeg(2)
	// 2,3,4 index
	// 1,2,2 terms

	f := &TermsRLE{
		BaseC: 0, // want -> 1
		Endi:  4,
		Runs: []*TermRLE{
			{Term: 1, Count: 2}, // want -> {Term: 1, Count:1}
			{Term: 2, Count: 2},
		},
	}
	var ti IndexTerm
	f.CompactNewBeg(2, &ti)
	if f.BaseC != 1 {
		panic(fmt.Sprintf("wanted f.BaseC == 1, got %v", f.BaseC))
	}
	if f.BaseC != ti.Index {
		panicf("ti.Index(%v) should match f.BaseC(%v)", ti.Index, f.BaseC)
	}
	if f.Runs[0].Count != 1 {
		panic(fmt.Sprintf("wanted f.Runs[0].Count == 1, got %v", f.Runs[0].Count))
	}
}

func TestRleCompactNewBeg3(t *testing.T) {
	// starting: base 1
	// 2,3,4 index
	// 1,2,2 terms

	// ending, after CompactNewBeg(3)
	// base 2
	// 3,4 index
	// 2,2 terms

	f := &TermsRLE{
		BaseC: 1, // want -> 2
		Endi:  4,
		Runs: []*TermRLE{
			{Term: 1, Count: 1}, // want -> disappear
			{Term: 2, Count: 2},
		},
	}
	var ti IndexTerm
	f.CompactNewBeg(3, &ti)
	if f.BaseC != 2 {
		panic(fmt.Sprintf("wanted f.BaseC == 2, got %v", f.BaseC))
	}
	if f.BaseC != ti.Index {
		panicf("ti.Index(%v) should match f.BaseC(%v)", ti.Index, f.BaseC)
	}
	if len(f.Runs) != 1 {
		panic(fmt.Sprintf("wanted len(f.Runs) == 1, got %v", len(f.Runs)))
	}
	if f.Runs[0].Count != 2 || f.Runs[0].Term != 2 {
		panic(fmt.Sprintf("wanted f.Runs[0].Count == 2, got %v; in term %v", f.Runs[0].Count, f.Runs[0].Term))
	}
}

func TestRleCompactNewBeg4(t *testing.T) {
	// starting: base 1
	// 2,3,4 index
	// 1,2,2 terms

	// ending, after CompactNewBeg(4)
	// base 3
	// 4 index
	// 2 term

	f := &TermsRLE{
		BaseC: 1, // want -> 3
		Endi:  4,
		Runs: []*TermRLE{
			{Term: 1, Count: 1}, // want -> disappear
			{Term: 2, Count: 2}, // want -> {Term:2, Count:1}
		},
	}
	var ti IndexTerm
	f.CompactNewBeg(4, &ti)
	if f.BaseC != 3 {
		panic(fmt.Sprintf("wanted f.BaseC == 2, got %v", f.BaseC))
	}
	if f.BaseC != ti.Index {
		panicf("ti.Index(%v) should match f.BaseC(%v)", ti.Index, f.BaseC)
	}
	if len(f.Runs) != 1 {
		panic(fmt.Sprintf("wanted len(f.Runs) == 1, got %v", len(f.Runs)))
	}
	if f.Runs[0].Count != 1 || f.Runs[0].Term != 2 {
		panic(fmt.Sprintf("wanted f.Runs[0].Count == 1 (in term 2), got count %v; in term %v", f.Runs[0].Count, f.Runs[0].Term))
	}
}

func TestRleTruncate3(t *testing.T) {
	// starting: base 1
	// 2,3,4 index
	// 1,2,2 terms

	// ending, after Truncate(3)
	// base 1
	// 2,3 index
	// 1,2 term

	f := &TermsRLE{
		BaseC: 1, // want stay same
		Endi:  4, // want -> 3
		Runs: []*TermRLE{
			{Term: 1, Count: 1}, // want -> kept
			{Term: 2, Count: 2}, // want -> split {Term:2, Count:1} kept.
		},
	}
	var ti IndexTerm
	f.Truncate(3, &ti)
	if f.Endi != 3 {
		panic(fmt.Sprintf("wanted f.Endi == 3, got %v", f.Endi))
	}
	if f.BaseC != 1 {
		panic(fmt.Sprintf("wanted f.BaseC == 1, got %v", f.BaseC))
	}
	if f.BaseC != ti.Index {
		panicf("ti.Index(%v) should match f.BaseC(%v)", ti.Index, f.BaseC)
	}
	if len(f.Runs) != 2 {
		panic(fmt.Sprintf("wanted len(f.Runs) == 2, got %v", len(f.Runs)))
	}
	if f.Runs[0].Count != 1 || f.Runs[0].Term != 1 {
		panic(fmt.Sprintf("wanted f.Runs[0].Count == 1 (in term 1), got count %v; in term %v", f.Runs[0].Count, f.Runs[0].Term))
	}
	if f.Runs[1].Count != 1 || f.Runs[1].Term != 2 {
		panic(fmt.Sprintf("wanted f.Runs[0].Count == 1 (in term 2), got count %v; in term %v", f.Runs[1].Count, f.Runs[1].Term))
	}
}

func TestRleTruncate2(t *testing.T) {
	// starting: base 1
	// 2,3,4 index
	// 1,2,2 terms

	// ending, after Truncate(2)
	// base 1
	// 2 index
	// 1 term

	f := &TermsRLE{
		BaseC: 1, // want stay same
		Endi:  4, // want -> 2
		Runs: []*TermRLE{
			{Term: 1, Count: 1}, // want -> kept
			{Term: 2, Count: 2}, // want -> gone.
		},
	}
	var ti IndexTerm
	f.Truncate(2, &ti)
	if f.Endi != 2 {
		panic(fmt.Sprintf("wanted f.Endi == 2, got %v", f.Endi))
	}
	if f.BaseC != 1 {
		panic(fmt.Sprintf("wanted f.BaseC == 1, got %v", f.BaseC))
	}
	if f.BaseC != ti.Index {
		panicf("ti.Index(%v) should match f.BaseC(%v)", ti.Index, f.BaseC)
	}
	if len(f.Runs) != 1 {
		panic(fmt.Sprintf("wanted len(f.Runs) == 1, got %v", len(f.Runs)))
	}
	if f.Runs[0].Count != 1 || f.Runs[0].Term != 1 {
		panic(fmt.Sprintf("wanted f.Runs[0].Count == 1 (in term 1), got count %v; in term %v", f.Runs[0].Count, f.Runs[0].Term))
	}
}

func TestRleLogExtendsLog(t *testing.T) {

	followerLog := &TermsRLE{
		Runs: []*TermRLE{
			{Term: 1, Count: 3},
			{Term: 2, Count: 2},
		},
	}
	leaderLog := &TermsRLE{
		Runs: []*TermRLE{
			{Term: 1, Count: 3},
			{Term: 2, Count: 2},
		},
	}
	followerLog.fixTot()
	leaderLog.fixTot()
	extends, longestCommonPrefix, _ := leaderLog.Extends(followerLog)

	if got, want := extends, true; got != want {
		t.Fatalf("identical logs, expected extends true, got %v", got)
	}
	if got, want := longestCommonPrefix, 5; got != int64(want) {
		t.Fatalf("expected longestCommonPrefix to be 5, got %v", got)
	}

	// check the base case of empty follower log
	followerLog.Runs = []*TermRLE{}

	extends, longestCommonPrefix, _ = leaderLog.Extends(followerLog)

	if got, want := extends, true; got != want {
		t.Fatalf("empty follower logs, expected extends true, got %v", got)
	}
	if got, want := longestCommonPrefix, 0; got != int64(want) {
		t.Fatalf("expected longestCommonPrefix to be 0, got %v", got)
	}

	// check nothing in common
	followerLog.Runs = []*TermRLE{{Term: 2, Count: 1}}
	followerLog.Endi = 1

	extends, longestCommonPrefix, _ = leaderLog.Extends(followerLog)

	if got, want := extends, false; got != want {
		t.Fatalf("nothing in common, expected extends false, got %v", got)
	}
	if got, want := longestCommonPrefix, 0; got != int64(want) {
		t.Fatalf("expected longestCommonPrefix to be 0, got %v", got)
	}

}

func TestRleLogExtendsWithAdvancedBase(t *testing.T) {

	// BaseC is CompactIndex. same now; so any early
	// missing must be due to compaction/snapshot.
	// want Extends true here:
	// follower  1 2
	// leader      2 3
	// terms     1 3 3

	followerLog := &TermsRLE{
		BaseC: 0,
		Runs: []*TermRLE{
			{Term: 1, Count: 1},
			{Term: 3, Count: 1},
		},
	}
	leaderLog := &TermsRLE{
		BaseC: 1,
		Runs: []*TermRLE{
			{Term: 3, Count: 2},
		},
	}
	followerLog.fixTot()
	leaderLog.fixTot()

	vv("followerLog = %v", followerLog)
	vv("leaderLog = %v", leaderLog)

	extends, longestCommonPrefix, _ := leaderLog.Extends(followerLog)

	if got, want := extends, true; got != want {
		t.Fatalf("leader base advanced, expected extends true, got %v", got)
	}
	if got, want := longestCommonPrefix, 2; got != int64(want) {
		t.Fatalf("expected longestCommonPrefix to be 2, got %v", got)
	}
}

func TestRleExtendsPredicateOnLogs(t *testing.T) {

	for base := int64(0); base < 7; base++ {
		tests := []struct {
			name        string
			lead        *TermsRLE
			foll        *TermsRLE
			wantLcp     int64
			wantExtends bool
			wantEqual   bool
		}{
			{
				name: "lead [1,2,3], follow [1]",
				lead: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 3},
					},
				},
				foll: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 1},
					},
				},
				wantLcp:     1,
				wantExtends: true,
				wantEqual:   false,
			},
			{
				name: "identical logs",
				lead: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 3},
						{Term: 2, Count: 2},
					},
				},
				foll: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 3},
						{Term: 2, Count: 2},
					},
				},
				wantLcp:     5,
				wantExtends: true,
				wantEqual:   true,
			},
			{
				name: "divergence in first run - different term",
				lead: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 3},
						{Term: 2, Count: 2},
					},
				},
				foll: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 2, Count: 3},
						{Term: 2, Count: 2},
					},
				},
				wantLcp:     0,
				wantExtends: false,
			},
			{
				name: "divergence in first run - different count",
				lead: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 3},
						{Term: 2, Count: 2},
					},
				},
				foll: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 2},
						{Term: 2, Count: 2},
					},
				},
				wantLcp:     2,
				wantExtends: false,
			},
			{
				name: "divergence in later run",
				lead: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 3},
						{Term: 2, Count: 2},
						{Term: 3, Count: 4},
					},
				},
				foll: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 3},
						{Term: 2, Count: 2},
						{Term: 4, Count: 4},
					},
				},
				wantLcp:     5,
				wantExtends: false,
			},
			{
				name: "lead shorter than foll",
				lead: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 3},
						{Term: 2, Count: 2},
					},
				},
				foll: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 3},
						{Term: 2, Count: 2},
						{Term: 3, Count: 4},
					},
				},
				wantLcp:     5,
				wantExtends: false,
			},
			{
				name: "foll shorter than lead",
				lead: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 3},
						{Term: 2, Count: 2},
						{Term: 3, Count: 4},
					},
				},
				foll: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 3},
						{Term: 2, Count: 2},
					},
				},
				wantLcp:     5,
				wantExtends: true,
			},
			{
				name: "empty lead",
				lead: &TermsRLE{
					Runs: []*TermRLE{},
				},
				foll: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 3},
					},
				},
				wantLcp:     0,
				wantExtends: false,
			},
			{
				name: "empty foll",
				lead: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 3},
					},
				},
				foll: &TermsRLE{
					Runs: []*TermRLE{},
				},
				wantLcp:     0,
				wantExtends: true,
			},
			{
				name: "both empty",
				lead: &TermsRLE{
					Runs: []*TermRLE{},
				},
				foll: &TermsRLE{
					Runs: []*TermRLE{},
				},
				wantLcp:     0,
				wantExtends: true,
				wantEqual:   true,
			},
			{
				name: "single run different term",
				lead: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 5},
					},
				},
				foll: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 2, Count: 5},
					},
				},
				wantLcp:     0,
				wantExtends: false,
			},
			{
				name: "single run different count",
				lead: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 5},
					},
				},
				foll: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 3},
					},
				},
				wantLcp:     3,
				wantExtends: true,
				wantEqual:   false,
			},
			{
				name: "complex divergence pattern",
				lead: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 3},
						{Term: 2, Count: 2},
						{Term: 1, Count: 4},
						{Term: 3, Count: 2},
					},
				},
				foll: &TermsRLE{
					Runs: []*TermRLE{
						{Term: 1, Count: 3},
						{Term: 2, Count: 2},
						{Term: 1, Count: 3},
						{Term: 3, Count: 2},
					},
				},
				wantLcp:     8,
				wantExtends: false,
			},
		}

		for i, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tt.lead.BaseC = base
				tt.foll.BaseC = base
				tt.lead.fixTot()
				tt.foll.fixTot()
				extends, lcp, _ := tt.lead.Extends(tt.foll)
				wantLcp := tt.wantLcp
				if lcp > 0 {
					wantLcp += base
				}
				if extends != tt.wantExtends || lcp != wantLcp {
					t.Errorf("i=%v: Extends() = (%v, %v), want (%v, %v) \n lead=%v \n foll=%v \n",
						i, extends, lcp, tt.wantExtends, wantLcp, tt.lead, tt.foll)
				}
				equal := tt.lead.Equal(tt.foll)
				if equal != tt.wantEqual {
					t.Errorf("lead.Equal(foll) = %v, want %v; tt.lead='%v'; tt.fol='%v'",
						equal, tt.wantEqual, tt.lead, tt.foll)
				}
				// symmetry of Equal
				equal2 := tt.foll.Equal(tt.lead)
				if equal2 != tt.wantEqual {
					t.Errorf("foll.Equal(lead) = %v, want %v",
						equal2, tt.wantEqual)
				}
			})
		}
	} // end base loop
}

// TestExtendsNilInputs verifies behavior with nil inputs
func TestRleExtendsNilInputs(t *testing.T) {
	log := &TermsRLE{
		Runs: []*TermRLE{
			{Term: 1, Count: 3},
		},
	}

	// Test nil other
	ext, lcp, _ := log.Extends(nil)
	if lcp != 0 || !ext {
		t.Errorf("Extends(nil) = (%v, %v), want (true, 0)",
			ext, lcp)
	}
	if log.Equal(nil) {
		t.Errorf("log with something should not be Equal to nil")
	}

	// Test nil receiver
	ext, lcp, _ = (*TermsRLE)(nil).Extends(log)
	if lcp != 0 || ext {
		t.Errorf("nil.Extends() = (%v, %v), want (false, 0)",
			ext, lcp)
	}
	if (*TermsRLE)(nil).Equal(log) {
		t.Errorf("nil should not be Equal to a log with something in it")
	}

	// Test both nil
	ext, lcp, _ = (*TermsRLE)(nil).Extends(nil)
	if lcp != 0 || !ext {
		t.Errorf("nil.Extends(nil) = (%v, %v), want (true, 0)",
			ext, lcp)
	}
	if !(*TermsRLE)(nil).Equal(nil) {
		t.Errorf("nil should equal nil")
	}

	//vv("begin Equal test.")
	// Test both empty
	a, b := newTermsRLE(), newTermsRLE()
	if !a.Equal(b) {
		t.Errorf("!a.Equal(b), but they are both empty newTermsRLE(). a='%#v', b='%#v'", a, b)
	}
	//vv("done Equal test.")
}

func TestRleGetTermForIndex(t *testing.T) {
	leaderLog := &TermsRLE{
		BaseC: 3,
		Runs: []*TermRLE{
			{Term: 1, Count: 1}, // 4
			{Term: 2, Count: 1}, // 5
			{Term: 3, Count: 1}, // 6
			{Term: 4, Count: 1}, // 7
			{Term: 5, Count: 2}, // 8,9
			{Term: 6, Count: 3}, // 10,11,12
			{Term: 7, Count: 1}, // 13
			{Term: 8, Count: 1}, // 14
		},
	}
	leaderLog.fixTot()
	for i := leaderLog.BaseC + 1; i < leaderLog.Endi; i++ {
		gotTerm := leaderLog.getTermForIndex(i)
		wantTerm := i - 3
		switch i {
		case 9:
			wantTerm = 5
		case 10, 11, 12:
			wantTerm = 6
		case 13:
			wantTerm = 7
		case 14:
			wantTerm = 8
		}
		if gotTerm != wantTerm {
			panic(fmt.Sprintf("at i=%v: got %v, want %v term", i, gotTerm, wantTerm)) // panic: at i=4: got 4, want 1 term
		}
	}
}

func TestRle2LogExtendsAfterCompaction(t *testing.T) {

	// want Extends true even after compaction, or
	// snapshot installation, with CompactIndex:1
	// follower  (empty Runs, but has CompactIndex:1), so effectively:
	// follower  1
	// leader      2 3
	// terms     1 3 3

	followerLog := &TermsRLE{
		CompactTerm: 1,
		BaseC:       1,
		Endi:        1,
		// (no Runs)
		//Runs: []*TermRLE{
		//	{Term: 1, Count: 1},
		//	{Term: 3, Count: 1},
		//},
	}
	leaderLog := &TermsRLE{
		BaseC: 1,
		Endi:  3,
		Runs: []*TermRLE{
			{Term: 3, Count: 2},
		},
		CompactTerm: 1,
	}
	followerLog.fixTot()
	leaderLog.fixTot()
	extends, longestCommonPrefix, _ := leaderLog.Extends(followerLog)

	if got, want := extends, true; got != want {
		t.Fatalf("follower installed snapshot and leader compacted but still expected extends true, got %v", got)
	}
	if got, want := longestCommonPrefix, 1; got != int64(want) {
		t.Fatalf("expected longestCommonPrefix to be 1, got %v", got)
	}
}

func TestRle3LogExtendsAfterCompaction(t *testing.T) {

	// want Extends true even after compaction, or
	// snapshot installation, with CompactIndex:2
	// follower  (empty Runs, but has CompactIndex:2), so effectively:
	// follower  1 2
	// leader      2 3
	// terms     1 3 3

	followerLog := &TermsRLE{
		//CompactIndex: 2,
		CompactTerm: 1,
		BaseC:       2,
		Endi:        2,
		// (no Runs)
		//Runs: []*TermRLE{
		//	{Term: 1, Count: 1},
		//	{Term: 3, Count: 1},
		//},
	}
	leaderLog := &TermsRLE{
		BaseC: 1,
		Endi:  3,
		Runs: []*TermRLE{
			{Term: 3, Count: 2},
		},
	}
	followerLog.fixTot()
	leaderLog.fixTot()
	extends, longestCommonPrefix, _ := leaderLog.Extends(followerLog)

	if got, want := extends, true; got != want {
		t.Fatalf("follower installed snapshot and leader compacted but still expected extends true, got %v", got)
	}
	if got, want := longestCommonPrefix, 2; got != int64(want) {
		t.Fatalf("expected longestCommonPrefix to be 2, got %v", got)
	}
}
