package tube

import "testing"

// TestRleTruncatePreservesCompactTermAtBase is a
// regression test for the bug
// where Truncate(keepIndex, ...) with keepIndex == s.BaseC would
// unconditionally zero out CompactTerm even
// though BaseC itself was not changing.
// That left the log in a state
// where CompactTerm=0 while BaseC
// was a large non-zero value, causing Raft
// to lose track of the term at
// the compaction boundary.
//
// Observed failure (from the bug report):
//
// s.wal.logIndex=[BaseC: 364 [CompactTerm: 0]|logical len 7; (364:371]
// TermsRLE{ Base: 364, Endi: 371, Runs:
//
//	   &tube.TermRLE{Term:6, Count:6}
//	   &tube.TermRLE{Term:11, Count:1}
//	}
//
// Steps that reproduce it:
//
//  1. Build a previously-compacted log (BaseC=364, CompactTerm=6) with
//     some runs on top.
//  2. Call Truncate(364, nil) — roll back to the compaction boundary.
//  3. Re-add 7 entries (terms 6 × 6 then 11 × 1).
//  4. Before the fix: CompactTerm is 0.  After the fix: CompactTerm is 6.
func TestRleTruncatePreservesCompactTermAtBase(t *testing.T) {

	// ------------------------------------------------------------------ //
	// 1. Build a log that already has a compaction at BaseC=364, term=6.  //
	//    Entries 365..370 are term 6, entry 371 is term 11.               //
	// ------------------------------------------------------------------ //
	rle := &TermsRLE{
		BaseC:       364,
		CompactTerm: 6, // the term recorded at the snapshot boundary
		Endi:        371,
		Runs: []*TermRLE{
			{Term: 6, Count: 6},  // covers indices 365-370
			{Term: 11, Count: 1}, // covers index 371
		},
	}

	if rle.CompactTerm != 6 {
		t.Fatalf("pre-condition: wanted CompactTerm=6, got %v", rle.CompactTerm)
	}

	// ------------------------------------------------------------------ //
	// 2. Truncate back to exactly the compaction boundary.               //
	//    keepIndex == BaseC is the key edge-case.                        //
	// ------------------------------------------------------------------ //
	rle.Truncate(364, nil)

	// After truncation to the compaction boundary:
	//   BaseC  must stay 364
	//   Endi   must equal 364   (no runs remain)
	//   Runs   must be empty
	//   CompactTerm must STILL BE 6  ← this was the bug (was 0)
	if rle.BaseC != 364 {
		t.Errorf("after Truncate: BaseC = %v, want 364", rle.BaseC)
	}
	if rle.Endi != 364 {
		t.Errorf("after Truncate: Endi = %v, want 364", rle.Endi)
	}
	if len(rle.Runs) != 0 {
		t.Errorf("after Truncate: len(Runs) = %v, want 0", len(rle.Runs))
	}
	if rle.CompactTerm != 6 {
		t.Errorf("after Truncate: CompactTerm = %v, want 6 — "+
			"Truncate(keepIndex==BaseC) must NOT zero out CompactTerm", rle.CompactTerm)
	}

	// ------------------------------------------------------------------ //
	// 3. Re-add entries (simulating new leader entries after the reset).  //
	//    This is what converts the silent bug into the observed dump.     //
	// ------------------------------------------------------------------ //
	for range 6 {
		rle.AddTerm(6)
	}
	rle.AddTerm(11)

	// The log should now look exactly like the pre-truncation log above,
	// INCLUDING a correct CompactTerm.
	if rle.CompactTerm != 6 {
		t.Errorf("after re-adding entries: CompactTerm = %v, want 6 — "+
			"this reproduces the exact bug-report state (CompactTerm: 0)", rle.CompactTerm)
	}
	if rle.BaseC != 364 {
		t.Errorf("after re-adding entries: BaseC = %v, want 364", rle.BaseC)
	}
	if rle.Endi != 371 {
		t.Errorf("after re-adding entries: Endi = %v, want 371", rle.Endi)
	}
	if len(rle.Runs) != 2 {
		t.Fatalf("after re-adding entries: len(Runs) = %v, want 2", len(rle.Runs))
	}
	if rle.Runs[0].Term != 6 || rle.Runs[0].Count != 6 {
		t.Errorf("Runs[0] = {Term:%v, Count:%v}, want {6, 6}",
			rle.Runs[0].Term, rle.Runs[0].Count)
	}
	if rle.Runs[1].Term != 11 || rle.Runs[1].Count != 1 {
		t.Errorf("Runs[1] = {Term:%v, Count:%v}, want {11, 1}",
			rle.Runs[1].Term, rle.Runs[1].Count)
	}
}

// TestRleTruncateBeforeBaseZerosCompactTerm confirms that going strictly
// *before* BaseC (a full wipe below the compaction point) is the one
// case where resetting CompactTerm to 0 is correct.
func TestRleTruncateBeforeBaseZerosCompactTerm(t *testing.T) {
	rle := &TermsRLE{
		BaseC:       364,
		CompactTerm: 6,
		Endi:        371,
		Runs: []*TermRLE{
			{Term: 6, Count: 6},
			{Term: 11, Count: 1},
		},
	}

	// keepIndex < BaseC — wipe everything including the snapshot.
	rle.Truncate(100, nil)

	if rle.BaseC != 100 {
		t.Errorf("BaseC = %v, want 100", rle.BaseC)
	}
	if rle.CompactTerm != 0 {
		t.Errorf("CompactTerm = %v, want 0 when truncating below BaseC", rle.CompactTerm)
	}
	if len(rle.Runs) != 0 {
		t.Errorf("len(Runs) = %v, want 0", len(rle.Runs))
	}
}

// TestRleTruncateWithSyncmePreservesCompactTermAtBase
// checks that the syncme out-parameter also receives
// the correct (non-zero) CompactTerm when
// Truncate is called with keepIndex == BaseC.
func TestRleTruncateWithSyncmePreservesCompactTermAtBase(t *testing.T) {
	rle := &TermsRLE{
		BaseC:       364,
		CompactTerm: 6,
		Endi:        371,
		Runs: []*TermRLE{
			{Term: 6, Count: 6},
			{Term: 11, Count: 1},
		},
	}

	var sync IndexTerm
	rle.Truncate(364, &sync)

	if rle.CompactTerm != 6 {
		t.Errorf("rle.CompactTerm = %v, want 6", rle.CompactTerm)
	}
	// syncme should reflect the post-truncation state.
	if sync.Term != 6 {
		t.Errorf("syncme.Term = %v, want 6 — syncme must not carry the zeroed value", sync.Term)
	}
	if sync.Index != 364 {
		t.Errorf("syncme.Index = %v, want 364", sync.Index)
	}
}
