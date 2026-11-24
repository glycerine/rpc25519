package tube

import "testing"

func TestMemWALCompactionThreshold(t *testing.T) {
	entries := createLogEntries(1, 25, 1)
	size := func(e *RaftLogEntry) int64 {
		t.Helper()
		b, err := e.MarshalMsg(nil)
		if err != nil {
			t.Fatal(err)
		}
		return int64(len(b))
	}
	var threshold int64
	for i, e := range entries {
		e.Ticket.Val = make([]byte, 128+(i%3)*48)
		if i < 6 {
			threshold += size(e)
		}
	}
	cfg := &TubeConfig{NoDisk: true, CompactionThresholdBytes: threshold}
	w, err := cfg.newRaftWriteAheadLog("", false)
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()

	var base, retained int64
	var cuts, suffixLengths []int64
	for _, e := range entries {
		i := e.Index
		// Let apply lag behind commit by different amounts across cuts.
		applied := max(int64(0), i-1-(i/5)%3)
		if err := w.overwriteEntries(i-1, []*RaftLogEntry{e}, false, i-1, applied, nil, nil); err != nil {
			t.Fatal(err)
		}
		retained += size(e)
		if retained > threshold {
			// The follower keeps the last applied entry as its snapshot anchor.
			base = applied - 1
			suffixLengths = append(suffixLengths, i-base)
			retained = 0
			for _, kept := range entries[base:i] {
				retained += size(kept)
			}
			cuts = append(cuts, i)
		}
		if w.logIndex.BaseC != base || w.logSizeOnDisk() != retained {
			t.Fatalf("write %d: compacted through %d, retained %d bytes; want %d and %d bytes",
				i, w.logIndex.BaseC, w.logSizeOnDisk(), base, retained)
		}
	}
	if len(cuts) < 2 || cuts[0] != 7 {
		t.Fatalf("expected repeated compactions starting at write 7, got %v", cuts)
	}
	t.Logf("threshold %d bytes, compactions at writes %v, retained suffix lengths %v", threshold, cuts, suffixLengths)

	// A leader change can replace an uncommitted suffix after compaction.
	last := entries[len(entries)-1]
	replacement := &RaftLogEntry{Index: last.Index, Term: 2, Ticket: &Ticket{Val: []byte("short")}}
	if err := w.overwriteEntries(last.Index-1, []*RaftLogEntry{replacement}, false, last.Index-1, last.Index-1, nil, nil); err != nil {
		t.Fatal(err)
	}
	if want := retained - size(last) + size(replacement); w.logSizeOnDisk() != want {
		t.Fatalf("retained %d bytes after overwrite, want %d", w.logSizeOnDisk(), want)
	}
	for i := range w.raftLog {
		if _, err := w.loadRaftLogEntry(i, false); err != nil {
			t.Fatal(err)
		}
	}
	w.installedSnapshot(&RaftState{CompactionDiscardedLast: IndexTerm{Index: last.Index, Term: 2}})
	if n := w.logSizeOnDisk(); n != 0 {
		t.Fatalf("retained %d bytes after snapshot install", n)
	}
}

func TestMemWALCompactionAcrossCommitBatches(t *testing.T) {
	const threshold = 4 << 10
	cfg := &TubeConfig{NoDisk: true, CompactionThresholdBytes: threshold}
	w, err := cfg.newRaftWriteAheadLog("", false)
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	entries := createLogEntries(1, 21, 1)
	for i, e := range entries {
		e.Ticket.Val = make([]byte, 384+64*(i%4))
	}
	var previous int64
	for _, step := range []struct {
		end, commit, applied, wantBase int64
	}{
		{3, 0, 0, 0},
		{7, 0, 0, 0},
		{12, 0, 0, 0}, // Over the byte threshold with all twelve writes uncommitted.
		{16, 8, 5, 4}, // Keep the applied anchor and the three committed, unapplied entries.
		{20, 16, 14, 13},
	} {
		if err := w.overwriteEntries(previous, entries[previous:step.end], false, step.commit, step.applied, nil, nil); err != nil {
			t.Fatal(err)
		}
		if w.logIndex.BaseC != step.wantBase || int64(len(w.raftLog)) != step.end-step.wantBase {
			t.Fatalf("after %d writes, commit %d, applied %d: compacted through %d, retained %d entries",
				step.end, step.commit, step.applied, w.logIndex.BaseC, len(w.raftLog))
		}
		var bytes int64
		for i, want := range entries[step.wantBase:step.end] {
			got, err := w.loadRaftLogEntry(i, false)
			if err != nil {
				t.Fatal(err)
			}
			if got.Index != want.Index || got.Term != want.Term || got.Ticket.Key != want.Ticket.Key {
				t.Fatalf("retained entry %d has index %d, term %d, key %v", want.Index, got.Index, got.Term, got.Ticket.Key)
			}
			record, err := want.MarshalMsg(nil)
			if err != nil {
				t.Fatal(err)
			}
			bytes += int64(len(record))
		}
		if w.logSizeOnDisk() != bytes || step.end == 12 && bytes <= threshold {
			t.Fatalf("after %d writes: WAL reports %d bytes, encoded suffix is %d, threshold is %d",
				step.end, w.logSizeOnDisk(), bytes, threshold)
		}
		previous = step.end
	}
}
