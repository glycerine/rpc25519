package tube

import "fmt"

// derived from Gstring but avoid spamming with the hughe Session table.
func (z *RaftState) String() (r string) {
	r = "&RaftState{\n"
	r += fmt.Sprintf("                 Serial: %v,\n", z.Serial)
	r += fmt.Sprintf("               PeerName: \"%v\",\n", z.PeerName)
	r += fmt.Sprintf("        PeerServiceName: \"%v\",\n", z.PeerServiceName)
	r += fmt.Sprintf(" PeerServiceNameVersion: \"%v\",\n", z.PeerServiceNameVersion)
	r += fmt.Sprintf("                 PeerID: \"%v\",\n", z.PeerID)
	r += fmt.Sprintf("              ClusterID: \"%v\",\n", z.ClusterID)
	r += fmt.Sprintf("            CurrentTerm: %v,\n", z.CurrentTerm)
	r += fmt.Sprintf("               VotedFor: \"%v\",\n", z.VotedFor)
	r += fmt.Sprintf("           VotedForName: \"%v\",\n", z.VotedForName)
	r += fmt.Sprintf("              HaveVoted: %v,\n", z.HaveVoted)
	r += fmt.Sprintf("          HaveVotedTerm: %v,\n", z.HaveVotedTerm)
	r += fmt.Sprintf("            CommitIndex: %v,\n", z.CommitIndex)
	r += fmt.Sprintf("   CommitIndexEntryTerm: %v,\n", z.CommitIndexEntryTerm)
	r += fmt.Sprintf("            LastApplied: %v,\n", z.LastApplied)
	r += fmt.Sprintf("        LastAppliedTerm: %v,\n", z.LastAppliedTerm)
	r += fmt.Sprintf("                KVstore: %v,\n", z.KVstore)
	r += fmt.Sprintf("                     MC: %v,\n", z.MC)
	r += fmt.Sprintf("                  Known: %v,\n", z.Known)
	r += fmt.Sprintf("              Observers: %v,\n", z.Observers)
	r += fmt.Sprintf("         ShadowReplicas: %v,\n", z.ShadowReplicas)
	r += fmt.Sprintf("      LastSaveTimestamp: %v,\n", z.LastSaveTimestamp)
	r += fmt.Sprintf("CompactionDiscardedLast: %v,\n", z.CompactionDiscardedLast)
	if z.SessTable != nil && len(z.SessTable) > 0 {
		// SessTable is a map[string]*SessionTableEntry
		sz := 0
		var first *SessionTableEntry
		for _, ste := range z.SessTable {
			sz += ste.Msgsize()
			first = ste
		}
		r += fmt.Sprintf("       SessTable: len %v (est bytes: %v),\n", len(z.SessTable), sz)

		r += fmt.Sprintf("     one random SessTable entry: %v\n  ...\n", first)

	}
	r += "}\n"
	return
}

func (z *IndexTerm) String() (r string) {
	r = "&IndexTerm{\n"
	r += fmt.Sprintf("Index: %v,\n", z.Index)
	r += fmt.Sprintf(" Term: %v,\n", z.Term)
	r += fmt.Sprintf(" Name: \"%v\",\n", z.Name)
	r += "}\n"
	return
}
