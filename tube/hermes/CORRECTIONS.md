# Hermes-Protocol
TLA spec - Hermes: fault-tolerant replication protocol with strong consistency and high performance

---
Warning 
protocol-actions png contains some optimizations over the Hermes protocol presented 
in the paper such as issuing writes while being in Invalid state.

# Comment from jea, 2026 Sept 15

After finding Ivy and Lean correctness proves, we concluded some corrections were needed:

The README warning is real: protocol-actions.png includes optimizations 
beyond the paper, especially writes while Invalid.

But we also found that the PNG is too compressed to be used literally.

  Main corrections/clarifications:

  - protocol-actions.png: INV TS == local.TS should be ACK-only. 
    Some cells look like “apply invalidation,” but equal timestamps
    are duplicates/replays and must not overwrite value or LastWriterID.

  - protocol-actions.png: sWrite + INV TS == local.TS is not impossible. 
    The PNG marks it as X, but duplicate/reordered traffic
    can produce it. Safe behavior is ACK-only or drop self-echo.

  - protocol-actions.png: stale write INV and stale RMW INV differ.
    Stale non-RMW writes are ACKed; stale RMWs are not ACKed and
    should be answered with the receiver’s newer local INV.

  - protocol-actions.png: A_RR in sInvalidWR must mean retry/resume
    the read when safe, not return a value immediately.
	
  - HermesRMWs.tla: we did not find a clear bug in its core 
    safety rules. It correctly guided the RMW timestamp spacing: RMW +1, write +2.

  - HermesRMWs.tla: it is not complete as an implementation spec.
    It abstracts away client operation buffers, overwritten local
    operation completion, lease/membership timing, O3, and fair 
	completion draining.

  - Final proof addition: for RMW O3 we needed explicit parent/conflict
    metadata: parent_ts and rmw_conflict. That is more precise
    than both the PNG and the original TLA materials for the final protocol.

So: no smoking-gun “HermesRMWs.tla is wrong,” but yes, several PNG
corrections and several implementation obligations that the
original TLA does not spell out.
