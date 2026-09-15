import Std

/-!
# Hermes RMW+O3 protocol models

The first namespace below contains the earlier Ivy-shaped invariant lemmas kept
for comparison. The `Reference` namespace at the end of the file is the
stand-alone Lean model for the RMW+O3 protocol: it defines an operational
transition relation in the vocabulary of `Hermes.tla`, `HermesRMWs.tla`, and the
paper's O3 optimization. That model includes epoch-tagged messages, ACK reset on
membership change, RMW replay ACK reset, local stale-RMW refusal, and an O3
quorum rule that treats the coordinator's contribution as implicit.

The reference liveness facts are stated over transition labels rather than Ivy
state pulses. The paper and TLA artifacts prove safety and absence of deadlock,
not unconditional eventual completion, so fairness remains an explicit trace
hypothesis rather than a protocol invariant.
-/

set_option autoImplicit false
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace HermesRmwO3

universe uNode uTs uValue uEpoch

inductive HState where
  | hs_valid
  | hs_invalid
  | hs_invalid_write
  | hs_write
  | hs_replay
deriving DecidableEq, Repr

inductive LTask where
  | ready_finish
  | o3_finish
deriving DecidableEq, Repr

structure TotalOrder (alpha : Type uTs) where
  le : alpha -> alpha -> Prop
  refl : forall x, le x x
  trans : forall {x y z}, le x y -> le y z -> le x z
  antisymm : forall {x y}, le x y -> le y x -> x = y
  total : forall x y, le x y \/ le y x

def lt {alpha : Type uTs} (ord : TotalOrder alpha) (x y : alpha) : Prop :=
  ord.le x y /\ (x = y -> False)

theorem le_of_lt {alpha : Type uTs} {ord : TotalOrder alpha} {x y : alpha} :
    lt ord x y -> ord.le x y := by
  intro h
  exact h.1

theorem not_lt_self {alpha : Type uTs} {ord : TotalOrder alpha} (x : alpha) :
    Not (lt ord x x) := by
  intro h
  exact h.2 rfl

theorem eq_of_le_le {alpha : Type uTs} {ord : TotalOrder alpha} {x y : alpha} :
    ord.le x y -> ord.le y x -> x = y := by
  intro hxy hyx
  exact ord.antisymm hxy hyx

structure State
    (Node : Type uNode) (TS : Type uTs)
    (Value : Type uValue) (Epoch : Type uEpoch) where
  readyTask : LTask
  o3Task : LTask
  state : Node -> HState
  curTs : Node -> TS
  curValue : Node -> Value
  curRmw : Node -> Prop
  lastWriter : Node -> Node
  live : Node -> Prop
  pending : Node -> Prop
  pendingTs : Node -> TS
  pendingRmw : Node -> Prop
  acked : Node -> Node -> Prop
  ready : Node -> Prop
  readyEpoch : Node -> Epoch
  seenEpoch : Epoch -> Prop
  epochDone : Epoch -> Prop
  seenTs : TS -> Prop
  parent : TS -> TS -> Prop
  parentTs : TS -> TS
  tsValue : TS -> Value -> Prop
  tsRmw : TS -> Prop
  rmwConflict : TS -> Prop
  invWrite : Node -> TS -> Value -> Prop
  invRmw : Node -> TS -> Value -> Prop
  ackMsg : Node -> Node -> TS -> Prop
  valMsg : TS -> Prop
  o3Quorum : Node -> Node -> TS -> Prop
  completeTry : Node -> Prop
  o3Try : Node -> Prop
  completed : TS -> Prop

def initState
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (initTs : TS) (initValue : Value) (initEpoch : Epoch) :
    State Node TS Value Epoch where
  readyTask := LTask.ready_finish
  o3Task := LTask.o3_finish
  state := fun _ => HState.hs_valid
  curTs := fun _ => initTs
  curValue := fun _ => initValue
  curRmw := fun _ => False
  lastWriter := fun n => n
  live := fun _ => True
  pending := fun _ => False
  pendingTs := fun _ => initTs
  pendingRmw := fun _ => False
  acked := fun _ _ => False
  ready := fun _ => False
  readyEpoch := fun _ => initEpoch
  seenEpoch := fun e => e = initEpoch
  epochDone := fun e => e = initEpoch
  seenTs := fun t => t = initTs
  parent := fun _ _ => False
  parentTs := fun _ => initTs
  tsValue := fun t v => t = initTs /\ v = initValue
  tsRmw := fun _ => False
  rmwConflict := fun _ => False
  invWrite := fun _ _ _ => False
  invRmw := fun _ _ _ => False
  ackMsg := fun _ _ _ => False
  valMsg := fun t => t = initTs
  o3Quorum := fun _ _ _ => False
  completeTry := fun _ => False
  o3Try := fun _ => False
  completed := fun t => t = initTs

structure InitAssumptions {TS : Type uTs} (ord : TotalOrder TS) (initTs : TS) : Prop where
  init_min : forall t, ord.le initTs t

structure Safety
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (ord : TotalOrder TS) (initTs : TS) (initValue : Value) (initEpoch : Epoch)
    (st : State Node TS Value Epoch) : Prop where
  -- Initial distinguished facts.
  seen_epoch_init : st.seenEpoch initEpoch
  seen_ts_init : st.seenTs initTs
  completed_init : st.completed initTs
  ts_value_init : st.tsValue initTs initValue

  -- Timestamp metadata is functional and well-formed.
  ts_value_functional :
    forall T V1 V2, st.tsValue T V1 -> st.tsValue T V2 -> V1 = V2
  parent_functional :
    forall T B1 B2, st.parent T B1 -> st.parent T B2 -> B1 = B2
  parent_seen :
    forall T B, st.parent T B -> st.seenTs T /\ st.seenTs B /\ lt ord B T
  ts_value_seen :
    forall T V, st.tsValue T V -> st.seenTs T
  ts_rmw_seen :
    forall T, st.tsRmw T -> st.seenTs T
  rmw_conflict_seen :
    forall T, st.rmwConflict T -> st.seenTs T
  rmw_conflict_rmw :
    forall T, st.rmwConflict T -> st.tsRmw T

  -- Persistent network buffers carry valid timestamp metadata.
  inv_write_wf :
    forall S T V, st.invWrite S T V -> st.seenTs T /\ st.tsValue T V /\ Not (st.tsRmw T)
  inv_rmw_wf :
    forall S T V, st.invRmw S T V -> st.seenTs T /\ st.tsValue T V /\ st.tsRmw T
  ack_msg_seen :
    forall A C T, st.ackMsg A C T -> st.seenTs T
  ack_msg_advanced :
    forall A C T, st.ackMsg A C T -> ord.le T (st.curTs A)
  val_msg_completed :
    forall T, st.valMsg T -> st.completed T
  completed_seen :
    forall T, st.completed T -> st.seenTs T
  o3_quorum_live_ack :
    forall N C T A, st.o3Quorum N C T -> st.live A -> st.ackMsg A C T
  o3_quorum_seen :
    forall N C T, st.o3Quorum N C T -> st.seenTs T

  -- Per-node metadata is consistent with timestamp metadata.
  cur_seen :
    forall N, st.seenTs (st.curTs N)
  cur_value_seen :
    forall N, st.tsValue (st.curTs N) (st.curValue N)
  cur_rmw_ts :
    forall N, st.curRmw N -> st.tsRmw (st.curTs N)
  cur_non_rmw_ts :
    forall N, Not (st.curRmw N) -> Not (st.tsRmw (st.curTs N))
  pending_seen :
    forall N, st.pending N -> st.seenTs (st.pendingTs N)
  pending_self_acked :
    forall N, st.pending N -> st.acked N N
  pending_below_cur :
    forall N, st.pending N -> ord.le (st.pendingTs N) (st.curTs N)
  pending_rmw_ts :
    forall N, st.pending N -> st.pendingRmw N -> st.tsRmw (st.pendingTs N)
  pending_non_rmw_ts :
    forall N, st.pending N -> Not (st.pendingRmw N) -> Not (st.tsRmw (st.pendingTs N))
  pending_rmw_current :
    forall N, st.pending N -> st.pendingRmw N -> st.pendingTs N = st.curTs N
  pending_rmw_cur_flag :
    forall N, st.pending N -> st.pendingRmw N -> st.curRmw N
  pending_acked_advanced :
    forall N A, st.pending N -> st.acked N A -> ord.le (st.pendingTs N) (st.curTs A)
  ready_pending :
    forall N, st.ready N -> st.pending N
  ready_live_acked :
    forall N A, st.ready N -> st.live A -> st.acked N A
  ready_ts_current_or_old :
    forall N, st.ready N -> st.pendingTs N = st.curTs N \/ lt ord (st.pendingTs N) (st.curTs N)
  ready_current_state :
    forall N, st.ready N -> st.pendingTs N = st.curTs N ->
      st.state N = HState.hs_write \/ st.state N = HState.hs_replay
  ready_old_state :
    forall N, st.ready N -> lt ord (st.pendingTs N) (st.curTs N) ->
      st.state N = HState.hs_invalid_write \/
        st.state N = HState.hs_invalid \/
        st.state N = HState.hs_valid
  pending_current_state :
    forall N, st.pending N -> st.pendingTs N = st.curTs N ->
      st.state N = HState.hs_write \/ st.state N = HState.hs_replay
  pending_old_state :
    forall N, st.pending N -> lt ord (st.pendingTs N) (st.curTs N) ->
      st.state N = HState.hs_invalid_write \/
        st.state N = HState.hs_invalid \/
        st.state N = HState.hs_valid
  not_pending_not_acked :
    forall N A, Not (st.pending N) -> Not (st.acked N A)
  not_pending_not_ready :
    forall N, Not (st.pending N) -> Not (st.ready N)

  -- Core Hermes safety facts.
  completed_live_advanced :
    forall T N, st.completed T -> st.live N -> ord.le T (st.curTs N)
  valid_completed :
    forall N, st.state N = HState.hs_valid -> st.completed (st.curTs N)
  valid_live_ts_agree :
    forall N1 N2,
      st.live N1 -> st.live N2 ->
      st.state N1 = HState.hs_valid -> st.state N2 = HState.hs_valid ->
      st.curTs N1 = st.curTs N2
  valid_live_value_agree :
    forall N1 N2,
      st.live N1 -> st.live N2 ->
      st.state N1 = HState.hs_valid -> st.state N2 = HState.hs_valid ->
      st.curValue N1 = st.curValue N2
  write_rmw_spacing :
    forall R W B, st.parent R B -> st.parent W B -> st.tsRmw R -> Not (st.tsRmw W) ->
      lt ord R W
  ready_rmw_no_completed_conflict :
    forall N B R,
      st.live N -> st.ready N -> st.pendingRmw N ->
      st.parent (st.pendingTs N) B -> st.completed R -> st.tsRmw R -> st.parent R B ->
      st.pendingTs N = R
  ready_rmw_same_base :
    forall N1 N2 B,
      st.live N1 -> st.ready N1 -> st.pendingRmw N1 ->
      st.parent (st.pendingTs N1) B ->
      st.live N2 -> st.ready N2 -> st.pendingRmw N2 ->
      st.parent (st.pendingTs N2) B ->
      st.pendingTs N1 = st.pendingTs N2
  completed_rmw_same_base :
    forall R1 R2 B,
      st.completed R1 -> st.completed R2 ->
      st.tsRmw R1 -> st.tsRmw R2 ->
      st.parent R1 B -> st.parent R2 B ->
      R1 = R2

  -- Liveness monitor invariants from the Ivy file.
  ready_live :
    forall N, st.ready N -> st.live N
  ready_epoch_seen :
    forall N, st.ready N -> st.seenEpoch (st.readyEpoch N)
  ready_epoch_not_init :
    forall N, st.ready N -> st.readyEpoch N = initEpoch -> False
  epoch_done_seen :
    forall E, st.epochDone E -> st.seenEpoch E
  no_complete_try :
    forall N, Not (st.completeTry N)
  no_o3_try :
    forall N, Not (st.o3Try N)
  ready_task_finish :
    st.readyTask = LTask.ready_finish
  o3_task_finish :
    st.o3Task = LTask.o3_finish

theorem valid_read_timestamps_agree
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch st) :
    forall N1 N2,
      st.live N1 -> st.live N2 ->
      st.state N1 = HState.hs_valid -> st.state N2 = HState.hs_valid ->
      st.curTs N1 = st.curTs N2 := by
  exact h.valid_live_ts_agree

theorem valid_read_values_agree
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch st) :
    forall N1 N2,
      st.live N1 -> st.live N2 ->
      st.state N1 = HState.hs_valid -> st.state N2 = HState.hs_valid ->
      st.curValue N1 = st.curValue N2 := by
  exact h.valid_live_value_agree

theorem valid_read_timestamps_agree_from_core
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch st) :
    forall N1 N2,
      st.live N1 -> st.live N2 ->
      st.state N1 = HState.hs_valid -> st.state N2 = HState.hs_valid ->
      st.curTs N1 = st.curTs N2 := by
  intro N1 N2 live1 live2 valid1 valid2
  have completed1 : st.completed (st.curTs N1) := h.valid_completed N1 valid1
  have completed2 : st.completed (st.curTs N2) := h.valid_completed N2 valid2
  have le12 : ord.le (st.curTs N1) (st.curTs N2) :=
    h.completed_live_advanced (st.curTs N1) N2 completed1 live2
  have le21 : ord.le (st.curTs N2) (st.curTs N1) :=
    h.completed_live_advanced (st.curTs N2) N1 completed2 live1
  exact ord.antisymm le12 le21

theorem valid_read_values_agree_from_core
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch st) :
    forall N1 N2,
      st.live N1 -> st.live N2 ->
      st.state N1 = HState.hs_valid -> st.state N2 = HState.hs_valid ->
      st.curValue N1 = st.curValue N2 := by
  intro N1 N2 live1 live2 valid1 valid2
  have tsEq : st.curTs N1 = st.curTs N2 :=
    valid_read_timestamps_agree_from_core h N1 N2 live1 live2 valid1 valid2
  have value1 : st.tsValue (st.curTs N1) (st.curValue N1) := h.cur_value_seen N1
  have value2 : st.tsValue (st.curTs N2) (st.curValue N2) := h.cur_value_seen N2
  have value1' : st.tsValue (st.curTs N2) (st.curValue N1) := by
    simpa [tsEq] using value1
  exact h.ts_value_functional (st.curTs N2) (st.curValue N1) (st.curValue N2) value1' value2

theorem completed_rmw_unique_per_base
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch st) :
    forall R1 R2 B,
      st.completed R1 -> st.completed R2 ->
      st.tsRmw R1 -> st.tsRmw R2 ->
      st.parent R1 B -> st.parent R2 B ->
      R1 = R2 := by
  exact h.completed_rmw_same_base

theorem rmw_conflict_seen_ts
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch st) :
    forall T, st.rmwConflict T -> st.seenTs T := by
  exact h.rmw_conflict_seen

theorem rmw_conflict_is_rmw
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch st) :
    forall T, st.rmwConflict T -> st.tsRmw T := by
  exact h.rmw_conflict_rmw

theorem o3_quorum_seen_ts
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch st) :
    forall N C T, st.o3Quorum N C T -> st.seenTs T := by
  exact h.o3_quorum_seen

theorem o3_quorum_live_ack_msg
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch st) :
    forall N C T A, st.o3Quorum N C T -> st.live A -> st.ackMsg A C T := by
  exact h.o3_quorum_live_ack

theorem o3_quorum_live_nodes_advanced
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch st) :
    forall N C T A, st.o3Quorum N C T -> st.live A -> ord.le T (st.curTs A) := by
  intro N C T A quorum liveA
  exact h.ack_msg_advanced A C T (h.o3_quorum_live_ack N C T A quorum liveA)

theorem init_safety
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    (hinit : InitAssumptions ord initTs) :
    Safety ord initTs initValue initEpoch
      (initState (Node := Node) (TS := TS) (Value := Value) (Epoch := Epoch)
        initTs initValue initEpoch) := by
  refine {
    seen_epoch_init := rfl
    seen_ts_init := rfl
    completed_init := rfl
    ts_value_init := And.intro rfl rfl
    ts_value_functional := by
      intro T V1 V2 h1 h2
      exact h1.2.trans h2.2.symm
    parent_functional := by
      intro T B1 B2 h
      cases h
    parent_seen := by
      intro T B h
      cases h
    ts_value_seen := by
      intro T V h
      exact h.1
    ts_rmw_seen := by
      intro T h
      cases h
    rmw_conflict_seen := by
      intro T h
      cases h
    rmw_conflict_rmw := by
      intro T h
      cases h
    inv_write_wf := by
      intro S T V h
      cases h
    inv_rmw_wf := by
      intro S T V h
      cases h
    ack_msg_seen := by
      intro A C T h
      cases h
    ack_msg_advanced := by
      intro A C T h
      cases h
    val_msg_completed := by
      intro T h
      exact h
    completed_seen := by
      intro T h
      exact h
    o3_quorum_live_ack := by
      intro N C T A hq _live
      cases hq
    o3_quorum_seen := by
      intro N C T hq
      cases hq
    cur_seen := by
      intro N
      rfl
    cur_value_seen := by
      intro N
      exact And.intro rfl rfl
    cur_rmw_ts := by
      intro N h
      cases h
    cur_non_rmw_ts := by
      intro N _h hts
      cases hts
    pending_seen := by
      intro N h
      cases h
    pending_self_acked := by
      intro N h
      cases h
    pending_below_cur := by
      intro N h
      cases h
    pending_rmw_ts := by
      intro N hp _hpr
      cases hp
    pending_non_rmw_ts := by
      intro N hp _hnpr
      cases hp
    pending_rmw_current := by
      intro N hp _hpr
      cases hp
    pending_rmw_cur_flag := by
      intro N hp _hpr
      cases hp
    pending_acked_advanced := by
      intro N A hp _ha
      cases hp
    ready_pending := by
      intro N hr
      cases hr
    ready_live_acked := by
      intro N A hr _live
      cases hr
    ready_ts_current_or_old := by
      intro N hr
      cases hr
    ready_current_state := by
      intro N hr _heq
      cases hr
    ready_old_state := by
      intro N hr _hlt
      cases hr
    pending_current_state := by
      intro N hp _heq
      cases hp
    pending_old_state := by
      intro N hp _hlt
      cases hp
    not_pending_not_acked := by
      intro N A _hnp ha
      cases ha
    not_pending_not_ready := by
      intro N _hnp hr
      cases hr
    completed_live_advanced := by
      intro T N hcomp _live
      subst T
      simpa [initState] using hinit.init_min initTs
    valid_completed := by
      intro N _hstate
      rfl
    valid_live_ts_agree := by
      intro N1 N2 _live1 _live2 _valid1 _valid2
      rfl
    valid_live_value_agree := by
      intro N1 N2 _live1 _live2 _valid1 _valid2
      rfl
    write_rmw_spacing := by
      intro R W B hparent _hparentW _hrmw _hnrmw
      cases hparent
    ready_rmw_no_completed_conflict := by
      intro N B R _live hready _hprmw _hparent _hcompleted _hrmw _hparentR
      cases hready
    ready_rmw_same_base := by
      intro N1 N2 B _live1 hready1 _hprmw1 _hparent1 _live2 _hready2 _hprmw2 _hparent2
      cases hready1
    completed_rmw_same_base := by
      intro R1 R2 B _hcompleted1 _hcompleted2 hrmw1 _hrmw2 _hparent1 _hparent2
      cases hrmw1
    ready_live := by
      intro N hready
      cases hready
    ready_epoch_seen := by
      intro N hready
      cases hready
    ready_epoch_not_init := by
      intro N hready
      cases hready
    epoch_done_seen := by
      intro E hdone
      exact hdone
    no_complete_try := by
      intro N htry
      cases htry
    no_o3_try := by
      intro N htry
      cases htry
    ready_task_finish := rfl
    o3_task_finish := rfl
  }

def Globally {World : Type uNode} (p : Nat -> World -> Prop) (tr : Nat -> World) : Prop :=
  forall i, p i (tr i)

def EventuallyFrom {World : Type uNode} (p : Nat -> World -> Prop) (tr : Nat -> World)
    (i : Nat) : Prop :=
  exists j, i <= j /\ p j (tr j)

def GloballyEventually {World : Type uNode} (p : Nat -> World -> Prop)
    (tr : Nat -> World) : Prop :=
  forall i, EventuallyFrom p tr i

def AlwaysFrom {World : Type uNode} (p : Nat -> World -> Prop) (tr : Nat -> World)
    (i : Nat) : Prop :=
  forall j, i <= j -> p j (tr j)

def ReadyEpochStuck
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (tr : Nat -> State Node TS Value Epoch) (N : Node) (E : Epoch) (i : Nat) : Prop :=
  (tr i).ready N /\ (tr i).readyEpoch N = E /\
    AlwaysFrom (fun _ st => Not (st.epochDone E)) tr i

def ReadyEpochLiveness
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (tr : Nat -> State Node TS Value Epoch) : Prop :=
  forall N E,
    GloballyEventually (fun _ st => st.completeTry N) tr ->
      Globally (fun i _ => Not (ReadyEpochStuck tr N E i)) tr

theorem ready_epochs_eventually_finish
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (tr : Nat -> State Node TS Value Epoch)
    (complete_try_finishes_ready_epoch :
      forall i N E,
        (tr i).completeTry N ->
        (tr i).ready N ->
        (tr i).readyEpoch N = E ->
        (tr i).epochDone E)
    (ready_epoch_persists_until_done :
      forall i j N E,
        i <= j ->
        (tr i).ready N ->
        (tr i).readyEpoch N = E ->
        (forall k, i <= k -> k <= j -> Not ((tr k).epochDone E)) ->
        (tr j).ready N /\ (tr j).readyEpoch N = E) :
    ReadyEpochLiveness tr := by
  intro N E fair
  intro i
  intro stuck
  rcases stuck with ⟨readyI, epochI, neverDone⟩
  rcases fair i with ⟨j, ij, completeJ⟩
  have notDoneBetween : forall k, i <= k -> k <= j -> Not ((tr k).epochDone E) := by
    intro k ik _kj
    exact neverDone k ik
  have readyJ : (tr j).ready N /\ (tr j).readyEpoch N = E :=
    ready_epoch_persists_until_done i j N E ij readyI epochI notDoneBetween
  have doneJ : (tr j).epochDone E :=
    complete_try_finishes_ready_epoch j N E completeJ readyJ.1 readyJ.2
  exact neverDone j ij doneJ

def O3QuorumStuck
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (tr : Nat -> State Node TS Value Epoch) (N C : Node) (T : TS) (i : Nat) : Prop :=
  (tr i).o3Quorum N C T /\
    (tr i).curTs N = T /\
    ((tr i).state N = HState.hs_valid -> False) /\
    Not ((tr i).completed T) /\
    Not ((tr i).rmwConflict T) /\
    AlwaysFrom
      (fun _ st =>
        st.curTs N = T /\
          (st.state N = HState.hs_valid -> False) /\
          Not (st.completed T) /\
          Not (st.rmwConflict T))
      tr i

def O3QuorumLiveness
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (tr : Nat -> State Node TS Value Epoch) : Prop :=
  forall N C T,
    GloballyEventually (fun _ st => st.o3Try N) tr ->
      Globally (fun i _ => Not (O3QuorumStuck tr N C T i)) tr

theorem o3_ack_quorums_eventually_finish_or_conflict
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (tr : Nat -> State Node TS Value Epoch)
    (o3_try_finishes_quorum :
      forall i N C T,
        (tr i).o3Try N ->
        (tr i).o3Quorum N C T ->
        (tr i).curTs N = T ->
        ((tr i).state N = HState.hs_valid -> False) ->
        Not ((tr i).completed T) ->
        Not ((tr i).rmwConflict T) ->
        (tr i).completed T \/
          (tr i).state N = HState.hs_valid \/
          ((tr i).curTs N = T -> False) \/
          (tr i).rmwConflict T)
    (o3_quorum_persists :
      forall i j N C T,
        i <= j ->
        (tr i).o3Quorum N C T ->
        (tr j).o3Quorum N C T) :
    O3QuorumLiveness tr := by
  intro N C T fair
  intro i
  intro stuck
  rcases stuck with ⟨quorumI, _curI, _stateI, _notCompletedI, _notConflictI, staysBad⟩
  rcases fair i with ⟨j, ij, o3TryJ⟩
  have quorumJ : (tr j).o3Quorum N C T :=
    o3_quorum_persists i j N C T ij quorumI
  have badJ :
      (tr j).curTs N = T /\
        ((tr j).state N = HState.hs_valid -> False) /\
        Not ((tr j).completed T) /\
        Not ((tr j).rmwConflict T) :=
    staysBad j ij
  have doneJ :
      (tr j).completed T \/
        (tr j).state N = HState.hs_valid \/
        ((tr j).curTs N = T -> False) \/
        (tr j).rmwConflict T :=
    o3_try_finishes_quorum j N C T
      o3TryJ quorumJ badJ.1 badJ.2.1 badJ.2.2.1 badJ.2.2.2
  rcases doneJ with completedJ | validOrMovedOrConflict
  · exact badJ.2.2.1 completedJ
  · rcases validOrMovedOrConflict with validJ | movedOrConflict
    · exact badJ.2.1 validJ
    · rcases movedOrConflict with movedJ | conflictJ
      · exact movedJ badJ.1
      · exact badJ.2.2.2 conflictJ

def O3RmwQuorumStuck
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (tr : Nat -> State Node TS Value Epoch) (N C : Node) (T : TS) (i : Nat) : Prop :=
  (tr i).o3Quorum N C T /\
    (tr i).tsRmw T /\
    (tr i).curTs N = T /\
    ((tr i).state N = HState.hs_valid -> False) /\
    Not ((tr i).completed T) /\
    Not ((tr i).rmwConflict T) /\
    AlwaysFrom
      (fun _ st =>
        st.curTs N = T /\
          (st.state N = HState.hs_valid -> False) /\
          Not (st.completed T) /\
          Not (st.rmwConflict T))
      tr i

def O3RmwQuorumLiveness
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (tr : Nat -> State Node TS Value Epoch) : Prop :=
  forall N C T,
    GloballyEventually (fun _ st => st.o3Try N) tr ->
      Globally (fun i _ => Not (O3RmwQuorumStuck tr N C T i)) tr

theorem o3_rmw_ack_quorums_eventually_finish_or_conflict
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (tr : Nat -> State Node TS Value Epoch)
    (o3_try_finishes_rmw_quorum :
      forall i N C T,
        (tr i).o3Try N ->
        (tr i).o3Quorum N C T ->
        (tr i).tsRmw T ->
        (tr i).curTs N = T ->
        ((tr i).state N = HState.hs_valid -> False) ->
        Not ((tr i).completed T) ->
        Not ((tr i).rmwConflict T) ->
        (tr i).completed T \/
          (tr i).state N = HState.hs_valid \/
          ((tr i).curTs N = T -> False) \/
          (tr i).rmwConflict T)
    (o3_quorum_persists :
      forall i j N C T,
        i <= j ->
        (tr i).o3Quorum N C T ->
        (tr j).o3Quorum N C T)
    (ts_rmw_persists :
      forall i j T,
        i <= j ->
        (tr i).tsRmw T ->
        (tr j).tsRmw T) :
    O3RmwQuorumLiveness tr := by
  intro N C T fair
  intro i
  intro stuck
  rcases stuck with ⟨quorumI, rmwI, _curI, _stateI, _notCompletedI, _notConflictI, staysBad⟩
  rcases fair i with ⟨j, ij, o3TryJ⟩
  have quorumJ : (tr j).o3Quorum N C T :=
    o3_quorum_persists i j N C T ij quorumI
  have rmwJ : (tr j).tsRmw T :=
    ts_rmw_persists i j T ij rmwI
  have badJ :
      (tr j).curTs N = T /\
        ((tr j).state N = HState.hs_valid -> False) /\
        Not ((tr j).completed T) /\
        Not ((tr j).rmwConflict T) :=
    staysBad j ij
  have doneJ :
      (tr j).completed T \/
        (tr j).state N = HState.hs_valid \/
        ((tr j).curTs N = T -> False) \/
        (tr j).rmwConflict T :=
    o3_try_finishes_rmw_quorum j N C T
      o3TryJ quorumJ rmwJ badJ.1 badJ.2.1 badJ.2.2.1 badJ.2.2.2
  rcases doneJ with completedJ | validOrMovedOrConflict
  · exact badJ.2.2.1 completedJ
  · rcases validOrMovedOrConflict with validJ | movedOrConflict
    · exact badJ.2.1 validJ
    · rcases movedOrConflict with movedJ | conflictJ
      · exact movedJ badJ.1
      · exact badJ.2.2.2 conflictJ

/-!
## Operational model

The theorems above are useful as small logical lemmas, but the protocol proof
below is the stand-alone check: it defines concrete Hermes actions, combines
them in `HRNext`, proves the inductive invariant is initialized and preserved
by every action, and states the safety/liveness facts over reachable traces.
-/

noncomputable section Operational
open Classical

def upd {alpha : Sort uNode} {beta : Sort uTs}
    (f : alpha -> beta) (x : alpha) (v : beta) : alpha -> beta :=
  fun y => if y = x then v else f y

def add1 {alpha : Sort uNode} (r : alpha -> Prop) (x : alpha) : alpha -> Prop :=
  fun y => r y \/ y = x

def clear1 {alpha : Sort uNode} (r : alpha -> Prop) (x : alpha) : alpha -> Prop :=
  fun y => r y /\ y ≠ x

def set1 {alpha : Sort uNode} (r : alpha -> Prop) (x : alpha) (b : Prop) : alpha -> Prop :=
  fun y => if y = x then b else r y

def add2 {alpha : Sort uNode} {beta : Sort uTs}
    (r : alpha -> beta -> Prop) (x : alpha) (y : beta) : alpha -> beta -> Prop :=
  fun a b => r a b \/ (a = x /\ b = y)

def clear2First {alpha : Sort uNode} {beta : Sort uTs}
    (r : alpha -> beta -> Prop) (x : alpha) : alpha -> beta -> Prop :=
  fun a b => r a b /\ a ≠ x

def set2FirstSelf {alpha : Sort uNode}
    (r : alpha -> alpha -> Prop) (x : alpha) : alpha -> alpha -> Prop :=
  fun a b => if a = x then b = x else r a b

def add3 {alpha : Sort uNode} {beta : Sort uTs} {gamma : Sort uValue}
    (r : alpha -> beta -> gamma -> Prop)
    (x : alpha) (y : beta) (z : gamma) : alpha -> beta -> gamma -> Prop :=
  fun a b c => r a b c \/ (a = x /\ b = y /\ c = z)

def addTsValue {TS : Type uTs} {Value : Type uValue}
    (r : TS -> Value -> Prop) (t : TS) (v : Value) : TS -> Value -> Prop :=
  fun t' v' => r t' v' \/ (t' = t /\ v' = v)

def addParent {TS : Type uTs}
    (r : TS -> TS -> Prop) (t base : TS) : TS -> TS -> Prop :=
  fun t' b' => r t' b' \/ (t' = t /\ b' = base)

def removeTs {TS : Type uTs} (r : TS -> Prop) (t : TS) : TS -> Prop :=
  fun t' => r t' /\ t' ≠ t

def addRmwConflicts {TS : Type uTs}
    (tsRmw rmwConflict : TS -> Prop) (parentTs : TS -> TS) (winner : TS) :
    TS -> Prop :=
  fun t => rmwConflict t \/
    (tsRmw winner /\ tsRmw t /\ t ≠ winner /\ parentTs winner = parentTs t)

inductive HRLabel (Node : Type uNode) where
  | silent
  | completeReady (n : Node)
  | o3Complete (n : Node)
deriving Repr

def localWritePost
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (st : State Node TS Value Epoch) (n : Node) (t : TS) (v : Value) :
    State Node TS Value Epoch :=
  { st with
    state := upd st.state n HState.hs_write
    curTs := upd st.curTs n t
    curValue := upd st.curValue n v
    curRmw := set1 st.curRmw n False
    lastWriter := upd st.lastWriter n n
    pending := set1 st.pending n True
    pendingTs := upd st.pendingTs n t
    pendingRmw := set1 st.pendingRmw n False
    acked := set2FirstSelf st.acked n
    ready := set1 st.ready n False
    seenTs := add1 st.seenTs t
    parent := addParent st.parent t (st.curTs n)
    parentTs := upd st.parentTs t (st.curTs n)
    tsValue := addTsValue st.tsValue t v
    tsRmw := removeTs st.tsRmw t
    rmwConflict := removeTs st.rmwConflict t
    invWrite := add3 st.invWrite n t v }

def localRmwPost
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (st : State Node TS Value Epoch) (n : Node) (t : TS) (v : Value) :
    State Node TS Value Epoch :=
  { st with
    state := upd st.state n HState.hs_write
    curTs := upd st.curTs n t
    curValue := upd st.curValue n v
    curRmw := set1 st.curRmw n True
    lastWriter := upd st.lastWriter n n
    pending := set1 st.pending n True
    pendingTs := upd st.pendingTs n t
    pendingRmw := set1 st.pendingRmw n True
    acked := set2FirstSelf st.acked n
    ready := set1 st.ready n False
    seenTs := add1 st.seenTs t
    parent := addParent st.parent t (st.curTs n)
    parentTs := upd st.parentTs t (st.curTs n)
    tsValue := addTsValue st.tsValue t v
    tsRmw := add1 st.tsRmw t
    rmwConflict := removeTs st.rmwConflict t
    invRmw := add3 st.invRmw n t v }

def receiveWriteInvPost
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (ord : TotalOrder TS) (st : State Node TS Value Epoch)
    (n s : Node) (t : TS) (v : Value) : State Node TS Value Epoch :=
  let ackedMsg := add3 st.ackMsg n s t
  if lt ord (st.curTs n) t then
    let overwritesRmw := st.pending n /\ st.pendingRmw n
    { st with
      state := upd st.state n
        (if overwritesRmw then HState.hs_invalid
         else if st.pending n then HState.hs_invalid_write
         else HState.hs_invalid)
      curTs := upd st.curTs n t
      curValue := upd st.curValue n v
      curRmw := set1 st.curRmw n False
      lastWriter := upd st.lastWriter n s
      pending := if overwritesRmw then set1 st.pending n False else st.pending
      pendingRmw := if overwritesRmw then set1 st.pendingRmw n False else st.pendingRmw
      acked := if overwritesRmw then clear2First st.acked n else st.acked
      ready := if overwritesRmw then set1 st.ready n False else st.ready
      epochDone := if overwritesRmw /\ st.ready n then add1 st.epochDone (st.readyEpoch n) else st.epochDone
      ackMsg := ackedMsg }
  else
    { st with ackMsg := ackedMsg }

def receiveRmwInvPost
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (ord : TotalOrder TS) (st : State Node TS Value Epoch)
    (n s : Node) (t : TS) (v : Value) : State Node TS Value Epoch :=
  if lt ord (st.curTs n) t then
    let overwritesRmw := st.pending n /\ st.pendingRmw n
    { st with
      state := upd st.state n
        (if overwritesRmw then HState.hs_invalid
         else if st.pending n then HState.hs_invalid_write
         else HState.hs_invalid)
      curTs := upd st.curTs n t
      curValue := upd st.curValue n v
      curRmw := set1 st.curRmw n True
      lastWriter := upd st.lastWriter n s
      pending := if overwritesRmw then set1 st.pending n False else st.pending
      pendingRmw := if overwritesRmw then set1 st.pendingRmw n False else st.pendingRmw
      acked := if overwritesRmw then clear2First st.acked n else st.acked
      ready := if overwritesRmw then set1 st.ready n False else st.ready
      epochDone := if overwritesRmw /\ st.ready n then add1 st.epochDone (st.readyEpoch n) else st.epochDone
      ackMsg := add3 st.ackMsg n s t }
  else if st.curTs n = t then
    { st with ackMsg := add3 st.ackMsg n s t }
  else if st.curRmw n then
    { st with invRmw := add3 st.invRmw n (st.curTs n) (st.curValue n) }
  else
    { st with invWrite := add3 st.invWrite n (st.curTs n) (st.curValue n) }

def receiveRmwInvCompletedConflictPost
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (st : State Node TS Value Epoch) (n : Node) : State Node TS Value Epoch :=
  if st.curRmw n then
    { st with invRmw := add3 st.invRmw n (st.curTs n) (st.curValue n) }
  else
    { st with invWrite := add3 st.invWrite n (st.curTs n) (st.curValue n) }

def receiveAckPost
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (st : State Node TS Value Epoch) (n a : Node) : State Node TS Value Epoch :=
  { st with
    epochDone := if st.ready n then add1 st.epochDone (st.readyEpoch n) else st.epochDone
    acked := add2 st.acked n a
    ready := set1 st.ready n False }

def markReadyPost
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (st : State Node TS Value Epoch) (n : Node) (e : Epoch) : State Node TS Value Epoch :=
  { st with
    seenEpoch := add1 st.seenEpoch e
    epochDone := clear1 st.epochDone e
    readyEpoch := upd st.readyEpoch n e
    ready := set1 st.ready n True }

def completeCurrentPost
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (st : State Node TS Value Epoch) (n : Node) : State Node TS Value Epoch :=
  { st with
    completed := add1 st.completed (st.curTs n)
    rmwConflict := addRmwConflicts st.tsRmw st.rmwConflict st.parentTs (st.curTs n)
    valMsg := add1 st.valMsg (st.curTs n)
    state := upd st.state n HState.hs_valid
    epochDone := add1 st.epochDone (st.readyEpoch n)
    pending := set1 st.pending n False
    pendingRmw := set1 st.pendingRmw n False
    acked := clear2First st.acked n
    ready := set1 st.ready n False
    completeTry := fun _ => False }

def completeOverwrittenPost
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (st : State Node TS Value Epoch) (n : Node) : State Node TS Value Epoch :=
  { st with
    completed := add1 st.completed (st.pendingTs n)
    rmwConflict := addRmwConflicts st.tsRmw st.rmwConflict st.parentTs (st.pendingTs n)
    epochDone := add1 st.epochDone (st.readyEpoch n)
    pending := set1 st.pending n False
    pendingRmw := set1 st.pendingRmw n False
    acked := clear2First st.acked n
    ready := set1 st.ready n False
    state := if st.state n = HState.hs_invalid_write then upd st.state n HState.hs_invalid else st.state
    completeTry := fun _ => False }

def receiveValidatePost
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (st : State Node TS Value Epoch) (n : Node) (t : TS) : State Node TS Value Epoch :=
  if st.curTs n = t then
    let clearsPending := st.pending n /\ st.pendingTs n = st.curTs n
    { st with
      state := upd st.state n HState.hs_valid
      epochDone := if clearsPending /\ st.ready n then add1 st.epochDone (st.readyEpoch n) else st.epochDone
      pending := if clearsPending then set1 st.pending n False else st.pending
      pendingRmw := if clearsPending then set1 st.pendingRmw n False else st.pendingRmw
      acked := if clearsPending then clear2First st.acked n else st.acked
      ready := if clearsPending then set1 st.ready n False else st.ready }
  else
    st

def replayAfterFailurePost
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (st : State Node TS Value Epoch) (n : Node) : State Node TS Value Epoch :=
  let base :=
    { st with
      state := upd st.state n HState.hs_replay
      pending := set1 st.pending n True
      pendingTs := upd st.pendingTs n (st.curTs n)
      pendingRmw := set1 st.pendingRmw n (st.curRmw n)
      acked := set2FirstSelf st.acked n
      epochDone := if st.ready n then add1 st.epochDone (st.readyEpoch n) else st.epochDone
      ready := set1 st.ready n False }
  if st.curRmw n then
    { base with invRmw := add3 st.invRmw n (st.curTs n) (st.curValue n) }
  else
    { base with invWrite := add3 st.invWrite n (st.curTs n) (st.curValue n) }

def failPost
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (st : State Node TS Value Epoch) (n : Node) : State Node TS Value Epoch :=
  { st with
    live := set1 st.live n False
    pending := set1 st.pending n False
    pendingRmw := set1 st.pendingRmw n False
    acked := clear2First st.acked n
    epochDone := if st.ready n then add1 st.epochDone (st.readyEpoch n) else st.epochDone
    ready := set1 st.ready n False }

def o3ObserveQuorumPost
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (st : State Node TS Value Epoch) (n c : Node) (t : TS) : State Node TS Value Epoch :=
  { st with o3Quorum := add3 st.o3Quorum n c t }

def o3CompletePost
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (st : State Node TS Value Epoch) (n : Node) (t : TS) : State Node TS Value Epoch :=
  let clearsPending := st.pending n /\ st.pendingTs n = t
  { st with
    completed := add1 st.completed t
    rmwConflict := addRmwConflicts st.tsRmw st.rmwConflict st.parentTs t
    state := upd st.state n HState.hs_valid
    epochDone := if clearsPending /\ st.ready n then add1 st.epochDone (st.readyEpoch n) else st.epochDone
    pending := if clearsPending then set1 st.pending n False else st.pending
    pendingRmw := if clearsPending then set1 st.pendingRmw n False else st.pendingRmw
    acked := if clearsPending then clear2First st.acked n else st.acked
    ready := if clearsPending then set1 st.ready n False else st.ready
    o3Try := fun _ => False }

def OpInvariant
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (ord : TotalOrder TS) (initTs : TS) (initValue : Value) (initEpoch : Epoch)
    (st : State Node TS Value Epoch) : Prop :=
  st.seenEpoch initEpoch /\
  st.seenTs initTs /\
  st.completed initTs /\
  st.tsValue initTs initValue /\
  (forall T V1 V2, st.tsValue T V1 -> st.tsValue T V2 -> V1 = V2) /\
  (forall T B1 B2, st.parent T B1 -> st.parent T B2 -> B1 = B2) /\
  (forall T B, st.parent T B -> st.seenTs T /\ st.seenTs B /\ lt ord B T) /\
  (forall T V, st.tsValue T V -> st.seenTs T) /\
  (forall T, st.tsRmw T -> st.seenTs T) /\
  (forall T, st.rmwConflict T -> st.seenTs T) /\
  (forall T, st.rmwConflict T -> st.tsRmw T) /\
  (forall S T V, st.invWrite S T V -> st.seenTs T /\ st.tsValue T V /\ Not (st.tsRmw T)) /\
  (forall S T V, st.invRmw S T V -> st.seenTs T /\ st.tsValue T V /\ st.tsRmw T) /\
  (forall A C T, st.ackMsg A C T -> st.seenTs T) /\
  (forall A C T, st.ackMsg A C T -> ord.le T (st.curTs A)) /\
  (forall T, st.valMsg T -> st.completed T) /\
  (forall T, st.completed T -> st.seenTs T) /\
  (forall N C T A, st.o3Quorum N C T -> st.live A -> st.ackMsg A C T) /\
  (forall N C T, st.o3Quorum N C T -> st.seenTs T) /\
  (forall N, st.seenTs (st.curTs N)) /\
  (forall N, st.tsValue (st.curTs N) (st.curValue N)) /\
  (forall N, st.curRmw N -> st.tsRmw (st.curTs N)) /\
  (forall N, Not (st.curRmw N) -> Not (st.tsRmw (st.curTs N))) /\
  (forall N, st.pending N -> st.seenTs (st.pendingTs N)) /\
  (forall N, st.pending N -> st.acked N N) /\
  (forall N, st.pending N -> ord.le (st.pendingTs N) (st.curTs N)) /\
  (forall N, st.pending N -> st.pendingRmw N -> st.tsRmw (st.pendingTs N)) /\
  (forall N, st.pending N -> Not (st.pendingRmw N) -> Not (st.tsRmw (st.pendingTs N))) /\
  (forall N, st.pending N -> st.pendingRmw N -> st.pendingTs N = st.curTs N) /\
  (forall N, st.pending N -> st.pendingRmw N -> st.curRmw N) /\
  (forall N A, st.pending N -> st.acked N A -> ord.le (st.pendingTs N) (st.curTs A)) /\
  (forall N, st.ready N -> st.pending N) /\
  (forall N A, st.ready N -> st.live A -> st.acked N A) /\
  (forall N, st.ready N -> st.pendingTs N = st.curTs N \/ lt ord (st.pendingTs N) (st.curTs N)) /\
  (forall N, st.ready N -> st.pendingTs N = st.curTs N ->
    st.state N = HState.hs_write \/ st.state N = HState.hs_replay) /\
  (forall N, st.ready N -> lt ord (st.pendingTs N) (st.curTs N) ->
    st.state N = HState.hs_invalid_write \/ st.state N = HState.hs_invalid \/ st.state N = HState.hs_valid) /\
  (forall N, st.pending N -> st.pendingTs N = st.curTs N ->
    st.state N = HState.hs_write \/ st.state N = HState.hs_replay) /\
  (forall N, st.pending N -> lt ord (st.pendingTs N) (st.curTs N) ->
    st.state N = HState.hs_invalid_write \/ st.state N = HState.hs_invalid \/ st.state N = HState.hs_valid) /\
  (forall N A, Not (st.pending N) -> Not (st.acked N A)) /\
  (forall N, Not (st.pending N) -> Not (st.ready N)) /\
  (forall T N, st.completed T -> st.live N -> ord.le T (st.curTs N)) /\
  (forall N, st.state N = HState.hs_valid -> st.completed (st.curTs N)) /\
  (forall R W B, st.parent R B -> st.parent W B -> st.tsRmw R -> Not (st.tsRmw W) -> lt ord R W) /\
  (forall N B R, st.live N -> st.ready N -> st.pendingRmw N ->
    st.parent (st.pendingTs N) B -> st.completed R -> st.tsRmw R -> st.parent R B ->
    st.pendingTs N = R) /\
  (forall R1 R2 B, st.completed R1 -> st.completed R2 ->
    st.tsRmw R1 -> st.tsRmw R2 -> st.parent R1 B -> st.parent R2 B -> R1 = R2) /\
  (forall N, st.ready N -> st.live N) /\
  (forall N, st.ready N -> st.seenEpoch (st.readyEpoch N)) /\
  (forall N, st.ready N -> st.readyEpoch N = initEpoch -> False) /\
  (forall E, st.epochDone E -> st.seenEpoch E) /\
  (forall N, Not (st.completeTry N)) /\
  (forall N, Not (st.o3Try N)) /\
  st.readyTask = LTask.ready_finish /\
  st.o3Task = LTask.o3_finish

inductive HRNext
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (ord : TotalOrder TS) :
    State Node TS Value Epoch -> HRLabel Node -> State Node TS Value Epoch -> Prop where
  | local_write {st n t v} :
      st.live n ->
      Not (st.pending n) ->
      (st.state n = HState.hs_valid \/ st.state n = HState.hs_invalid) ->
      lt ord (st.curTs n) t ->
      Not (st.seenTs t) ->
      (forall R, st.parent R (st.curTs n) -> st.tsRmw R -> lt ord R t) ->
      HRNext ord st HRLabel.silent (localWritePost st n t v)
  | local_rmw {st n t v} :
      st.live n ->
      Not (st.pending n) ->
      st.state n = HState.hs_valid ->
      lt ord (st.curTs n) t ->
      Not (st.seenTs t) ->
      (forall W, st.parent W (st.curTs n) -> Not (st.tsRmw W) -> lt ord t W) ->
      HRNext ord st HRLabel.silent (localRmwPost st n t v)
  | receive_write_inv {st n s t v} :
      st.live n ->
      n ≠ s ->
      st.invWrite s t v ->
      HRNext ord st HRLabel.silent (receiveWriteInvPost ord st n s t v)
  | receive_rmw_inv {st n s t v} :
      st.live n ->
      n ≠ s ->
      st.invRmw s t v ->
      (forall B R, st.parent t B -> st.completed R -> st.tsRmw R -> st.parent R B -> R = t) ->
      HRNext ord st HRLabel.silent (receiveRmwInvPost ord st n s t v)
  | receive_rmw_inv_completed_conflict {st n s t v b r} :
      st.live n ->
      n ≠ s ->
      st.invRmw s t v ->
      st.parent t b ->
      st.completed r ->
      st.tsRmw r ->
      st.parent r b ->
      r ≠ t ->
      HRNext ord st HRLabel.silent (receiveRmwInvCompletedConflictPost st n)
  | receive_ack {st n a t} :
      st.live n ->
      st.pending n ->
      st.pendingTs n = t ->
      st.ackMsg a n t ->
      HRNext ord st HRLabel.silent (receiveAckPost st n a)
  | mark_ready {st n e} :
      st.live n ->
      st.pending n ->
      Not (st.ready n) ->
      Not (st.seenEpoch e) ->
      (forall A, st.live A -> st.acked n A) ->
      (forall B R, st.pendingRmw n -> st.parent (st.pendingTs n) B ->
        st.completed R -> st.tsRmw R -> st.parent R B -> R = st.pendingTs n) ->
      HRNext ord st HRLabel.silent (markReadyPost st n e)
  | complete_current {st n} :
      st.live n ->
      st.pending n ->
      st.pendingTs n = st.curTs n ->
      (st.state n = HState.hs_write \/ st.state n = HState.hs_replay) ->
      st.ready n ->
      HRNext ord st HRLabel.silent (completeCurrentPost st n)
  | complete_overwritten {st n} :
      st.live n ->
      st.pending n ->
      lt ord (st.pendingTs n) (st.curTs n) ->
      st.ready n ->
      HRNext ord st HRLabel.silent (completeOverwrittenPost st n)
  | complete_ready_current {st n} :
      st.live n ->
      st.pending n ->
      st.pendingTs n = st.curTs n ->
      (st.state n = HState.hs_write \/ st.state n = HState.hs_replay) ->
      st.ready n ->
      HRNext ord st (HRLabel.completeReady n) (completeCurrentPost st n)
  | complete_ready_overwritten {st n} :
      st.live n ->
      st.pending n ->
      lt ord (st.pendingTs n) (st.curTs n) ->
      st.ready n ->
      HRNext ord st (HRLabel.completeReady n) (completeOverwrittenPost st n)
  | receive_validate {st n t} :
      st.live n ->
      st.valMsg t ->
      HRNext ord st HRLabel.silent (receiveValidatePost st n t)
  | replay_after_failure {st n} :
      st.live n ->
      Not (st.pending n) ->
      st.state n = HState.hs_invalid ->
      Not (st.live (st.lastWriter n)) ->
      (forall B R, st.curRmw n -> st.parent (st.curTs n) B ->
        st.completed R -> st.tsRmw R -> st.parent R B -> R = st.curTs n) ->
      HRNext ord st HRLabel.silent (replayAfterFailurePost st n)
  | fail {st n} :
      st.live n ->
      HRNext ord st HRLabel.silent (failPost st n)
  | o3_observe_quorum {st n c t} :
      st.live n ->
      (forall A, st.live A -> st.ackMsg A c t) ->
      HRNext ord st HRLabel.silent (o3ObserveQuorumPost st n c t)
  | o3_complete {st n c t} :
      st.live n ->
      st.curTs n = t ->
      st.state n ≠ HState.hs_valid ->
      st.o3Quorum n c t ->
      Not (st.rmwConflict t) ->
      (forall B R, st.tsRmw t -> st.parent t B -> st.completed R ->
        st.tsRmw R -> st.parent R B -> R = t) ->
      HRNext ord st (HRLabel.o3Complete n) (o3CompletePost st n t)

theorem op_invariant_of_safety
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch st) :
    OpInvariant ord initTs initValue initEpoch st := by
  unfold OpInvariant
  exact And.intro h.seen_epoch_init <| And.intro h.seen_ts_init <|
    And.intro h.completed_init <| And.intro h.ts_value_init <|
    And.intro h.ts_value_functional <| And.intro h.parent_functional <|
    And.intro h.parent_seen <| And.intro h.ts_value_seen <|
    And.intro h.ts_rmw_seen <| And.intro h.rmw_conflict_seen <|
    And.intro h.rmw_conflict_rmw <| And.intro h.inv_write_wf <|
    And.intro h.inv_rmw_wf <| And.intro h.ack_msg_seen <|
    And.intro h.ack_msg_advanced <| And.intro h.val_msg_completed <|
    And.intro h.completed_seen <| And.intro h.o3_quorum_live_ack <|
    And.intro h.o3_quorum_seen <| And.intro h.cur_seen <|
    And.intro h.cur_value_seen <| And.intro h.cur_rmw_ts <|
    And.intro h.cur_non_rmw_ts <| And.intro h.pending_seen <|
    And.intro h.pending_self_acked <| And.intro h.pending_below_cur <|
    And.intro h.pending_rmw_ts <| And.intro h.pending_non_rmw_ts <|
    And.intro h.pending_rmw_current <| And.intro h.pending_rmw_cur_flag <|
    And.intro h.pending_acked_advanced <| And.intro h.ready_pending <|
    And.intro h.ready_live_acked <| And.intro h.ready_ts_current_or_old <|
    And.intro h.ready_current_state <| And.intro h.ready_old_state <|
    And.intro h.pending_current_state <| And.intro h.pending_old_state <|
    And.intro h.not_pending_not_acked <| And.intro h.not_pending_not_ready <|
    And.intro h.completed_live_advanced <| And.intro h.valid_completed <|
    And.intro h.write_rmw_spacing <| And.intro h.ready_rmw_no_completed_conflict <|
    And.intro h.completed_rmw_same_base <| And.intro h.ready_live <|
    And.intro h.ready_epoch_seen <| And.intro h.ready_epoch_not_init <|
    And.intro h.epoch_done_seen <| And.intro h.no_complete_try <|
    And.intro h.no_o3_try <| And.intro h.ready_task_finish h.o3_task_finish

theorem op_init_invariant
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    (hinit : InitAssumptions ord initTs) :
    OpInvariant ord initTs initValue initEpoch
      (initState (Node := Node) (TS := TS) (Value := Value) (Epoch := Epoch)
        initTs initValue initEpoch) := by
  exact op_invariant_of_safety (init_safety hinit)

theorem op_valid_read_timestamps_agree
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch}
    (h : OpInvariant ord initTs initValue initEpoch st) :
    forall N1 N2,
      st.live N1 -> st.live N2 ->
      st.state N1 = HState.hs_valid -> st.state N2 = HState.hs_valid ->
      st.curTs N1 = st.curTs N2 := by
  unfold OpInvariant at h
  grind [TotalOrder.antisymm]

theorem op_valid_read_values_agree
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch}
    (h : OpInvariant ord initTs initValue initEpoch st) :
    forall N1 N2,
      st.live N1 -> st.live N2 ->
      st.state N1 = HState.hs_valid -> st.state N2 = HState.hs_valid ->
      st.curValue N1 = st.curValue N2 := by
  intro N1 N2 live1 live2 valid1 valid2
  unfold OpInvariant at h
  have tsEq : st.curTs N1 = st.curTs N2 := by
    grind [TotalOrder.antisymm]
  have value1 : st.tsValue (st.curTs N1) (st.curValue N1) := by grind
  have value2 : st.tsValue (st.curTs N2) (st.curValue N2) := by grind
  have value1' : st.tsValue (st.curTs N2) (st.curValue N1) := by
    simpa [tsEq] using value1
  grind

theorem op_completed_rmw_unique_per_base
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch}
    (h : OpInvariant ord initTs initValue initEpoch st) :
    forall R1 R2 B,
      st.completed R1 -> st.completed R2 ->
      st.tsRmw R1 -> st.tsRmw R2 ->
      st.parent R1 B -> st.parent R2 B ->
      R1 = R2 := by
  unfold OpInvariant at h
  grind

theorem op_o3_quorum_live_nodes_advanced
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch}
    (h : OpInvariant ord initTs initValue initEpoch st) :
    forall N C T A, st.o3Quorum N C T -> st.live A -> ord.le T (st.curTs A) := by
  unfold OpInvariant at h
  grind

theorem le_of_not_lt {TS : Type uTs} (ord : TotalOrder TS) {x y : TS} :
    Not (lt ord x y) -> ord.le y x := by
  intro hNot
  cases ord.total x y with
  | inl hxy =>
      by_cases hEq : x = y
      · subst hEq
        exact ord.refl x
      · exact False.elim (hNot (And.intro hxy hEq))
  | inr hyx => exact hyx

structure CoreInvariant
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (ord : TotalOrder TS) (initTs : TS) (initValue : Value) (initEpoch : Epoch)
    (st : State Node TS Value Epoch) : Prop where
  ts_value_functional :
    forall T V1 V2, st.tsValue T V1 -> st.tsValue T V2 -> V1 = V2
  parent_seen :
    forall T B, st.parent T B -> st.seenTs T /\ st.seenTs B /\ lt ord B T
  ts_value_seen :
    forall T V, st.tsValue T V -> st.seenTs T
  ts_rmw_seen :
    forall T, st.tsRmw T -> st.seenTs T
  rmw_conflict_seen :
    forall T, st.rmwConflict T -> st.seenTs T
  rmw_conflict_rmw :
    forall T, st.rmwConflict T -> st.tsRmw T
  inv_write_wf :
    forall S T V, st.invWrite S T V -> st.seenTs T /\ st.tsValue T V /\ Not (st.tsRmw T)
  inv_rmw_wf :
    forall S T V, st.invRmw S T V -> st.seenTs T /\ st.tsValue T V /\ st.tsRmw T
  ack_msg_advanced :
    forall A C T, st.ackMsg A C T -> ord.le T (st.curTs A)
  val_msg_completed :
    forall T, st.valMsg T -> st.completed T
  o3_quorum_live_ack :
    forall N C T A, st.o3Quorum N C T -> st.live A -> st.ackMsg A C T
  cur_value_seen :
    forall N, st.tsValue (st.curTs N) (st.curValue N)
  cur_rmw_ts :
    forall N, st.curRmw N -> st.tsRmw (st.curTs N)
  cur_non_rmw_ts :
    forall N, Not (st.curRmw N) -> Not (st.tsRmw (st.curTs N))
  pending_below_cur :
    forall N, st.pending N -> ord.le (st.pendingTs N) (st.curTs N)
  pending_rmw_ts :
    forall N, st.pending N -> st.pendingRmw N -> st.tsRmw (st.pendingTs N)
  pending_non_rmw_ts :
    forall N, st.pending N -> Not (st.pendingRmw N) -> Not (st.tsRmw (st.pendingTs N))
  pending_rmw_current :
    forall N, st.pending N -> st.pendingRmw N -> st.pendingTs N = st.curTs N
  pending_acked_advanced :
    forall N A, st.pending N -> st.acked N A -> ord.le (st.pendingTs N) (st.curTs A)
  ready_pending :
    forall N, st.ready N -> st.pending N
  ready_live_acked :
    forall N A, st.ready N -> st.live A -> st.acked N A
  ready_rmw_no_completed_conflict :
    forall N B R, st.live N -> st.ready N -> st.pendingRmw N ->
      st.parent (st.pendingTs N) B -> st.completed R -> st.tsRmw R -> st.parent R B ->
      st.pendingTs N = R
  completed_live_advanced :
    forall T N, st.completed T -> st.live N -> ord.le T (st.curTs N)
  valid_completed :
    forall N, st.state N = HState.hs_valid -> st.completed (st.curTs N)
  write_rmw_spacing :
    forall R W B, st.parent R B -> st.parent W B -> st.tsRmw R -> Not (st.tsRmw W) ->
      lt ord R W
  completed_rmw_same_base :
    forall R1 R2 B, st.completed R1 -> st.completed R2 ->
      st.tsRmw R1 -> st.tsRmw R2 -> st.parent R1 B -> st.parent R2 B -> R1 = R2
  ready_live :
    forall N, st.ready N -> st.live N
  no_complete_try :
    forall N, Not (st.completeTry N)
  no_o3_try :
    forall N, Not (st.o3Try N)

theorem core_of_safety
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch st) :
    CoreInvariant ord initTs initValue initEpoch st where
  ts_value_functional := h.ts_value_functional
  parent_seen := h.parent_seen
  ts_value_seen := h.ts_value_seen
  ts_rmw_seen := h.ts_rmw_seen
  rmw_conflict_seen := h.rmw_conflict_seen
  rmw_conflict_rmw := h.rmw_conflict_rmw
  inv_write_wf := h.inv_write_wf
  inv_rmw_wf := h.inv_rmw_wf
  ack_msg_advanced := h.ack_msg_advanced
  val_msg_completed := h.val_msg_completed
  o3_quorum_live_ack := h.o3_quorum_live_ack
  cur_value_seen := h.cur_value_seen
  cur_rmw_ts := h.cur_rmw_ts
  cur_non_rmw_ts := h.cur_non_rmw_ts
  pending_below_cur := h.pending_below_cur
  pending_rmw_ts := h.pending_rmw_ts
  pending_non_rmw_ts := h.pending_non_rmw_ts
  pending_rmw_current := h.pending_rmw_current
  pending_acked_advanced := h.pending_acked_advanced
  ready_pending := h.ready_pending
  ready_live_acked := h.ready_live_acked
  ready_rmw_no_completed_conflict := h.ready_rmw_no_completed_conflict
  completed_live_advanced := h.completed_live_advanced
  valid_completed := h.valid_completed
  write_rmw_spacing := h.write_rmw_spacing
  completed_rmw_same_base := h.completed_rmw_same_base
  ready_live := h.ready_live
  no_complete_try := h.no_complete_try
  no_o3_try := h.no_o3_try

theorem core_init_invariant
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    (hinit : InitAssumptions ord initTs) :
    CoreInvariant ord initTs initValue initEpoch
      (initState (Node := Node) (TS := TS) (Value := Value) (Epoch := Epoch)
        initTs initValue initEpoch) :=
  core_of_safety (init_safety hinit)

structure AgreementInvariant
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    (ord : TotalOrder TS) (initTs : TS) (initValue : Value) (initEpoch : Epoch)
    (st : State Node TS Value Epoch) : Prop where
  ts_value_functional :
    forall T V1 V2, st.tsValue T V1 -> st.tsValue T V2 -> V1 = V2
  ts_value_seen :
    forall T V, st.tsValue T V -> st.seenTs T
  ts_rmw_seen :
    forall T, st.tsRmw T -> st.seenTs T
  rmw_conflict_seen :
    forall T, st.rmwConflict T -> st.seenTs T
  rmw_conflict_rmw :
    forall T, st.rmwConflict T -> st.tsRmw T
  inv_write_wf :
    forall S T V, st.invWrite S T V -> st.seenTs T /\ st.tsValue T V /\ Not (st.tsRmw T)
  inv_rmw_wf :
    forall S T V, st.invRmw S T V -> st.seenTs T /\ st.tsValue T V /\ st.tsRmw T
  ack_msg_seen :
    forall A C T, st.ackMsg A C T -> st.seenTs T
  ack_msg_advanced :
    forall A C T, st.ackMsg A C T -> ord.le T (st.curTs A)
  val_msg_completed :
    forall T, st.valMsg T -> st.completed T
  completed_seen :
    forall T, st.completed T -> st.seenTs T
  o3_quorum_live_ack :
    forall N C T A, st.o3Quorum N C T -> st.live A -> st.ackMsg A C T
  o3_quorum_seen :
    forall N C T, st.o3Quorum N C T -> st.seenTs T
  cur_value_seen :
    forall N, st.tsValue (st.curTs N) (st.curValue N)
  cur_rmw_ts :
    forall N, st.curRmw N -> st.tsRmw (st.curTs N)
  cur_non_rmw_ts :
    forall N, Not (st.curRmw N) -> Not (st.tsRmw (st.curTs N))
  pending_below_cur :
    forall N, st.pending N -> ord.le (st.pendingTs N) (st.curTs N)
  pending_acked_advanced :
    forall N A, st.pending N -> st.acked N A -> ord.le (st.pendingTs N) (st.curTs A)
  ready_pending :
    forall N, st.ready N -> st.pending N
  ready_live_acked :
    forall N A, st.ready N -> st.live A -> st.acked N A
  completed_live_advanced :
    forall T N, st.completed T -> st.live N -> ord.le T (st.curTs N)
  valid_completed :
    forall N, st.state N = HState.hs_valid -> st.completed (st.curTs N)

theorem agreement_of_safety
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch st) :
    AgreementInvariant ord initTs initValue initEpoch st where
  ts_value_functional := h.ts_value_functional
  ts_value_seen := h.ts_value_seen
  ts_rmw_seen := h.ts_rmw_seen
  rmw_conflict_seen := h.rmw_conflict_seen
  rmw_conflict_rmw := h.rmw_conflict_rmw
  inv_write_wf := h.inv_write_wf
  inv_rmw_wf := h.inv_rmw_wf
  ack_msg_seen := h.ack_msg_seen
  ack_msg_advanced := h.ack_msg_advanced
  val_msg_completed := h.val_msg_completed
  completed_seen := h.completed_seen
  o3_quorum_live_ack := h.o3_quorum_live_ack
  o3_quorum_seen := h.o3_quorum_seen
  cur_value_seen := h.cur_value_seen
  cur_rmw_ts := h.cur_rmw_ts
  cur_non_rmw_ts := h.cur_non_rmw_ts
  pending_below_cur := h.pending_below_cur
  pending_acked_advanced := h.pending_acked_advanced
  ready_pending := h.ready_pending
  ready_live_acked := h.ready_live_acked
  completed_live_advanced := h.completed_live_advanced
  valid_completed := h.valid_completed

theorem agreement_init_invariant
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    (hinit : InitAssumptions ord initTs) :
    AgreementInvariant ord initTs initValue initEpoch
      (initState (Node := Node) (TS := TS) (Value := Value) (Epoch := Epoch)
        initTs initValue initEpoch) :=
  agreement_of_safety (init_safety hinit)

set_option maxHeartbeats 80000 in
theorem fail_preserves_agreement
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch} {n : Node}
    (hInv : AgreementInvariant ord initTs initValue initEpoch st)
    (hLive : st.live n) :
    AgreementInvariant ord initTs initValue initEpoch (failPost st n) := by
  rcases hInv with
    ⟨hTsValueFunctional, hTsValueSeen, hTsRmwSeen,
      hRmwConflictSeen, hRmwConflictRmw, hInvWriteWf, hInvRmwWf,
      hAckMsgSeen, hAckMsgAdvanced, hValMsgCompleted, hCompletedSeen,
      hO3QuorumLiveAck, hO3QuorumSeen, hCurValueSeen, hCurRmwTs,
      hCurNonRmwTs, hPendingBelowCur, hPendingAckedAdvanced,
      hReadyPending, hReadyLiveAcked, hCompletedLiveAdvanced,
      hValidCompleted⟩
  refine {
    ts_value_functional := hTsValueFunctional
    ts_value_seen := hTsValueSeen
    ts_rmw_seen := hTsRmwSeen
    rmw_conflict_seen := hRmwConflictSeen
    rmw_conflict_rmw := hRmwConflictRmw
    inv_write_wf := hInvWriteWf
    inv_rmw_wf := hInvRmwWf
    ack_msg_seen := hAckMsgSeen
    ack_msg_advanced := hAckMsgAdvanced
    val_msg_completed := hValMsgCompleted
    completed_seen := hCompletedSeen
    o3_quorum_live_ack := by
      intro N C T A hq hPostLive
      have hOldLive : st.live A := by
        have hBoth : A ≠ n /\ st.live A := by
          simpa [failPost, set1] using hPostLive
        exact hBoth.2
      exact hO3QuorumLiveAck N C T A hq hOldLive
    o3_quorum_seen := hO3QuorumSeen
    cur_value_seen := hCurValueSeen
    cur_rmw_ts := hCurRmwTs
    cur_non_rmw_ts := hCurNonRmwTs
    pending_below_cur := by
      intro N hPostPending
      by_cases hN : N = n
      · simp [failPost, set1, hN] at hPostPending
      · have hOldPending : st.pending N := by
          simpa [failPost, set1, hN] using hPostPending
        exact hPendingBelowCur N hOldPending
    pending_acked_advanced := by
      intro N A hPostPending hPostAcked
      by_cases hN : N = n
      · simp [failPost, set1, hN] at hPostPending
      · have hOldPending : st.pending N := by
          simpa [failPost, set1, hN] using hPostPending
        have hOldAcked : st.acked N A := by
          simpa [failPost, clear2First, hN] using hPostAcked
        exact hPendingAckedAdvanced N A hOldPending hOldAcked
    ready_pending := by
      intro N hPostReady
      by_cases hN : N = n
      · simp [failPost, set1, hN] at hPostReady
      · have hOldReady : st.ready N := by
          simpa [failPost, set1, hN] using hPostReady
        have hOldPending : st.pending N := hReadyPending N hOldReady
        simpa [failPost, set1, hN] using hOldPending
    ready_live_acked := by
      intro N A hPostReady hPostLive
      by_cases hN : N = n
      · simp [failPost, set1, hN] at hPostReady
      · have hOldReady : st.ready N := by
          simpa [failPost, set1, hN] using hPostReady
        have hOldLive : st.live A := by
          have hBoth : A ≠ n /\ st.live A := by
            simpa [failPost, set1] using hPostLive
          exact hBoth.2
        have hOldAcked : st.acked N A :=
          hReadyLiveAcked N A hOldReady hOldLive
        simpa [failPost, clear2First, hN] using hOldAcked
    completed_live_advanced := by
      intro T N hCompleted hPostLive
      have hOldLive : st.live N := by
        have hBoth : N ≠ n /\ st.live N := by
          simpa [failPost, set1] using hPostLive
        exact hBoth.2
      exact hCompletedLiveAdvanced T N hCompleted hOldLive
    valid_completed := hValidCompleted
  }

theorem o3_observe_quorum_preserves_agreement
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch} {n c : Node} {t : TS}
    (hInv : AgreementInvariant ord initTs initValue initEpoch st)
    (hLive : st.live n)
    (hAllAcked : forall A, st.live A -> st.ackMsg A c t) :
    AgreementInvariant ord initTs initValue initEpoch (o3ObserveQuorumPost st n c t) := by
  rcases hInv with
    ⟨hTsValueFunctional, hTsValueSeen, hTsRmwSeen,
      hRmwConflictSeen, hRmwConflictRmw, hInvWriteWf, hInvRmwWf,
      hAckMsgSeen, hAckMsgAdvanced, hValMsgCompleted, hCompletedSeen,
      hO3QuorumLiveAck, hO3QuorumSeen, hCurValueSeen, hCurRmwTs,
      hCurNonRmwTs, hPendingBelowCur, hPendingAckedAdvanced,
      hReadyPending, hReadyLiveAcked, hCompletedLiveAdvanced,
      hValidCompleted⟩
  refine {
    ts_value_functional := hTsValueFunctional
    ts_value_seen := hTsValueSeen
    ts_rmw_seen := hTsRmwSeen
    rmw_conflict_seen := hRmwConflictSeen
    rmw_conflict_rmw := hRmwConflictRmw
    inv_write_wf := hInvWriteWf
    inv_rmw_wf := hInvRmwWf
    ack_msg_seen := hAckMsgSeen
    ack_msg_advanced := hAckMsgAdvanced
    val_msg_completed := hValMsgCompleted
    completed_seen := hCompletedSeen
    o3_quorum_live_ack := by
      intro N C T A hq hLiveA
      rcases hq with hOld | hNew
      · exact hO3QuorumLiveAck N C T A hOld hLiveA
      · rcases hNew with ⟨rfl, rfl, rfl⟩
        exact hAllAcked A hLiveA
    o3_quorum_seen := by
      intro N C T hq
      rcases hq with hOld | hNew
      · exact hO3QuorumSeen N C T hOld
      · have hSeenNew : st.seenTs t :=
          hAckMsgSeen n c t (hAllAcked n hLive)
        rcases hNew with ⟨rfl, rfl, rfl⟩
        exact hSeenNew
    cur_value_seen := hCurValueSeen
    cur_rmw_ts := hCurRmwTs
    cur_non_rmw_ts := hCurNonRmwTs
    pending_below_cur := hPendingBelowCur
    pending_acked_advanced := hPendingAckedAdvanced
    ready_pending := hReadyPending
    ready_live_acked := hReadyLiveAcked
    completed_live_advanced := hCompletedLiveAdvanced
    valid_completed := hValidCompleted
  }

set_option maxHeartbeats 200000 in
theorem local_write_preserves_core
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch} {n : Node} {t : TS} {v : Value}
    (hCore : CoreInvariant ord initTs initValue initEpoch st)
    (hLive : st.live n)
    (hNotPending : Not (st.pending n))
    (hState : st.state n = HState.hs_valid \/ st.state n = HState.hs_invalid)
    (hLt : lt ord (st.curTs n) t)
    (hFresh : Not (st.seenTs t))
    (hSpacing : forall R, st.parent R (st.curTs n) -> st.tsRmw R -> lt ord R t) :
    CoreInvariant ord initTs initValue initEpoch (localWritePost st n t v) := by
  rcases hCore with
    ⟨hTsValueFunctional, hParentSeen, hTsValueSeen, hTsRmwSeen,
      hRmwConflictSeen, hRmwConflictRmw, hInvWriteWf, hInvRmwWf,
      hAckMsgAdvanced, hValMsgCompleted, hO3QuorumLiveAck,
      hCurValueSeen, hCurRmwTs, hCurNonRmwTs, hPendingBelowCur,
      hPendingRmwTs, hPendingNonRmwTs, hPendingRmwCurrent,
      hPendingAckedAdvanced, hReadyPending, hReadyLiveAcked,
      hReadyRmwNoCompletedConflict, hCompletedLiveAdvanced,
      hValidCompleted, hWriteRmwSpacing, hCompletedRmwSameBase,
      hReadyLive, hNoCompleteTry, hNoO3Try⟩
  refine {
    ts_value_functional := by
      intro T V1 V2 hv1 hv2
      simp [localWritePost, addTsValue] at hv1 hv2
      rcases hv1 with hv1 | hNew1
      · rcases hv2 with hv2 | hNew2
        · exact hTsValueFunctional T V1 V2 hv1 hv2
        · exact False.elim (hFresh (by
            simpa [hNew2.1] using hTsValueSeen T V1 hv1))
      · rcases hv2 with hv2 | hNew2
        · exact False.elim (hFresh (by
            simpa [hNew1.1] using hTsValueSeen T V2 hv2))
        · exact hNew1.2.trans hNew2.2.symm
    parent_seen := by
      intro T B hp
      simp [localWritePost, addParent] at hp
      rcases hp with hp | ⟨rfl, rfl⟩
      · rcases hParentSeen T B hp with ⟨hSeenT, hSeenB, hLtBT⟩
        exact ⟨Or.inl hSeenT, Or.inl hSeenB, hLtBT⟩
      · have hSeenCur : st.seenTs (st.curTs n) :=
          hTsValueSeen (st.curTs n) (st.curValue n) (hCurValueSeen n)
        exact ⟨Or.inr rfl, Or.inl hSeenCur, hLt⟩
    ts_value_seen := by
      intro T V hv
      simp [localWritePost, addTsValue] at hv ⊢
      rcases hv with hv | hNew
      · exact Or.inl (hTsValueSeen T V hv)
      · exact Or.inr hNew.1
    ts_rmw_seen := by
      intro T hr
      simp [localWritePost, removeTs] at hr ⊢
      exact Or.inl (hTsRmwSeen T hr.1)
    rmw_conflict_seen := by
      intro T hc
      simp [localWritePost, removeTs] at hc ⊢
      exact Or.inl (hRmwConflictSeen T hc.1)
    rmw_conflict_rmw := by
      intro T hc
      simp [localWritePost, removeTs] at hc ⊢
      exact ⟨hRmwConflictRmw T hc.1, hc.2⟩
    inv_write_wf := by
      intro S T V hi
      simp [localWritePost, add3, add1, addTsValue, removeTs] at hi ⊢
      rcases hi with hi | hNew
      · rcases hInvWriteWf S T V hi with ⟨hSeen, hVal, hNotRmw⟩
        exact ⟨Or.inl hSeen, Or.inl hVal, by
          intro hRmw
          exact False.elim (hNotRmw hRmw)⟩
      · exact ⟨Or.inr hNew.2.1, Or.inr ⟨hNew.2.1, hNew.2.2⟩, by
          intro hRmw
          exact hNew.2.1⟩
    inv_rmw_wf := by
      intro S T V hi
      simp [localWritePost, add1, addTsValue, removeTs] at hi ⊢
      rcases hInvRmwWf S T V hi with ⟨hSeen, hVal, hRmw⟩
      have hNe : T ≠ t := by
        intro hEq
        subst hEq
        exact hFresh hSeen
      exact ⟨Or.inl hSeen, Or.inl hVal, hRmw, hNe⟩
    ack_msg_advanced := by
      intro A C T ha
      by_cases hA : A = n
      · have hAdvance : ord.le (st.curTs A) t := by
          simpa [hA] using hLt.1
        simpa [localWritePost, upd, hA] using
          ord.trans (hAckMsgAdvanced A C T ha) hAdvance
      · simpa [localWritePost, upd, hA] using hAckMsgAdvanced A C T ha
    val_msg_completed := by
      intro T hv
      exact hValMsgCompleted T hv
    o3_quorum_live_ack := by
      intro N C T A hq hl
      exact hO3QuorumLiveAck N C T A hq hl
    cur_value_seen := by
      intro N
      by_cases hN : N = n
      · simp [localWritePost, upd, addTsValue, hN]
      · simp [localWritePost, upd, addTsValue, hN]
        exact Or.inl (hCurValueSeen N)
    cur_rmw_ts := by
      intro N hRmw
      by_cases hN : N = n
      · simp [localWritePost, upd, set1, removeTs, hN] at hRmw
      · simp [localWritePost, upd, set1, removeTs, hN] at hRmw ⊢
        have oldRmw : st.tsRmw (st.curTs N) := hCurRmwTs N hRmw
        have hNe : st.curTs N ≠ t := by
          intro hEq
          exact hFresh (by simpa [hEq] using hTsRmwSeen (st.curTs N) oldRmw)
        exact ⟨oldRmw, hNe⟩
    cur_non_rmw_ts := by
      intro N hNotCur hRmw
      by_cases hN : N = n
      · simp [localWritePost, upd, set1, removeTs, hN] at hNotCur hRmw
      · simp [localWritePost, upd, set1, removeTs, hN] at hNotCur hRmw
        exact hCurNonRmwTs N hNotCur hRmw.1
    pending_below_cur := by
      intro N hp
      by_cases hN : N = n
      · simp [localWritePost, upd, set1, hN]
        exact ord.refl t
      · simp [localWritePost, upd, set1, hN] at hp ⊢
        exact hPendingBelowCur N hp
    pending_rmw_ts := by
      intro N hp hr
      by_cases hN : N = n
      · simp [localWritePost, set1, upd, removeTs, hN] at hr
      · simp [localWritePost, set1, upd, removeTs, hN] at hp hr ⊢
        have oldRmw : st.tsRmw (st.pendingTs N) :=
          hPendingRmwTs N hp hr
        have hNe : st.pendingTs N ≠ t := by
          intro hEq
          exact hFresh (by simpa [hEq] using hTsRmwSeen (st.pendingTs N) oldRmw)
        exact ⟨oldRmw, hNe⟩
    pending_non_rmw_ts := by
      intro N hp hNotRmw hRmw
      by_cases hN : N = n
      · simp [localWritePost, set1, upd, removeTs, hN] at hRmw
      · simp [localWritePost, set1, upd, removeTs, hN] at hp hNotRmw hRmw
        exact hPendingNonRmwTs N hp hNotRmw hRmw.1
    pending_rmw_current := by
      intro N hp hr
      by_cases hN : N = n
      · simp [localWritePost, set1, upd, hN] at hr
      · simp [localWritePost, set1, upd, hN] at hp hr ⊢
        exact hPendingRmwCurrent N hp hr
    pending_acked_advanced := by
      intro N A hp ha
      by_cases hN : N = n
      · have hA : A = n := by
          simpa [localWritePost, set1, set2FirstSelf, hN] using ha
        simp [localWritePost, set1, set2FirstSelf, upd, hN, hA]
        exact ord.refl t
      · by_cases hA : A = n
        · simp [localWritePost, set1, set2FirstSelf, upd, hN, hA] at hp ha ⊢
          exact ord.trans (hPendingAckedAdvanced N n hp (by simpa [hA] using ha)) hLt.1
        · simp [localWritePost, set1, set2FirstSelf, upd, hN, hA] at hp ha ⊢
          exact hPendingAckedAdvanced N A hp ha
    ready_pending := by
      intro N hr
      by_cases hN : N = n
      · simp [localWritePost, set1, hN] at hr
      · simp [localWritePost, set1, hN] at hr ⊢
        exact hReadyPending N hr
    ready_live_acked := by
      intro N A hr hl
      by_cases hN : N = n
      · simp [localWritePost, set1, hN] at hr
      · simp [localWritePost, set1, set2FirstSelf, hN] at hr ⊢
        exact hReadyLiveAcked N A hr hl
    ready_rmw_no_completed_conflict := by
      intro N B R hl hr hpRmw hParentPending hCompleted hRmwR hParentR
      by_cases hN : N = n
      · simp [localWritePost, set1, upd, addParent, removeTs, hN] at hr
      · simp [localWritePost, set1, upd, addParent, removeTs, hN] at hr hpRmw hParentPending hRmwR hParentR ⊢
        have oldReady : st.ready N := hr
        have oldPendingRmw : st.pendingRmw N := hpRmw
        have oldPending : st.pending N := hReadyPending N oldReady
        have oldPendingTsSeen : st.seenTs (st.pendingTs N) :=
          hTsRmwSeen (st.pendingTs N) (hPendingRmwTs N oldPending oldPendingRmw)
        have pendingParentOld : st.parent (st.pendingTs N) B := by
          rcases hParentPending with hp | ⟨hEq, _hb⟩
          · exact hp
          · exact False.elim (hFresh (by simpa [hEq] using oldPendingTsSeen))
        have rmwROld : st.tsRmw R := hRmwR.1
        have parentROld : st.parent R B := by
          rcases hParentR with hp | hNew
          · exact hp
          · exact False.elim (hRmwR.2 hNew.1)
        exact hReadyRmwNoCompletedConflict N B R hl oldReady oldPendingRmw
          pendingParentOld hCompleted rmwROld parentROld
    completed_live_advanced := by
      intro T N hCompleted hl
      by_cases hN : N = n
      · have hAdvance : ord.le (st.curTs N) t := by
          simpa [hN] using hLt.1
        simpa [localWritePost, upd, hN] using
          ord.trans (hCompletedLiveAdvanced T N hCompleted hl) hAdvance
      · simpa [localWritePost, upd, hN] using hCompletedLiveAdvanced T N hCompleted hl
    valid_completed := by
      intro N hValid
      by_cases hN : N = n
      · simp [localWritePost, upd, hN] at hValid
      · simp [localWritePost, upd, hN] at hValid ⊢
        exact hValidCompleted N hValid
    write_rmw_spacing := by
      intro R W B hParentR hParentW hRmwR hNotRmwW
      have hParentROldOrNew :
          st.parent R B \/ (R = t /\ B = st.curTs n) := by
        simpa [localWritePost, addParent] using hParentR
      have hParentWOldOrNew :
          st.parent W B \/ (W = t /\ B = st.curTs n) := by
        simpa [localWritePost, addParent] using hParentW
      have hRmwROld : st.tsRmw R /\ R ≠ t := by
        simpa [localWritePost, removeTs] using hRmwR
      have hNotOldW : ¬st.tsRmw W := by
        intro hOldW
        have hEqW : W = t := by
          have hNotPost : ¬(st.tsRmw W /\ W ≠ t) := by
            simpa [localWritePost, removeTs] using hNotRmwW
          by_cases hWt : W = t
          · exact hWt
          · exact False.elim (hNotPost ⟨hOldW, hWt⟩)
        exact hFresh (by simpa [hEqW] using hTsRmwSeen W hOldW)
      rcases hParentROldOrNew with hParentROld | hParentRNew
      · rcases hParentWOldOrNew with hParentWOld | hParentWNew
        · exact hWriteRmwSpacing R W B hParentROld hParentWOld hRmwROld.1 hNotOldW
        · have hParentRBase : st.parent R (st.curTs n) := by
            simpa [hParentWNew.2] using hParentROld
          simpa [hParentWNew.1] using hSpacing R hParentRBase hRmwROld.1
      · exact False.elim (hRmwROld.2 hParentRNew.1)
    completed_rmw_same_base := by
      intro R1 R2 B hCompleted1 hCompleted2 hRmw1 hRmw2 hParent1 hParent2
      have hRmw1Old : st.tsRmw R1 /\ R1 ≠ t := by
        simpa [localWritePost, removeTs] using hRmw1
      have hRmw2Old : st.tsRmw R2 /\ R2 ≠ t := by
        simpa [localWritePost, removeTs] using hRmw2
      have hParent1OldOrNew : st.parent R1 B \/ (R1 = t /\ B = st.curTs n) := by
        simpa [localWritePost, addParent] using hParent1
      have hParent2OldOrNew : st.parent R2 B \/ (R2 = t /\ B = st.curTs n) := by
        simpa [localWritePost, addParent] using hParent2
      have parent1Old : st.parent R1 B := by
        rcases hParent1OldOrNew with hp | hNew
        · exact hp
        · exact False.elim (hRmw1Old.2 hNew.1)
      have parent2Old : st.parent R2 B := by
        rcases hParent2OldOrNew with hp | hNew
        · exact hp
        · exact False.elim (hRmw2Old.2 hNew.1)
      exact hCompletedRmwSameBase R1 R2 B hCompleted1 hCompleted2
        hRmw1Old.1 hRmw2Old.1 parent1Old parent2Old
    ready_live := by
      intro N hr
      by_cases hN : N = n
      · simp [localWritePost, set1, hN] at hr
      · simp [localWritePost, set1, hN] at hr
        exact hReadyLive N hr
    no_complete_try := by
      intro N hTry
      exact hNoCompleteTry N hTry
    no_o3_try := by
      intro N hTry
      exact hNoO3Try N hTry
  }

end Operational

/-!
## TLA/paper-aligned reference model

This namespace is deliberately separate from the Ivy-shaped model above.  It
uses the state variables and transition names from `Hermes.tla` and
`HermesRMWs.tla`, with the paper's O3 broadcast-ACK optimization added as an
extra validation path.  In particular:

* ACK and RINV messages carry the current membership epoch.
* `nodeFailurePost` increments the epoch and resets every ACK set.
* `rmwReplayPost` resets gathered ACKs, while `writeReplayPost` preserves them.
* Stale RMW invalidations send the receiver's local invalidation back and do
  not create an ACK for the stale timestamp.
* O3 quorums range over live non-coordinator nodes; the coordinator's
  contribution is implicit and no self-ACK is required.
-/

namespace Reference

universe uRefNode uRefValue

inductive OpKind where
  | write
  | rmw
deriving DecidableEq, Repr

structure Timestamp (Node : Type uRefNode) where
  version : Nat
  tieBreaker : Node
deriving DecidableEq, Repr

structure NodeRank (Node : Type uRefNode) where
  rank : Node -> Nat
  rank_injective : forall {a b : Node}, rank a = rank b -> a = b

def tsLe {Node : Type uRefNode} (rank : NodeRank Node)
    (a b : Timestamp Node) : Prop :=
  a.version < b.version \/
    (a.version = b.version /\ rank.rank a.tieBreaker <= rank.rank b.tieBreaker)

def tsLt {Node : Type uRefNode} (rank : NodeRank Node)
    (a b : Timestamp Node) : Prop :=
  tsLe rank a b /\ a ≠ b

instance instDecidableTsLe {Node : Type uRefNode} [DecidableEq Node]
    (rank : NodeRank Node) (a b : Timestamp Node) :
    Decidable (tsLe rank a b) := by
  unfold tsLe
  infer_instance

instance instDecidableTsLt {Node : Type uRefNode} [DecidableEq Node]
    (rank : NodeRank Node) (a b : Timestamp Node) :
    Decidable (tsLt rank a b) := by
  unfold tsLt
  infer_instance

theorem tsLe_refl {Node : Type uRefNode} (rank : NodeRank Node)
    (a : Timestamp Node) :
    tsLe rank a a := by
  right
  exact ⟨rfl, Nat.le_refl _⟩

theorem tsLe_antisymm {Node : Type uRefNode} (rank : NodeRank Node)
    {a b : Timestamp Node} :
    tsLe rank a b -> tsLe rank b a -> a = b := by
  intro hab hba
  rcases a with ⟨av, atie⟩
  rcases b with ⟨bv, btie⟩
  unfold tsLe at hab hba
  simp at hab hba
  rcases hab with havb | ⟨habv, habtie⟩
  · rcases hba with hbav | ⟨hbav, _hbtie⟩
    · omega
    · omega
  · rcases hba with hbav | ⟨hbav, hbatie⟩
    · omega
    · have hRank : rank.rank atie = rank.rank btie :=
        Nat.le_antisymm habtie hbatie
      have hTie : atie = btie := rank.rank_injective hRank
      subst hbav
      subst hTie
      rfl

theorem tsLe_trans {Node : Type uRefNode} (rank : NodeRank Node)
    {a b c : Timestamp Node} :
    tsLe rank a b -> tsLe rank b c -> tsLe rank a c := by
  intro hab hbc
  rcases a with ⟨av, atie⟩
  rcases b with ⟨bv, btie⟩
  rcases c with ⟨cv, ctie⟩
  unfold tsLe at hab hbc ⊢
  simp at hab hbc ⊢
  rcases hab with havb | ⟨habv, habtie⟩
  · rcases hbc with hbvc | ⟨hbcv, hbctie⟩
    · exact Or.inl (by omega)
    · exact Or.inl (by omega)
  · rcases hbc with hbvc | ⟨hbcv, hbctie⟩
    · exact Or.inl (by omega)
    · exact Or.inr ⟨by omega, Nat.le_trans habtie hbctie⟩

theorem tsLt_irrefl {Node : Type uRefNode} (rank : NodeRank Node)
    (a : Timestamp Node) :
    Not (tsLt rank a a) := by
  intro h
  exact h.2 rfl

def tsOf {Node : Type uRefNode} (version : Nat) (tie : Node) :
    Timestamp Node where
  version := version
  tieBreaker := tie

structure RInvMsg (Node : Type uRefNode) (Value : Type uRefValue) where
  sender : Node
  epochID : Nat
  ts : Timestamp Node
  value : Value
  parent : Timestamp Node
  kind : OpKind
deriving Repr

structure AckMsg (Node : Type uRefNode) where
  sender : Node
  epochID : Nat
  ts : Timestamp Node
deriving Repr

structure ValMsg (Node : Type uRefNode) where
  ts : Timestamp Node
deriving Repr

structure RefState (Node : Type uRefNode) (Value : Type uRefValue) where
  epochID : Nat
  live : Node -> Prop
  nodeTS : Node -> Timestamp Node
  nodeValue : Node -> Value
  nodeState : Node -> HState
  nodeRcvedAcks : Node -> Node -> Prop
  nodeLastWriter : Node -> Node
  nodeLastWriteTS : Node -> Timestamp Node
  nodeWriteEpochID : Node -> Nat
  nodeFlagRMW : Node -> Bool
  parentOf : Timestamp Node -> Timestamp Node
  tsKind : Timestamp Node -> OpKind -> Prop
  tsValue : Timestamp Node -> Value -> Prop
  rmsgs : RInvMsg Node Value -> Prop
  ackMsgs : AckMsg Node -> Prop
  valMsgs : ValMsg Node -> Prop
  committedRMWs : Timestamp Node -> Prop
  committedWrites : Timestamp Node -> Prop
  o3Quorum : Node -> Node -> Timestamp Node -> Nat -> Prop

def replace {α : Sort uRefNode} {β : Sort uRefValue}
    [DecidableEq α] (f : α -> β) (x : α) (v : β) : α -> β :=
  fun y => if y = x then v else f y

def replaceRelFirst {α : Sort uRefNode} {β : Sort uRefValue}
    [DecidableEq α] (r : α -> β -> Prop) (x : α) (next : β -> Prop) :
    α -> β -> Prop :=
  fun y z => if y = x then next z else r y z

def removeLive {Node : Type uRefNode} [DecidableEq Node]
    (live : Node -> Prop) (n : Node) : Node -> Prop :=
  fun a => live a /\ a ≠ n

def addRInv {Node : Type uRefNode} {Value : Type uRefValue}
    (r : RInvMsg Node Value -> Prop) (m : RInvMsg Node Value) :
    RInvMsg Node Value -> Prop :=
  fun x => r x \/ x = m

def addAck {Node : Type uRefNode}
    (r : AckMsg Node -> Prop) (m : AckMsg Node) : AckMsg Node -> Prop :=
  fun x => r x \/ x = m

def addVal {Node : Type uRefNode}
    (r : ValMsg Node -> Prop) (m : ValMsg Node) : ValMsg Node -> Prop :=
  fun x => r x \/ x = m

def addTsKind {Node : Type uRefNode}
    (r : Timestamp Node -> OpKind -> Prop) (t : Timestamp Node) (k : OpKind) :
    Timestamp Node -> OpKind -> Prop :=
  fun t' k' => r t' k' \/ (t' = t /\ k' = k)

def addTsValue {Node : Type uRefNode} {Value : Type uRefValue}
    (r : Timestamp Node -> Value -> Prop) (t : Timestamp Node) (v : Value) :
    Timestamp Node -> Value -> Prop :=
  fun t' v' => r t' v' \/ (t' = t /\ v' = v)

def addCommitted
    {Node : Type uRefNode}
    (r : Timestamp Node -> Prop) (t : Timestamp Node) : Timestamp Node -> Prop :=
  fun t' => r t' \/ t' = t

def addO3
    {Node : Type uRefNode}
    (r : Node -> Node -> Timestamp Node -> Nat -> Prop)
    (n c : Node) (t : Timestamp Node) (e : Nat) :
    Node -> Node -> Timestamp Node -> Nat -> Prop :=
  fun n' c' t' e' => r n' c' t' e' \/ (n' = n /\ c' = c /\ t' = t /\ e' = e)

def receivedAllAcks {Node : Type uRefNode} {Value : Type uRefValue}
    (st : RefState Node Value) (n : Node) : Prop :=
  forall a, st.live a -> a ≠ n -> st.nodeRcvedAcks n a

def o3AckQuorum {Node : Type uRefNode} {Value : Type uRefValue}
    (rank : NodeRank Node) (st : RefState Node Value)
    (coordinator : Node) (t : Timestamp Node) : Prop :=
  st.live coordinator /\
    tsLe rank t (st.nodeTS coordinator) /\
    forall a, st.live a -> a ≠ coordinator ->
      st.ackMsgs { sender := a, epochID := st.epochID, ts := t }

def invalidStateAfterGreater {Node : Type uRefNode} {Value : Type uRefValue}
    (st : RefState Node Value) (n : Node) : HState :=
  if st.nodeState n = HState.hs_valid \/
      st.nodeState n = HState.hs_invalid \/
      st.nodeState n = HState.hs_replay then
    HState.hs_invalid
  else if (st.nodeState n = HState.hs_write \/
      st.nodeState n = HState.hs_invalid_write) /\ st.nodeFlagRMW n = false then
    HState.hs_invalid_write
  else
    HState.hs_invalid

def sendRInvMsg {Node : Type uRefNode} {Value : Type uRefValue}
    (st : RefState Node Value) (n : Node) (t : Timestamp Node) (v : Value)
    (parent : Timestamp Node) (kind : OpKind) : RInvMsg Node Value where
  sender := n
  epochID := st.epochID
  ts := t
  value := v
  parent := parent
  kind := kind

def sendAckMsg {Node : Type uRefNode}
    (stEpoch : Nat) (n : Node) (t : Timestamp Node) : AckMsg Node where
  sender := n
  epochID := stEpoch
  ts := t

def startUpdatePost
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (st : RefState Node Value) (n : Node) (t : Timestamp Node) (v : Value)
    (kind : OpKind) : RefState Node Value :=
  let parent := st.nodeTS n
  { st with
    nodeTS := replace st.nodeTS n t
    nodeValue := replace st.nodeValue n v
    nodeState := replace st.nodeState n HState.hs_write
    nodeRcvedAcks := replaceRelFirst st.nodeRcvedAcks n (fun _ => False)
    nodeLastWriter := replace st.nodeLastWriter n n
    nodeLastWriteTS := replace st.nodeLastWriteTS n t
    nodeWriteEpochID := replace st.nodeWriteEpochID n st.epochID
    nodeFlagRMW := replace st.nodeFlagRMW n (kind = OpKind.rmw)
    parentOf := replace st.parentOf t parent
    tsKind := addTsKind st.tsKind t kind
    tsValue := addTsValue st.tsValue t v
    rmsgs := addRInv st.rmsgs (sendRInvMsg st n t v parent kind) }

def writePost
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (st : RefState Node Value) (n : Node) (v : Value) : RefState Node Value :=
  startUpdatePost st n (tsOf (st.nodeTS n).version.succ.succ n) v OpKind.write

def rmwPost
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (st : RefState Node Value) (n : Node) (v : Value) : RefState Node Value :=
  startUpdatePost st n (tsOf (st.nodeTS n).version.succ n) v OpKind.rmw

def writeReplayPost
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (st : RefState Node Value) (n : Node) : RefState Node Value :=
  { st with
    nodeState := replace st.nodeState n HState.hs_replay
    nodeLastWriter := replace st.nodeLastWriter n n
    nodeLastWriteTS := replace st.nodeLastWriteTS n (st.nodeTS n)
    nodeWriteEpochID := replace st.nodeWriteEpochID n st.epochID
    rmsgs := addRInv st.rmsgs
      (sendRInvMsg st n (st.nodeTS n) (st.nodeValue n)
        (st.parentOf (st.nodeTS n)) OpKind.write) }

def rmwReplayPost
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (st : RefState Node Value) (n : Node) : RefState Node Value :=
  { st with
    nodeState := replace st.nodeState n HState.hs_replay
    nodeRcvedAcks := replaceRelFirst st.nodeRcvedAcks n (fun _ => False)
    nodeLastWriter := replace st.nodeLastWriter n n
    nodeLastWriteTS := replace st.nodeLastWriteTS n (st.nodeTS n)
    nodeWriteEpochID := replace st.nodeWriteEpochID n st.epochID
    rmsgs := addRInv st.rmsgs
      (sendRInvMsg st n (st.nodeTS n) (st.nodeValue n)
        (st.parentOf (st.nodeTS n)) OpKind.rmw) }

def receiveWriteInvPost
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (rank : NodeRank Node) (st : RefState Node Value)
    (n : Node) (m : RInvMsg Node Value) : RefState Node Value :=
  let ack := sendAckMsg st.epochID n m.ts
  if tsLt rank (st.nodeTS n) m.ts then
    { st with
      nodeTS := replace st.nodeTS n m.ts
      nodeValue := replace st.nodeValue n m.value
      nodeState := replace st.nodeState n (invalidStateAfterGreater st n)
      nodeLastWriter := replace st.nodeLastWriter n m.sender
      nodeFlagRMW := replace st.nodeFlagRMW n false
      parentOf := replace st.parentOf m.ts m.parent
      tsKind := addTsKind st.tsKind m.ts OpKind.write
      tsValue := addTsValue st.tsValue m.ts m.value
      ackMsgs := addAck st.ackMsgs ack }
  else
    { st with ackMsgs := addAck st.ackMsgs ack }

def receiveRmwInvPost
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (rank : NodeRank Node) (st : RefState Node Value)
    (n : Node) (m : RInvMsg Node Value) : RefState Node Value :=
  if tsLt rank (st.nodeTS n) m.ts then
    let ack := sendAckMsg st.epochID n m.ts
    { st with
      nodeTS := replace st.nodeTS n m.ts
      nodeValue := replace st.nodeValue n m.value
      nodeState := replace st.nodeState n (invalidStateAfterGreater st n)
      nodeLastWriter := replace st.nodeLastWriter n m.sender
      nodeFlagRMW := replace st.nodeFlagRMW n true
      parentOf := replace st.parentOf m.ts m.parent
      tsKind := addTsKind st.tsKind m.ts OpKind.rmw
      tsValue := addTsValue st.tsValue m.ts m.value
      ackMsgs := addAck st.ackMsgs ack }
  else if st.nodeTS n = m.ts then
    let ack := sendAckMsg st.epochID n m.ts
    { st with ackMsgs := addAck st.ackMsgs ack }
  else
    let localKind := if st.nodeFlagRMW n then OpKind.rmw else OpKind.write
    { st with
      rmsgs := addRInv st.rmsgs
        (sendRInvMsg st n (st.nodeTS n) (st.nodeValue n)
          (st.parentOf (st.nodeTS n)) localKind) }

def receiveAckPost
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (st : RefState Node Value) (n sender : Node) : RefState Node Value :=
  { st with
    nodeRcvedAcks :=
      replaceRelFirst st.nodeRcvedAcks n
        (fun a => st.nodeRcvedAcks n a \/ a = sender) }

def sendValsPost
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (st : RefState Node Value) (n : Node) : RefState Node Value :=
  let t := st.nodeTS n
  { st with
    nodeState := replace st.nodeState n HState.hs_valid
    valMsgs := addVal st.valMsgs { ts := t }
    committedRMWs :=
      if st.nodeFlagRMW n then addCommitted st.committedRMWs t else st.committedRMWs
    committedWrites :=
      if st.nodeFlagRMW n then st.committedWrites else addCommitted st.committedWrites t }

def receiveValPost
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (st : RefState Node Value) (n : Node) (t : Timestamp Node) :
    RefState Node Value :=
  if st.nodeTS n = t then
    { st with nodeState := replace st.nodeState n HState.hs_valid }
  else
    st

def followerReplayPost
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (st : RefState Node Value) (n : Node) : RefState Node Value :=
  { st with
    nodeState := replace st.nodeState n HState.hs_replay
    nodeRcvedAcks := replaceRelFirst st.nodeRcvedAcks n (fun _ => False)
    nodeLastWriter := replace st.nodeLastWriter n n
    nodeLastWriteTS := replace st.nodeLastWriteTS n (st.nodeTS n)
    nodeWriteEpochID := replace st.nodeWriteEpochID n st.epochID
    rmsgs := addRInv st.rmsgs
      (sendRInvMsg st n (st.nodeTS n) (st.nodeValue n)
        (st.parentOf (st.nodeTS n))
        (if st.nodeFlagRMW n then OpKind.rmw else OpKind.write)) }

def nodeFailurePost
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (st : RefState Node Value) (n : Node) : RefState Node Value :=
  { st with
    epochID := st.epochID + 1
    live := removeLive st.live n
    nodeRcvedAcks := fun _ _ => False }

def o3ObservePost
    {Node : Type uRefNode} {Value : Type uRefValue}
    (st : RefState Node Value) (n coordinator : Node) (t : Timestamp Node) :
    RefState Node Value :=
  { st with o3Quorum := addO3 st.o3Quorum n coordinator t st.epochID }

def o3CompletePost
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (st : RefState Node Value) (n : Node) : RefState Node Value :=
  let t := st.nodeTS n
  { st with
    nodeState := replace st.nodeState n HState.hs_valid
    committedRMWs :=
      if st.nodeFlagRMW n then addCommitted st.committedRMWs t else st.committedRMWs
    committedWrites :=
      if st.nodeFlagRMW n then st.committedWrites else addCommitted st.committedWrites t }

inductive RefLabel (Node : Type uRefNode) where
  | silent
  | sendVals (n : Node)
  | o3Complete (n : Node)
deriving Repr

inductive HRNext {Node : Type uRefNode} {Value : Type uRefValue}
    [DecidableEq Node] (rank : NodeRank Node) :
    RefState Node Value -> RefLabel Node -> RefState Node Value -> Prop where
  | hr_write {st n v} :
      st.live n ->
      st.nodeState n = HState.hs_valid ->
      HRNext rank st .silent (writePost st n v)
  | hr_rmw {st n v} :
      st.live n ->
      st.nodeState n = HState.hs_valid ->
      HRNext rank st .silent (rmwPost st n v)
  | hr_write_replay {st n} :
      st.live n ->
      (st.nodeState n = HState.hs_write \/ st.nodeState n = HState.hs_replay) ->
      st.nodeWriteEpochID n < st.epochID ->
      Not (receivedAllAcks st n) ->
      st.nodeFlagRMW n = false ->
      HRNext rank st .silent (writeReplayPost st n)
  | hr_rmw_replay {st n} :
      st.live n ->
      (st.nodeState n = HState.hs_write \/ st.nodeState n = HState.hs_replay) ->
      st.nodeWriteEpochID n < st.epochID ->
      Not (receivedAllAcks st n) ->
      st.nodeFlagRMW n = true ->
      HRNext rank st .silent (rmwReplayPost st n)
  | hr_rcv_ack {st n m} :
      st.live n ->
      st.ackMsgs m ->
      m.epochID = st.epochID ->
      m.sender ≠ n ->
      Not (st.nodeRcvedAcks n m.sender) ->
      m.ts = st.nodeLastWriteTS n ->
      (st.nodeState n = HState.hs_write \/
        st.nodeState n = HState.hs_invalid_write \/
        st.nodeState n = HState.hs_replay) ->
      HRNext rank st .silent (receiveAckPost st n m.sender)
  | hr_send_vals_rmw {st n} :
      st.live n ->
      st.nodeFlagRMW n = true ->
      (st.nodeState n = HState.hs_write \/ st.nodeState n = HState.hs_replay) ->
      receivedAllAcks st n ->
      HRNext rank st (.sendVals n) (sendValsPost st n)
  | hr_send_vals_write {st n} :
      st.live n ->
      st.nodeFlagRMW n = false ->
      (st.nodeState n = HState.hs_write \/ st.nodeState n = HState.hs_replay) ->
      receivedAllAcks st n ->
      HRNext rank st (.sendVals n) (sendValsPost st n)
  | hr_rcv_write_inv {st n m} :
      st.live n ->
      st.rmsgs m ->
      m.epochID = st.epochID ->
      m.sender ≠ n ->
      m.kind = OpKind.write ->
      HRNext rank st .silent (receiveWriteInvPost rank st n m)
  | hr_rcv_rmw_inv {st n m} :
      st.live n ->
      st.rmsgs m ->
      m.epochID = st.epochID ->
      m.sender ≠ n ->
      m.kind = OpKind.rmw ->
      HRNext rank st .silent (receiveRmwInvPost rank st n m)
  | hr_rcv_val {st n m} :
      st.live n ->
      st.valMsgs m ->
      st.nodeState n ≠ HState.hs_valid ->
      HRNext rank st .silent (receiveValPost st n m.ts)
  | hr_follower_replay {st n} :
      st.live n ->
      (st.nodeState n = HState.hs_invalid \/
        st.nodeState n = HState.hs_invalid_write) ->
      Not (st.live (st.nodeLastWriter n)) ->
      HRNext rank st .silent (followerReplayPost st n)
  | hr_node_failure {st n} :
      st.live n ->
      HRNext rank st .silent (nodeFailurePost st n)
  | hr_o3_observe {st n c t} :
      st.live n ->
      o3AckQuorum rank st c t ->
      HRNext rank st .silent (o3ObservePost st n c t)
  | hr_o3_complete {st n c} :
      st.live n ->
      st.nodeState n ≠ HState.hs_valid ->
      st.o3Quorum n c (st.nodeTS n) st.epochID ->
      HRNext rank st (.o3Complete n) (o3CompletePost st n)

def initTimestamp {Node : Type uRefNode} (initNode : Node) : Timestamp Node where
  version := 0
  tieBreaker := initNode

def RefCommitted
    {Node : Type uRefNode} {Value : Type uRefValue}
    (initTs : Timestamp Node) (st : RefState Node Value)
    (t : Timestamp Node) : Prop :=
  t = initTs \/ st.committedRMWs t \/ st.committedWrites t

def stateCanSendVal {Node : Type uRefNode} {Value : Type uRefValue}
    (st : RefState Node Value) (n : Node) : Prop :=
  st.nodeState n = HState.hs_write \/ st.nodeState n = HState.hs_replay

structure RefAgreementInvariant
    {Node : Type uRefNode} {Value : Type uRefValue}
    (rank : NodeRank Node) (initTs : Timestamp Node)
    (st : RefState Node Value) : Prop where
  init_le_live :
    forall n, st.live n -> tsLe rank initTs (st.nodeTS n)
  ack_msg_advanced :
    forall m, st.ackMsgs m -> tsLe rank m.ts (st.nodeTS m.sender)
  rcved_ack_advanced :
    forall n a, st.nodeRcvedAcks n a ->
      tsLe rank (st.nodeLastWriteTS n) (st.nodeTS a)
  val_msg_committed :
    forall m, st.valMsgs m -> RefCommitted initTs st m.ts
  committed_live_advanced :
    forall t n, (st.committedRMWs t \/ st.committedWrites t) ->
      st.live n -> tsLe rank t (st.nodeTS n)
  valid_committed :
    forall n, st.nodeState n = HState.hs_valid ->
      RefCommitted initTs st (st.nodeTS n)
  write_or_replay_last_current :
    forall n, stateCanSendVal st n ->
      st.nodeLastWriteTS n = st.nodeTS n
  o3_quorum_epoch_le :
    forall n c t e, st.o3Quorum n c t e -> e <= st.epochID
  o3_quorum_nonself_ack :
    forall n c t, st.o3Quorum n c t st.epochID ->
      forall a, st.live a -> a ≠ c ->
        st.ackMsgs { sender := a, epochID := st.epochID, ts := t }
  o3_current_quorum_advanced :
    forall n c t, st.o3Quorum n c t st.epochID ->
      forall a, st.live a -> tsLe rank t (st.nodeTS a)

theorem tsLe_total {Node : Type uRefNode} (rank : NodeRank Node)
    (a b : Timestamp Node) :
    tsLe rank a b \/ tsLe rank b a := by
  rcases a with ⟨av, atie⟩
  rcases b with ⟨bv, btie⟩
  unfold tsLe
  simp
  by_cases havb : av < bv
  · exact Or.inl (Or.inl havb)
  · by_cases hbav : bv < av
    · exact Or.inr (Or.inl hbav)
    · have hv : av = bv := by omega
      subst hv
      rcases Nat.le_total (rank.rank atie) (rank.rank btie) with hle | hle
      · exact Or.inl (Or.inr ⟨rfl, hle⟩)
      · exact Or.inr (Or.inr ⟨rfl, hle⟩)

theorem tsLe_of_not_tsLt {Node : Type uRefNode} (rank : NodeRank Node)
    {a b : Timestamp Node} :
    Not (tsLt rank a b) -> tsLe rank b a := by
  intro hNot
  rcases tsLe_total rank a b with hab | hba
  · by_cases hEq : a = b
    · simpa [hEq] using tsLe_refl rank b
    · exact False.elim (hNot ⟨hab, hEq⟩)
  · exact hba

theorem tsLe_to_succ {Node : Type uRefNode} (rank : NodeRank Node)
    (t : Timestamp Node) (n : Node) :
    tsLe rank t (tsOf (t.version.succ) n) := by
  unfold tsLe tsOf
  exact Or.inl (Nat.lt_succ_self t.version)

theorem tsLe_to_succ_succ {Node : Type uRefNode} (rank : NodeRank Node)
    (t : Timestamp Node) (n : Node) :
    tsLe rank t (tsOf (t.version.succ.succ) n) := by
  unfold tsLe tsOf
  exact Or.inl (Nat.lt_trans (Nat.lt_succ_self t.version)
    (Nat.lt_succ_self t.version.succ))

theorem ref_committed_public
    {Node : Type uRefNode} {Value : Type uRefValue}
    {initTs : Timestamp Node} {st : RefState Node Value} {t : Timestamp Node} :
    (st.committedRMWs t \/ st.committedWrites t) ->
      RefCommitted initTs st t := by
  intro h
  exact Or.inr h

theorem ref_committed_live_advanced
    {Node : Type uRefNode} {Value : Type uRefValue}
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st : RefState Node Value}
    (hInv : RefAgreementInvariant rank initTs st) :
    forall t n, RefCommitted initTs st t ->
      st.live n -> tsLe rank t (st.nodeTS n) := by
  intro t n hCommitted hLive
  rcases hCommitted with rfl | hPublic
  · exact hInv.init_le_live n hLive
  · exact hInv.committed_live_advanced t n hPublic hLive

theorem step_nodeTS_monotone
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {st st' : RefState Node Value}
    {label : RefLabel Node}
    (hStep : HRNext rank st label st') :
    forall a, tsLe rank (st.nodeTS a) (st'.nodeTS a) := by
  intro a
  cases hStep with
  | hr_write hLive hState =>
      simp [writePost, startUpdatePost, replace]
      split
      · subst_vars
        exact tsLe_to_succ_succ rank _ _
      · exact tsLe_refl rank _
  | hr_rmw hLive hState =>
      simp [rmwPost, startUpdatePost, replace]
      split
      · subst_vars
        exact tsLe_to_succ rank _ _
      · exact tsLe_refl rank _
  | hr_write_replay hLive hState hEpoch hMissing hFlag =>
      simp [writeReplayPost]
      exact tsLe_refl rank (st.nodeTS a)
  | hr_rmw_replay hLive hState hEpoch hMissing hFlag =>
      simp [rmwReplayPost]
      exact tsLe_refl rank (st.nodeTS a)
  | hr_rcv_ack hLive hMsg hEpoch hSender hFresh hTs hState =>
      simp [receiveAckPost]
      exact tsLe_refl rank (st.nodeTS a)
  | hr_send_vals_rmw hLive hFlag hState hAll =>
      simp [sendValsPost]
      exact tsLe_refl rank (st.nodeTS a)
  | hr_send_vals_write hLive hFlag hState hAll =>
      simp [sendValsPost]
      exact tsLe_refl rank (st.nodeTS a)
  | hr_rcv_write_inv hLive hMsg hEpoch hSender hKind =>
      unfold receiveWriteInvPost
      split
      · rename_i hGreater
        simp [replace]
        split
        · subst_vars
          exact hGreater.1
        · exact tsLe_refl rank _
      ·
        simp
        exact tsLe_refl rank _
  | hr_rcv_rmw_inv hLive hMsg hEpoch hSender hKind =>
      unfold receiveRmwInvPost
      split
      · rename_i hGreater
        simp [replace]
        split
        · subst_vars
          exact hGreater.1
        · exact tsLe_refl rank _
      · split
        · simp
          exact tsLe_refl rank _
        · simp
          exact tsLe_refl rank _
  | hr_rcv_val hLive hVal hState =>
      unfold receiveValPost
      split
      · simp
        exact tsLe_refl rank _
      · exact tsLe_refl rank _
  | hr_follower_replay hLive hState hDead =>
      simp [followerReplayPost]
      exact tsLe_refl rank (st.nodeTS a)
  | hr_node_failure hLive =>
      simp [nodeFailurePost]
      exact tsLe_refl rank (st.nodeTS a)
  | hr_o3_observe hLive hQuorum =>
      simp [o3ObservePost]
      exact tsLe_refl rank (st.nodeTS a)
  | hr_o3_complete hLive hState hQuorum =>
      simp [o3CompletePost]
      exact tsLe_refl rank (st.nodeTS a)

theorem step_live_old_of_new
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {st st' : RefState Node Value}
    {label : RefLabel Node}
    (hStep : HRNext rank st label st') :
    forall a, st'.live a -> st.live a := by
  intro a hLivePost
  cases hStep with
  | hr_write hLive hState =>
      simpa [writePost, startUpdatePost] using hLivePost
  | hr_rmw hLive hState =>
      simpa [rmwPost, startUpdatePost] using hLivePost
  | hr_write_replay hLive hState hEpoch hMissing hFlag =>
      simpa [writeReplayPost] using hLivePost
  | hr_rmw_replay hLive hState hEpoch hMissing hFlag =>
      simpa [rmwReplayPost] using hLivePost
  | hr_rcv_ack hLive hMsg hEpoch hSender hFresh hTs hState =>
      simpa [receiveAckPost] using hLivePost
  | hr_send_vals_rmw hLive hFlag hState hAll =>
      simpa [sendValsPost] using hLivePost
  | hr_send_vals_write hLive hFlag hState hAll =>
      simpa [sendValsPost] using hLivePost
  | hr_rcv_write_inv hLive hMsg hEpoch hSender hKind =>
      unfold receiveWriteInvPost at hLivePost
      split at hLivePost
      · change st.live a at hLivePost
        exact hLivePost
      · change st.live a at hLivePost
        exact hLivePost
  | hr_rcv_rmw_inv hLive hMsg hEpoch hSender hKind =>
      unfold receiveRmwInvPost at hLivePost
      split at hLivePost
      · change st.live a at hLivePost
        exact hLivePost
      · split at hLivePost
        · change st.live a at hLivePost
          exact hLivePost
        · change st.live a at hLivePost
          exact hLivePost
  | hr_rcv_val hLive hVal hState =>
      unfold receiveValPost at hLivePost
      split at hLivePost
      · change st.live a at hLivePost
        exact hLivePost
      · change st.live a at hLivePost
        exact hLivePost
  | hr_follower_replay hLive hState hDead =>
      simpa [followerReplayPost] using hLivePost
  | hr_node_failure hLive =>
      simp [nodeFailurePost, removeLive] at hLivePost
      exact hLivePost.1
  | hr_o3_observe hLive hQuorum =>
      simpa [o3ObservePost] using hLivePost
  | hr_o3_complete hLive hState hQuorum =>
      simpa [o3CompletePost] using hLivePost

theorem send_vals_quorum_advanced
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st : RefState Node Value} {n : Node}
    (hInv : RefAgreementInvariant rank initTs st)
    (hState : stateCanSendVal st n)
    (hAll : receivedAllAcks st n) :
    forall a, st.live a -> tsLe rank (st.nodeTS n) (st.nodeTS a) := by
  intro a hLiveA
  by_cases hA : a = n
  · subst hA
    exact tsLe_refl rank (st.nodeTS a)
  · have hAck : st.nodeRcvedAcks n a := hAll a hLiveA hA
    have hLast : st.nodeLastWriteTS n = st.nodeTS n :=
      hInv.write_or_replay_last_current n hState
    simpa [hLast] using hInv.rcved_ack_advanced n a hAck

theorem o3_ack_quorum_advanced
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st : RefState Node Value} {c : Node} {t : Timestamp Node}
    (hInv : RefAgreementInvariant rank initTs st)
    (hQuorum : o3AckQuorum rank st c t) :
    forall a, st.live a -> tsLe rank t (st.nodeTS a) := by
  intro a hLiveA
  rcases hQuorum with ⟨hLiveC, hCoordAdvanced, hAcked⟩
  by_cases hA : a = c
  · subst hA
    exact hCoordAdvanced
  · exact hInv.ack_msg_advanced
      { sender := a, epochID := st.epochID, ts := t }
      (hAcked a hLiveA hA)

set_option maxHeartbeats 400000 in
theorem step_ack_msg_advanced
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st st' : RefState Node Value} {label : RefLabel Node}
    (hInv : RefAgreementInvariant rank initTs st)
    (hStep : HRNext rank st label st') :
    forall msg, st'.ackMsgs msg -> tsLe rank msg.ts (st'.nodeTS msg.sender) := by
  have hMono := step_nodeTS_monotone hStep
  intro msg hMsgPost
  cases hStep with
  | hr_write hLive hState =>
      exact tsLe_trans rank (hInv.ack_msg_advanced msg (by
        simpa [writePost, startUpdatePost] using hMsgPost)) (hMono msg.sender)
  | hr_rmw hLive hState =>
      exact tsLe_trans rank (hInv.ack_msg_advanced msg (by
        simpa [rmwPost, startUpdatePost] using hMsgPost)) (hMono msg.sender)
  | hr_write_replay hLive hState hEpoch hMissing hFlag =>
      exact tsLe_trans rank (hInv.ack_msg_advanced msg (by
        simpa [writeReplayPost] using hMsgPost)) (hMono msg.sender)
  | hr_rmw_replay hLive hState hEpoch hMissing hFlag =>
      exact tsLe_trans rank (hInv.ack_msg_advanced msg (by
        simpa [rmwReplayPost] using hMsgPost)) (hMono msg.sender)
  | hr_rcv_ack hLive hMsg hEpoch hSender hFresh hTs hState =>
      exact tsLe_trans rank (hInv.ack_msg_advanced msg (by
        simpa [receiveAckPost] using hMsgPost)) (hMono msg.sender)
  | hr_send_vals_rmw hLive hFlag hState hAll =>
      exact tsLe_trans rank (hInv.ack_msg_advanced msg (by
        simpa [sendValsPost] using hMsgPost)) (hMono msg.sender)
  | hr_send_vals_write hLive hFlag hState hAll =>
      exact tsLe_trans rank (hInv.ack_msg_advanced msg (by
        simpa [sendValsPost] using hMsgPost)) (hMono msg.sender)
  | hr_rcv_write_inv hLive hRmsg hEpoch hSender hKind =>
      unfold receiveWriteInvPost at hMsgPost ⊢
      split
      · rename_i hGreater
        simp [hGreater, addAck] at hMsgPost
        rcases hMsgPost with hOld | rfl
        · have hOldLe := hInv.ack_msg_advanced msg hOld
          simp [replace]
          split
          · rename_i hSenderEq
            exact tsLe_trans rank hOldLe (by
              simpa [hSenderEq] using hGreater.1)
          · exact hOldLe
        · simp [sendAckMsg, replace]
          exact tsLe_refl rank _
      · rename_i hNotGreater
        simp [hNotGreater, addAck] at hMsgPost
        rcases hMsgPost with hOld | rfl
        · exact hInv.ack_msg_advanced msg hOld
        · simpa [sendAckMsg] using tsLe_of_not_tsLt rank hNotGreater
  | hr_rcv_rmw_inv hLive hRmsg hEpoch hSender hKind =>
      unfold receiveRmwInvPost at hMsgPost ⊢
      split
      · rename_i hGreater
        simp [hGreater, addAck] at hMsgPost
        rcases hMsgPost with hOld | rfl
        · have hOldLe := hInv.ack_msg_advanced msg hOld
          simp [replace]
          split
          · rename_i hSenderEq
            exact tsLe_trans rank hOldLe (by
              simpa [hSenderEq] using hGreater.1)
          · exact hOldLe
        · simp [sendAckMsg, replace]
          exact tsLe_refl rank _
      · rename_i hNotGreater
        simp [hNotGreater] at hMsgPost
        split
        · rename_i hEq
          simp [hEq, addAck] at hMsgPost
          rcases hMsgPost with hOld | rfl
          · exact hInv.ack_msg_advanced msg hOld
          · simpa [sendAckMsg, hEq] using tsLe_refl rank _
        · rename_i hEq
          simp [hEq] at hMsgPost
          exact hInv.ack_msg_advanced msg hMsgPost
  | hr_rcv_val hLive hVal hState =>
      exact tsLe_trans rank (hInv.ack_msg_advanced msg (by
        unfold receiveValPost at hMsgPost
        split at hMsgPost <;> simpa using hMsgPost)) (hMono msg.sender)
  | hr_follower_replay hLive hState hDead =>
      exact tsLe_trans rank (hInv.ack_msg_advanced msg (by
        simpa [followerReplayPost] using hMsgPost)) (hMono msg.sender)
  | hr_node_failure hLive =>
      exact tsLe_trans rank (hInv.ack_msg_advanced msg (by
        simpa [nodeFailurePost] using hMsgPost)) (hMono msg.sender)
  | hr_o3_observe hLive hQuorum =>
      exact tsLe_trans rank (hInv.ack_msg_advanced msg (by
        simpa [o3ObservePost] using hMsgPost)) (hMono msg.sender)
  | hr_o3_complete hLive hState hQuorum =>
      exact tsLe_trans rank (hInv.ack_msg_advanced msg (by
        simpa [o3CompletePost] using hMsgPost)) (hMono msg.sender)

set_option maxHeartbeats 500000 in
theorem step_rcved_ack_advanced
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st st' : RefState Node Value} {label : RefLabel Node}
    (hInv : RefAgreementInvariant rank initTs st)
    (hStep : HRNext rank st label st') :
    forall owner ackSender, st'.nodeRcvedAcks owner ackSender ->
      tsLe rank (st'.nodeLastWriteTS owner) (st'.nodeTS ackSender) := by
  have hMono := step_nodeTS_monotone hStep
  intro owner ackSender hAckPost
  cases hStep with
  | hr_write hLive hState =>
      rename_i n v
      simp [writePost, startUpdatePost, replaceRelFirst, replace] at hAckPost ⊢
      simp [hAckPost.1]
      exact tsLe_trans rank (hInv.rcved_ack_advanced owner ackSender hAckPost.2)
        (hMono ackSender)
  | hr_rmw hLive hState =>
      rename_i n v
      simp [rmwPost, startUpdatePost, replaceRelFirst, replace] at hAckPost ⊢
      simp [hAckPost.1]
      exact tsLe_trans rank (hInv.rcved_ack_advanced owner ackSender hAckPost.2)
        (hMono ackSender)
  | hr_write_replay hLive hState hEpoch hMissing hFlag =>
      rename_i n
      simp [writeReplayPost, replaceRelFirst, replace] at hAckPost ⊢
      by_cases hOwner : owner = n
      · subst owner
        have hLast := hInv.write_or_replay_last_current n hState
        simpa [hLast] using hInv.rcved_ack_advanced n ackSender hAckPost
      · simp [hOwner]
        exact tsLe_trans rank (hInv.rcved_ack_advanced owner ackSender hAckPost)
          (hMono ackSender)
  | hr_rmw_replay hLive hState hEpoch hMissing hFlag =>
      rename_i n
      simp [rmwReplayPost, replaceRelFirst, replace] at hAckPost ⊢
      simp [hAckPost.1]
      exact tsLe_trans rank (hInv.rcved_ack_advanced owner ackSender hAckPost.2)
        (hMono ackSender)
  | hr_rcv_ack hLive hMsg hEpoch hSender hFresh hTs hState =>
      rename_i n m
      simp [receiveAckPost, replaceRelFirst] at hAckPost ⊢
      by_cases hOwner : owner = n
      · subst owner
        simp at hAckPost
        rcases hAckPost with hOld | hNew
        · exact hInv.rcved_ack_advanced n ackSender hOld
        · subst hNew
          simpa [hTs] using hInv.ack_msg_advanced _ hMsg
      · have hOld : st.nodeRcvedAcks owner ackSender := by
          simpa [hOwner] using hAckPost
        exact hInv.rcved_ack_advanced owner ackSender hOld
  | hr_send_vals_rmw hLive hFlag hState hAll =>
      exact hInv.rcved_ack_advanced owner ackSender (by
        simpa [sendValsPost] using hAckPost)
  | hr_send_vals_write hLive hFlag hState hAll =>
      exact hInv.rcved_ack_advanced owner ackSender (by
        simpa [sendValsPost] using hAckPost)
  | hr_rcv_write_inv hLive hRmsg hEpoch hSender hKind =>
      unfold receiveWriteInvPost at hAckPost ⊢
      split
      · rename_i hGreater
        simp [hGreater, replace]
        split
        · rename_i hAckSender
          exact tsLe_trans rank
            (hInv.rcved_ack_advanced owner ackSender (by
              simpa [hGreater] using hAckPost))
            (by simpa [hAckSender] using hGreater.1)
        · exact hInv.rcved_ack_advanced owner ackSender (by
            simpa [hGreater] using hAckPost)
      · rename_i hNotGreater
        exact hInv.rcved_ack_advanced owner ackSender (by
          simpa [hNotGreater] using hAckPost)
  | hr_rcv_rmw_inv hLive hRmsg hEpoch hSender hKind =>
      unfold receiveRmwInvPost at hAckPost ⊢
      split
      · rename_i hGreater
        simp [hGreater, replace]
        split
        · rename_i hAckSender
          exact tsLe_trans rank
            (hInv.rcved_ack_advanced owner ackSender (by
              simpa [hGreater] using hAckPost))
            (by simpa [hAckSender] using hGreater.1)
        · exact hInv.rcved_ack_advanced owner ackSender (by
            simpa [hGreater] using hAckPost)
      · rename_i hNotGreater
        simp [hNotGreater] at hAckPost
        split
        · rename_i hEq
          exact hInv.rcved_ack_advanced owner ackSender (by
            simpa [hNotGreater, hEq] using hAckPost)
        · rename_i hEq
          exact hInv.rcved_ack_advanced owner ackSender (by
            simpa [hNotGreater, hEq] using hAckPost)
  | hr_rcv_val hLive hVal hState =>
      rename_i n m
      unfold receiveValPost at hAckPost ⊢
      by_cases hEq : st.nodeTS n = m.ts
      · simp [hEq] at hAckPost ⊢
        exact hInv.rcved_ack_advanced owner ackSender hAckPost
      · simp [hEq] at hAckPost ⊢
        exact hInv.rcved_ack_advanced owner ackSender hAckPost
  | hr_follower_replay hLive hState hDead =>
      rename_i n
      simp [followerReplayPost, replaceRelFirst, replace] at hAckPost ⊢
      simp [hAckPost.1]
      exact tsLe_trans rank (hInv.rcved_ack_advanced owner ackSender hAckPost.2)
        (hMono ackSender)
  | hr_node_failure hLive =>
      simp [nodeFailurePost] at hAckPost
  | hr_o3_observe hLive hQuorum =>
      exact hInv.rcved_ack_advanced owner ackSender (by
        simpa [o3ObservePost] using hAckPost)
  | hr_o3_complete hLive hState hQuorum =>
      exact hInv.rcved_ack_advanced owner ackSender (by
        simpa [o3CompletePost] using hAckPost)

theorem step_public_committed_mono
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node}
    {st st' : RefState Node Value} {label : RefLabel Node}
    (hStep : HRNext rank st label st') :
    forall t,
      (st.committedRMWs t \/ st.committedWrites t) ->
        st'.committedRMWs t \/ st'.committedWrites t := by
  intro t hCommitted
  cases hStep with
  | hr_write hLive hState =>
      simpa [writePost, startUpdatePost] using hCommitted
  | hr_rmw hLive hState =>
      simpa [rmwPost, startUpdatePost] using hCommitted
  | hr_write_replay hLive hState hEpoch hMissing hFlag =>
      simpa [writeReplayPost] using hCommitted
  | hr_rmw_replay hLive hState hEpoch hMissing hFlag =>
      simpa [rmwReplayPost] using hCommitted
  | hr_rcv_ack hLive hMsg hEpoch hSender hFresh hTs hState =>
      simpa [receiveAckPost] using hCommitted
  | hr_send_vals_rmw hLive hFlag hState hAll =>
      rcases hCommitted with hRmw | hWrite
      · left
        simp [sendValsPost, hFlag, addCommitted]
        exact Or.inl hRmw
      · right
        simpa [sendValsPost, hFlag] using hWrite
  | hr_send_vals_write hLive hFlag hState hAll =>
      rcases hCommitted with hRmw | hWrite
      · left
        simpa [sendValsPost, hFlag] using hRmw
      · right
        simp [sendValsPost, hFlag, addCommitted]
        exact Or.inl hWrite
  | hr_rcv_write_inv hLive hMsg hEpoch hSender hKind =>
      unfold receiveWriteInvPost
      split <;> simpa using hCommitted
  | hr_rcv_rmw_inv hLive hMsg hEpoch hSender hKind =>
      unfold receiveRmwInvPost
      split
      · simpa using hCommitted
      · split <;> simpa using hCommitted
  | hr_rcv_val hLive hVal hState =>
      unfold receiveValPost
      split <;> simpa using hCommitted
  | hr_follower_replay hLive hState hDead =>
      simpa [followerReplayPost] using hCommitted
  | hr_node_failure hLive =>
      simpa [nodeFailurePost] using hCommitted
  | hr_o3_observe hLive hQuorum =>
      simpa [o3ObservePost] using hCommitted
  | hr_o3_complete hLive hState hQuorum =>
      rename_i n c
      rcases hCommitted with hRmw | hWrite
      · left
        by_cases hFlag : st.nodeFlagRMW n
        · simp [o3CompletePost, hFlag, addCommitted]
          exact Or.inl hRmw
        · simpa [o3CompletePost, hFlag] using hRmw
      · right
        by_cases hFlag : st.nodeFlagRMW n
        · simpa [o3CompletePost, hFlag] using hWrite
        · simp [o3CompletePost, hFlag, addCommitted]
          exact Or.inl hWrite

theorem step_committed_mono
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st st' : RefState Node Value} {label : RefLabel Node}
    (hStep : HRNext rank st label st') :
    forall t, RefCommitted initTs st t -> RefCommitted initTs st' t := by
  intro t hCommitted
  rcases hCommitted with hInit | hPublic
  · exact Or.inl hInit
  · exact Or.inr (step_public_committed_mono hStep t hPublic)

theorem step_val_msg_committed
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st st' : RefState Node Value} {label : RefLabel Node}
    (hInv : RefAgreementInvariant rank initTs st)
    (hStep : HRNext rank st label st') :
    forall m, st'.valMsgs m -> RefCommitted initTs st' m.ts := by
  intro m hValPost
  cases hStep with
  | hr_write hLive hState =>
      exact step_committed_mono (initTs := initTs) (rank := rank)
        (HRNext.hr_write hLive hState) m.ts
        (hInv.val_msg_committed m (by
          simpa [writePost, startUpdatePost] using hValPost))
  | hr_rmw hLive hState =>
      exact step_committed_mono (initTs := initTs) (rank := rank)
        (HRNext.hr_rmw hLive hState) m.ts
        (hInv.val_msg_committed m (by
          simpa [rmwPost, startUpdatePost] using hValPost))
  | hr_write_replay hLive hState hEpoch hMissing hFlag =>
      exact step_committed_mono (initTs := initTs) (rank := rank)
        (HRNext.hr_write_replay hLive hState hEpoch hMissing hFlag) m.ts
        (hInv.val_msg_committed m (by
          simpa [writeReplayPost] using hValPost))
  | hr_rmw_replay hLive hState hEpoch hMissing hFlag =>
      exact step_committed_mono (initTs := initTs) (rank := rank)
        (HRNext.hr_rmw_replay hLive hState hEpoch hMissing hFlag) m.ts
        (hInv.val_msg_committed m (by
          simpa [rmwReplayPost] using hValPost))
  | hr_rcv_ack hLive hMsg hEpoch hSender hFresh hTs hState =>
      exact step_committed_mono (initTs := initTs) (rank := rank)
        (HRNext.hr_rcv_ack hLive hMsg hEpoch hSender hFresh hTs hState) m.ts
        (hInv.val_msg_committed m (by
          simpa [receiveAckPost] using hValPost))
  | hr_send_vals_rmw hLive hFlag hState hAll =>
      simp [sendValsPost, addVal] at hValPost
      rcases hValPost with hOld | hNew
      · exact step_committed_mono (initTs := initTs) (rank := rank)
          (HRNext.hr_send_vals_rmw hLive hFlag hState hAll) m.ts
          (hInv.val_msg_committed m hOld)
      · subst hNew
        right
        left
        simp [sendValsPost, hFlag, addCommitted]
  | hr_send_vals_write hLive hFlag hState hAll =>
      simp [sendValsPost, addVal] at hValPost
      rcases hValPost with hOld | hNew
      · exact step_committed_mono (initTs := initTs) (rank := rank)
          (HRNext.hr_send_vals_write hLive hFlag hState hAll) m.ts
          (hInv.val_msg_committed m hOld)
      · subst hNew
        right
        right
        simp [sendValsPost, hFlag, addCommitted]
  | hr_rcv_write_inv hLive hMsg hEpoch hSender hKind =>
      exact step_committed_mono (initTs := initTs) (rank := rank)
        (HRNext.hr_rcv_write_inv hLive hMsg hEpoch hSender hKind) m.ts
        (hInv.val_msg_committed m (by
          unfold receiveWriteInvPost at hValPost
          split at hValPost <;> simpa using hValPost))
  | hr_rcv_rmw_inv hLive hMsg hEpoch hSender hKind =>
      exact step_committed_mono (initTs := initTs) (rank := rank)
        (HRNext.hr_rcv_rmw_inv hLive hMsg hEpoch hSender hKind) m.ts
        (hInv.val_msg_committed m (by
          unfold receiveRmwInvPost at hValPost
          split at hValPost
          · simpa using hValPost
          · split at hValPost <;> simpa using hValPost))
  | hr_rcv_val hLive hVal hState =>
      exact step_committed_mono (initTs := initTs) (rank := rank)
        (HRNext.hr_rcv_val hLive hVal hState) m.ts
        (hInv.val_msg_committed m (by
          unfold receiveValPost at hValPost
          split at hValPost <;> simpa using hValPost))
  | hr_follower_replay hLive hState hDead =>
      exact step_committed_mono (initTs := initTs) (rank := rank)
        (HRNext.hr_follower_replay hLive hState hDead) m.ts
        (hInv.val_msg_committed m (by
          simpa [followerReplayPost] using hValPost))
  | hr_node_failure hLive =>
      exact step_committed_mono (initTs := initTs) (rank := rank)
        (HRNext.hr_node_failure hLive) m.ts
        (hInv.val_msg_committed m (by
          simpa [nodeFailurePost] using hValPost))
  | hr_o3_observe hLive hQuorum =>
      exact step_committed_mono (initTs := initTs) (rank := rank)
        (HRNext.hr_o3_observe hLive hQuorum) m.ts
        (hInv.val_msg_committed m (by
          simpa [o3ObservePost] using hValPost))
  | hr_o3_complete hLive hState hQuorum =>
      exact step_committed_mono (initTs := initTs) (rank := rank)
        (HRNext.hr_o3_complete hLive hState hQuorum) m.ts
        (hInv.val_msg_committed m (by
          simpa [o3CompletePost] using hValPost))

theorem step_committed_live_advanced
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st st' : RefState Node Value} {label : RefLabel Node}
    (hInv : RefAgreementInvariant rank initTs st)
    (hStep : HRNext rank st label st') :
    forall t a, (st'.committedRMWs t \/ st'.committedWrites t) ->
      st'.live a -> tsLe rank t (st'.nodeTS a) := by
  have hMono := step_nodeTS_monotone hStep
  have hLiveOld := step_live_old_of_new hStep
  intro t a hCommittedPost hLivePost
  have hOldAdvanced :
      (st.committedRMWs t \/ st.committedWrites t) ->
        tsLe rank t (st'.nodeTS a) := by
    intro hCommittedOld
    exact tsLe_trans rank
      (hInv.committed_live_advanced t a hCommittedOld (hLiveOld a hLivePost))
      (hMono a)
  cases hStep with
  | hr_write hLive hState =>
      exact hOldAdvanced (by
        simpa [writePost, startUpdatePost] using hCommittedPost)
  | hr_rmw hLive hState =>
      exact hOldAdvanced (by
        simpa [rmwPost, startUpdatePost] using hCommittedPost)
  | hr_write_replay hLive hState hEpoch hMissing hFlag =>
      exact hOldAdvanced (by
        simpa [writeReplayPost] using hCommittedPost)
  | hr_rmw_replay hLive hState hEpoch hMissing hFlag =>
      exact hOldAdvanced (by
        simpa [rmwReplayPost] using hCommittedPost)
  | hr_rcv_ack hLive hMsg hEpoch hSender hFresh hTs hState =>
      exact hOldAdvanced (by
        simpa [receiveAckPost] using hCommittedPost)
  | hr_send_vals_rmw hLive hFlag hState hAll =>
      simp [sendValsPost, hFlag, addCommitted] at hCommittedPost ⊢
      rcases hCommittedPost with hRmwOrNew | hWriteOld
      · rcases hRmwOrNew with hRmwOld | hNew
        · exact hInv.committed_live_advanced t a (Or.inl hRmwOld) hLivePost
        · subst hNew
          exact send_vals_quorum_advanced hInv hState hAll a hLivePost
      · exact hInv.committed_live_advanced t a (Or.inr hWriteOld) hLivePost
  | hr_send_vals_write hLive hFlag hState hAll =>
      simp [sendValsPost, hFlag, addCommitted] at hCommittedPost ⊢
      rcases hCommittedPost with hRmwOld | hWriteOrNew
      · exact hInv.committed_live_advanced t a (Or.inl hRmwOld) hLivePost
      · rcases hWriteOrNew with hWriteOld | hNew
        · exact hInv.committed_live_advanced t a (Or.inr hWriteOld) hLivePost
        · subst hNew
          exact send_vals_quorum_advanced hInv hState hAll a hLivePost
  | hr_rcv_write_inv hLive hMsg hEpoch hSender hKind =>
      exact hOldAdvanced (by
        unfold receiveWriteInvPost at hCommittedPost
        split at hCommittedPost <;> simpa using hCommittedPost)
  | hr_rcv_rmw_inv hLive hMsg hEpoch hSender hKind =>
      exact hOldAdvanced (by
        unfold receiveRmwInvPost at hCommittedPost
        split at hCommittedPost
        · simpa using hCommittedPost
        · split at hCommittedPost <;> simpa using hCommittedPost)
  | hr_rcv_val hLive hVal hState =>
      exact hOldAdvanced (by
        unfold receiveValPost at hCommittedPost
        split at hCommittedPost <;> simpa using hCommittedPost)
  | hr_follower_replay hLive hState hDead =>
      exact hOldAdvanced (by
        simpa [followerReplayPost] using hCommittedPost)
  | hr_node_failure hLive =>
      exact hOldAdvanced (by
        simpa [nodeFailurePost] using hCommittedPost)
  | hr_o3_observe hLive hQuorum =>
      exact hOldAdvanced (by
        simpa [o3ObservePost] using hCommittedPost)
  | hr_o3_complete hLive hState hQuorum =>
      rename_i n c
      by_cases hFlag : st.nodeFlagRMW n
      · simp [o3CompletePost, hFlag, addCommitted] at hCommittedPost ⊢
        rcases hCommittedPost with hRmwOrNew | hWriteOld
        · rcases hRmwOrNew with hRmwOld | hNew
          · exact hInv.committed_live_advanced t a (Or.inl hRmwOld) hLivePost
          · subst hNew
            exact hInv.o3_current_quorum_advanced n c (st.nodeTS n) hQuorum a hLivePost
        · exact hInv.committed_live_advanced t a (Or.inr hWriteOld) hLivePost
      · simp [o3CompletePost, hFlag, addCommitted] at hCommittedPost ⊢
        rcases hCommittedPost with hRmwOld | hWriteOrNew
        · exact hInv.committed_live_advanced t a (Or.inl hRmwOld) hLivePost
        · rcases hWriteOrNew with hWriteOld | hNew
          · exact hInv.committed_live_advanced t a (Or.inr hWriteOld) hLivePost
          · subst hNew
            exact hInv.o3_current_quorum_advanced n c (st.nodeTS n) hQuorum a hLivePost

theorem invalidStateAfterGreater_not_valid
    {Node : Type uRefNode} {Value : Type uRefValue}
    (st : RefState Node Value) (n : Node) :
    invalidStateAfterGreater st n ≠ HState.hs_valid := by
  unfold invalidStateAfterGreater
  split
  · simp
  · split <;> simp

theorem invalidStateAfterGreater_not_sendval
    {Node : Type uRefNode} {Value : Type uRefValue}
    (st : RefState Node Value) (n : Node) :
    invalidStateAfterGreater st n ≠ HState.hs_write /\
      invalidStateAfterGreater st n ≠ HState.hs_replay := by
  unfold invalidStateAfterGreater
  split
  · simp
  · split <;> simp

theorem step_valid_committed
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st st' : RefState Node Value} {label : RefLabel Node}
    (hInv : RefAgreementInvariant rank initTs st)
    (hStep : HRNext rank st label st') :
    forall a, st'.nodeState a = HState.hs_valid ->
      RefCommitted initTs st' (st'.nodeTS a) := by
  intro a hValidPost
  cases hStep with
  | hr_write hLive hState =>
      rename_i n v
      by_cases hA : a = n
      · simp [writePost, startUpdatePost, replace, hA] at hValidPost
      · have hOldValid : st.nodeState a = HState.hs_valid := by
          simpa [writePost, startUpdatePost, replace, hA] using hValidPost
        simpa [writePost, startUpdatePost, replace, hA] using
          step_committed_mono (initTs := initTs) (rank := rank)
            (HRNext.hr_write hLive hState) (st.nodeTS a)
            (hInv.valid_committed a hOldValid)
  | hr_rmw hLive hState =>
      rename_i n v
      by_cases hA : a = n
      · simp [rmwPost, startUpdatePost, replace, hA] at hValidPost
      · have hOldValid : st.nodeState a = HState.hs_valid := by
          simpa [rmwPost, startUpdatePost, replace, hA] using hValidPost
        simpa [rmwPost, startUpdatePost, replace, hA] using
          step_committed_mono (initTs := initTs) (rank := rank)
            (HRNext.hr_rmw hLive hState) (st.nodeTS a)
            (hInv.valid_committed a hOldValid)
  | hr_write_replay hLive hState hEpoch hMissing hFlag =>
      rename_i n
      by_cases hA : a = n
      · simp [writeReplayPost, replace, hA] at hValidPost
      · have hOldValid : st.nodeState a = HState.hs_valid := by
          simpa [writeReplayPost, replace, hA] using hValidPost
        simpa [writeReplayPost, replace, hA] using
          step_committed_mono (initTs := initTs) (rank := rank)
            (HRNext.hr_write_replay hLive hState hEpoch hMissing hFlag)
            (st.nodeTS a) (hInv.valid_committed a hOldValid)
  | hr_rmw_replay hLive hState hEpoch hMissing hFlag =>
      rename_i n
      by_cases hA : a = n
      · simp [rmwReplayPost, replace, hA] at hValidPost
      · have hOldValid : st.nodeState a = HState.hs_valid := by
          simpa [rmwReplayPost, replace, hA] using hValidPost
        simpa [rmwReplayPost, replace, hA] using
          step_committed_mono (initTs := initTs) (rank := rank)
            (HRNext.hr_rmw_replay hLive hState hEpoch hMissing hFlag)
            (st.nodeTS a) (hInv.valid_committed a hOldValid)
  | hr_rcv_ack hLive hMsg hEpoch hSender hFresh hTs hState =>
      have hOldValid : st.nodeState a = HState.hs_valid := by
        simpa [receiveAckPost] using hValidPost
      simpa [receiveAckPost] using
        step_committed_mono (initTs := initTs) (rank := rank)
          (HRNext.hr_rcv_ack hLive hMsg hEpoch hSender hFresh hTs hState)
          (st.nodeTS a) (hInv.valid_committed a hOldValid)
  | hr_send_vals_rmw hLive hFlag hState hAll =>
      rename_i n
      by_cases hA : a = n
      · subst a
        right
        left
        simp [sendValsPost, hFlag, addCommitted]
      · have hOldValid : st.nodeState a = HState.hs_valid := by
          simpa [sendValsPost, replace, hA] using hValidPost
        simpa [sendValsPost, replace, hA, hFlag] using
          step_committed_mono (initTs := initTs) (rank := rank)
            (HRNext.hr_send_vals_rmw hLive hFlag hState hAll)
            (st.nodeTS a) (hInv.valid_committed a hOldValid)
  | hr_send_vals_write hLive hFlag hState hAll =>
      rename_i n
      by_cases hA : a = n
      · subst a
        right
        right
        simp [sendValsPost, hFlag, addCommitted]
      · have hOldValid : st.nodeState a = HState.hs_valid := by
          simpa [sendValsPost, replace, hA] using hValidPost
        simpa [sendValsPost, replace, hA, hFlag] using
          step_committed_mono (initTs := initTs) (rank := rank)
            (HRNext.hr_send_vals_write hLive hFlag hState hAll)
            (st.nodeTS a) (hInv.valid_committed a hOldValid)
  | hr_rcv_write_inv hLive hMsg hEpoch hSender hKind =>
      rename_i n m
      unfold receiveWriteInvPost at hValidPost ⊢
      split
      · by_cases hA : a = n
        · rename_i hGreater
          simp [hGreater] at hValidPost
          have hBad : invalidStateAfterGreater st n = HState.hs_valid := by
            simpa [replace, hA] using hValidPost
          exact False.elim ((invalidStateAfterGreater_not_valid st n) hBad)
        · rename_i hGreater
          simp [hGreater] at hValidPost
          have hOldValid : st.nodeState a = HState.hs_valid := by
            simpa [replace, hA] using hValidPost
          simpa [RefCommitted, replace, hA] using
            hInv.valid_committed a hOldValid
      · rename_i hNotGreater
        simp [hNotGreater] at hValidPost
        have hOldValid : st.nodeState a = HState.hs_valid := by
          simpa using hValidPost
        simpa [RefCommitted] using hInv.valid_committed a hOldValid
  | hr_rcv_rmw_inv hLive hMsg hEpoch hSender hKind =>
      rename_i n m
      unfold receiveRmwInvPost at hValidPost ⊢
      split
      · by_cases hA : a = n
        · rename_i hGreater
          simp [hGreater] at hValidPost
          have hBad : invalidStateAfterGreater st n = HState.hs_valid := by
            simpa [replace, hA] using hValidPost
          exact False.elim ((invalidStateAfterGreater_not_valid st n) hBad)
        · rename_i hGreater
          simp [hGreater] at hValidPost
          have hOldValid : st.nodeState a = HState.hs_valid := by
            simpa [replace, hA] using hValidPost
          simpa [RefCommitted, replace, hA] using
            hInv.valid_committed a hOldValid
      · split
        · rename_i hNotGreater hEq
          simp [hNotGreater] at hValidPost
          simp [hEq] at hValidPost
          have hOldValid : st.nodeState a = HState.hs_valid := by
            simpa using hValidPost
          simpa [RefCommitted] using hInv.valid_committed a hOldValid
        · rename_i hNotGreater hEq
          simp [hNotGreater, hEq] at hValidPost
          have hOldValid : st.nodeState a = HState.hs_valid := by
            simpa using hValidPost
          simpa [RefCommitted] using hInv.valid_committed a hOldValid
  | hr_rcv_val hLive hVal hState =>
      rename_i n m
      unfold receiveValPost at hValidPost ⊢
      by_cases hEq : st.nodeTS n = m.ts
      · simp [hEq, replace] at hValidPost ⊢
        by_cases hA : a = n
        · subst a
          simpa [RefCommitted, hEq] using hInv.val_msg_committed m hVal
        · have hOldValid : st.nodeState a = HState.hs_valid := by
            simpa [hA] using hValidPost
          simpa [RefCommitted, hA] using hInv.valid_committed a hOldValid
      · simp [hEq] at hValidPost ⊢
        simpa [RefCommitted] using hInv.valid_committed a hValidPost
  | hr_follower_replay hLive hState hDead =>
      rename_i n
      by_cases hA : a = n
      · simp [followerReplayPost, replace, hA] at hValidPost
      · have hOldValid : st.nodeState a = HState.hs_valid := by
          simpa [followerReplayPost, replace, hA] using hValidPost
        simpa [followerReplayPost, replace, hA] using
          step_committed_mono (initTs := initTs) (rank := rank)
            (HRNext.hr_follower_replay hLive hState hDead)
            (st.nodeTS a) (hInv.valid_committed a hOldValid)
  | hr_node_failure hLive =>
      have hOldValid : st.nodeState a = HState.hs_valid := by
        simpa [nodeFailurePost] using hValidPost
      simpa [nodeFailurePost] using
        step_committed_mono (initTs := initTs) (rank := rank)
          (HRNext.hr_node_failure hLive)
          (st.nodeTS a) (hInv.valid_committed a hOldValid)
  | hr_o3_observe hLive hQuorum =>
      have hOldValid : st.nodeState a = HState.hs_valid := by
        simpa [o3ObservePost] using hValidPost
      simpa [o3ObservePost] using
        step_committed_mono (initTs := initTs) (rank := rank)
          (HRNext.hr_o3_observe hLive hQuorum)
          (st.nodeTS a) (hInv.valid_committed a hOldValid)
  | hr_o3_complete hLive hState hQuorum =>
      rename_i n c
      by_cases hA : a = n
      · subst a
        by_cases hFlag : st.nodeFlagRMW n
        · right
          left
          simp [o3CompletePost, hFlag, addCommitted]
        · right
          right
          simp [o3CompletePost, hFlag, addCommitted]
      · have hOldValid : st.nodeState a = HState.hs_valid := by
          simpa [o3CompletePost, replace, hA] using hValidPost
        by_cases hFlag : st.nodeFlagRMW n
        · simpa [o3CompletePost, replace, hA, hFlag] using
            step_committed_mono (initTs := initTs) (rank := rank)
              (HRNext.hr_o3_complete hLive hState hQuorum)
              (st.nodeTS a) (hInv.valid_committed a hOldValid)
        · simpa [o3CompletePost, replace, hA, hFlag] using
            step_committed_mono (initTs := initTs) (rank := rank)
              (HRNext.hr_o3_complete hLive hState hQuorum)
              (st.nodeTS a) (hInv.valid_committed a hOldValid)

theorem step_write_or_replay_last_current
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st st' : RefState Node Value} {label : RefLabel Node}
    (hInv : RefAgreementInvariant rank initTs st)
    (hStep : HRNext rank st label st') :
    forall a, stateCanSendVal st' a ->
      st'.nodeLastWriteTS a = st'.nodeTS a := by
  intro a hStatePost
  cases hStep with
  | hr_write hLive hState =>
      rename_i n v
      by_cases hA : a = n
      · simp [stateCanSendVal, writePost, startUpdatePost, replace, hA]
      · have hOldState : stateCanSendVal st a := by
          simpa [stateCanSendVal, writePost, startUpdatePost, replace, hA] using hStatePost
        simpa [writePost, startUpdatePost, replace, hA] using
          hInv.write_or_replay_last_current a hOldState
  | hr_rmw hLive hState =>
      rename_i n v
      by_cases hA : a = n
      · simp [stateCanSendVal, rmwPost, startUpdatePost, replace, hA]
      · have hOldState : stateCanSendVal st a := by
          simpa [stateCanSendVal, rmwPost, startUpdatePost, replace, hA] using hStatePost
        simpa [rmwPost, startUpdatePost, replace, hA] using
          hInv.write_or_replay_last_current a hOldState
  | hr_write_replay hLive hState hEpoch hMissing hFlag =>
      rename_i n
      by_cases hA : a = n
      · simp [stateCanSendVal, writeReplayPost, replace, hA]
      · have hOldState : stateCanSendVal st a := by
          simpa [stateCanSendVal, writeReplayPost, replace, hA] using hStatePost
        simpa [writeReplayPost, replace, hA] using
          hInv.write_or_replay_last_current a hOldState
  | hr_rmw_replay hLive hState hEpoch hMissing hFlag =>
      rename_i n
      by_cases hA : a = n
      · simp [stateCanSendVal, rmwReplayPost, replace, hA]
      · have hOldState : stateCanSendVal st a := by
          simpa [stateCanSendVal, rmwReplayPost, replace, hA] using hStatePost
        simpa [rmwReplayPost, replace, hA] using
          hInv.write_or_replay_last_current a hOldState
  | hr_rcv_ack hLive hMsg hEpoch hSender hFresh hTs hState =>
      have hOldState : stateCanSendVal st a := by
        simpa [stateCanSendVal, receiveAckPost] using hStatePost
      simpa [receiveAckPost] using hInv.write_or_replay_last_current a hOldState
  | hr_send_vals_rmw hLive hFlag hState hAll =>
      rename_i n
      by_cases hA : a = n
      · simp [stateCanSendVal, sendValsPost, replace, hA] at hStatePost
      · have hOldState : stateCanSendVal st a := by
          simpa [stateCanSendVal, sendValsPost, replace, hA] using hStatePost
        simpa [sendValsPost, replace, hA] using
          hInv.write_or_replay_last_current a hOldState
  | hr_send_vals_write hLive hFlag hState hAll =>
      rename_i n
      by_cases hA : a = n
      · simp [stateCanSendVal, sendValsPost, replace, hA] at hStatePost
      · have hOldState : stateCanSendVal st a := by
          simpa [stateCanSendVal, sendValsPost, replace, hA] using hStatePost
        simpa [sendValsPost, replace, hA] using
          hInv.write_or_replay_last_current a hOldState
  | hr_rcv_write_inv hLive hMsg hEpoch hSender hKind =>
      rename_i n m
      unfold receiveWriteInvPost at hStatePost ⊢
      split
      · rename_i hGreater
        simp [hGreater, stateCanSendVal, replace] at hStatePost ⊢
        by_cases hA : a = n
        · rcases hStatePost with hWrite | hReplay
          · exact False.elim ((invalidStateAfterGreater_not_sendval st n).1 (by simpa [hA] using hWrite))
          · exact False.elim ((invalidStateAfterGreater_not_sendval st n).2 (by simpa [hA] using hReplay))
        · have hOldState : stateCanSendVal st a := by
            simpa [stateCanSendVal, replace, hA] using hStatePost
          simpa [replace, hA] using hInv.write_or_replay_last_current a hOldState
      · rename_i hNotGreater
        simp [hNotGreater] at hStatePost ⊢
        exact hInv.write_or_replay_last_current a hStatePost
  | hr_rcv_rmw_inv hLive hMsg hEpoch hSender hKind =>
      rename_i n m
      unfold receiveRmwInvPost at hStatePost ⊢
      split
      · rename_i hGreater
        simp [hGreater, stateCanSendVal, replace] at hStatePost ⊢
        by_cases hA : a = n
        · rcases hStatePost with hWrite | hReplay
          · exact False.elim ((invalidStateAfterGreater_not_sendval st n).1 (by simpa [hA] using hWrite))
          · exact False.elim ((invalidStateAfterGreater_not_sendval st n).2 (by simpa [hA] using hReplay))
        · have hOldState : stateCanSendVal st a := by
            simpa [stateCanSendVal, replace, hA] using hStatePost
          simpa [replace, hA] using hInv.write_or_replay_last_current a hOldState
      · rename_i hNotGreater
        simp [hNotGreater] at hStatePost ⊢
        split
        · rename_i hEq
          simp [hEq] at hStatePost ⊢
          exact hInv.write_or_replay_last_current a hStatePost
        · rename_i hEq
          simp [hEq] at hStatePost ⊢
          exact hInv.write_or_replay_last_current a hStatePost
  | hr_rcv_val hLive hVal hState =>
      rename_i n m
      unfold receiveValPost at hStatePost ⊢
      by_cases hEq : st.nodeTS n = m.ts
      · simp [hEq, stateCanSendVal, replace] at hStatePost ⊢
        by_cases hA : a = n
        · simp [hA] at hStatePost
        · have hOldState : stateCanSendVal st a := by
            simpa [stateCanSendVal, hA] using hStatePost
          simpa [hA] using hInv.write_or_replay_last_current a hOldState
      · simp [hEq] at hStatePost ⊢
        exact hInv.write_or_replay_last_current a hStatePost
  | hr_follower_replay hLive hState hDead =>
      rename_i n
      by_cases hA : a = n
      · simp [stateCanSendVal, followerReplayPost, replace, hA]
      · have hOldState : stateCanSendVal st a := by
          simpa [stateCanSendVal, followerReplayPost, replace, hA] using hStatePost
        simpa [followerReplayPost, replace, hA] using
          hInv.write_or_replay_last_current a hOldState
  | hr_node_failure hLive =>
      have hOldState : stateCanSendVal st a := by
        simpa [stateCanSendVal, nodeFailurePost] using hStatePost
      simpa [nodeFailurePost] using hInv.write_or_replay_last_current a hOldState
  | hr_o3_observe hLive hQuorum =>
      have hOldState : stateCanSendVal st a := by
        simpa [stateCanSendVal, o3ObservePost] using hStatePost
      simpa [o3ObservePost] using hInv.write_or_replay_last_current a hOldState
  | hr_o3_complete hLive hState hQuorum =>
      rename_i n c
      by_cases hA : a = n
      · simp [stateCanSendVal, o3CompletePost, replace, hA] at hStatePost
      · have hOldState : stateCanSendVal st a := by
          simpa [stateCanSendVal, o3CompletePost, replace, hA] using hStatePost
        by_cases hFlag : st.nodeFlagRMW n
        · simpa [o3CompletePost, replace, hA, hFlag] using
            hInv.write_or_replay_last_current a hOldState
        · simpa [o3CompletePost, replace, hA, hFlag] using
            hInv.write_or_replay_last_current a hOldState

theorem step_init_le_live
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st st' : RefState Node Value} {label : RefLabel Node}
    (hInv : RefAgreementInvariant rank initTs st)
    (hStep : HRNext rank st label st') :
    forall a, st'.live a -> tsLe rank initTs (st'.nodeTS a) := by
  have hMono := step_nodeTS_monotone hStep
  have hLiveOld := step_live_old_of_new hStep
  intro a hLivePost
  exact tsLe_trans rank (hInv.init_le_live a (hLiveOld a hLivePost)) (hMono a)

theorem step_o3_quorum_epoch_le
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st st' : RefState Node Value} {label : RefLabel Node}
    (hInv : RefAgreementInvariant rank initTs st)
    (hStep : HRNext rank st label st') :
    forall n c t e, st'.o3Quorum n c t e -> e <= st'.epochID := by
  intro n c t e hQPost
  cases hStep with
  | hr_write hLive hState =>
      simpa [writePost, startUpdatePost] using hInv.o3_quorum_epoch_le n c t e hQPost
  | hr_rmw hLive hState =>
      simpa [rmwPost, startUpdatePost] using hInv.o3_quorum_epoch_le n c t e hQPost
  | hr_write_replay hLive hState hEpoch hMissing hFlag =>
      simpa [writeReplayPost] using hInv.o3_quorum_epoch_le n c t e hQPost
  | hr_rmw_replay hLive hState hEpoch hMissing hFlag =>
      simpa [rmwReplayPost] using hInv.o3_quorum_epoch_le n c t e hQPost
  | hr_rcv_ack hLive hMsg hEpoch hSender hFresh hTs hState =>
      simpa [receiveAckPost] using hInv.o3_quorum_epoch_le n c t e hQPost
  | hr_send_vals_rmw hLive hFlag hState hAll =>
      simpa [sendValsPost] using hInv.o3_quorum_epoch_le n c t e hQPost
  | hr_send_vals_write hLive hFlag hState hAll =>
      simpa [sendValsPost] using hInv.o3_quorum_epoch_le n c t e hQPost
  | hr_rcv_write_inv hLive hMsg hEpoch hSender hKind =>
      unfold receiveWriteInvPost at hQPost ⊢
      split
      · rename_i hGreater
        simp [hGreater] at hQPost
        simpa using hInv.o3_quorum_epoch_le n c t e hQPost
      · rename_i hNotGreater
        simp [hNotGreater] at hQPost
        simpa using hInv.o3_quorum_epoch_le n c t e hQPost
  | hr_rcv_rmw_inv hLive hMsg hEpoch hSender hKind =>
      unfold receiveRmwInvPost at hQPost ⊢
      split
      · rename_i hGreater
        simp [hGreater] at hQPost
        simpa using hInv.o3_quorum_epoch_le n c t e hQPost
      · rename_i hNotGreater
        simp [hNotGreater] at hQPost
        split
        · rename_i hEq
          simp [hEq] at hQPost
          simpa using hInv.o3_quorum_epoch_le n c t e hQPost
        · rename_i hEq
          simp [hEq] at hQPost
          simpa using hInv.o3_quorum_epoch_le n c t e hQPost
  | hr_rcv_val hLive hVal hState =>
      unfold receiveValPost at hQPost ⊢
      split
      · rename_i hEq
        simp [hEq] at hQPost
        simpa using hInv.o3_quorum_epoch_le n c t e hQPost
      · rename_i hEq
        simp [hEq] at hQPost
        simpa using hInv.o3_quorum_epoch_le n c t e hQPost
  | hr_follower_replay hLive hState hDead =>
      simpa [followerReplayPost] using hInv.o3_quorum_epoch_le n c t e hQPost
  | hr_node_failure hLive =>
      exact Nat.le_succ_of_le (by
        simpa [nodeFailurePost] using hInv.o3_quorum_epoch_le n c t e hQPost)
  | hr_o3_observe hLive hQuorum =>
      simp [o3ObservePost, addO3] at hQPost
      rcases hQPost with hOld | hNew
      · exact hInv.o3_quorum_epoch_le n c t e hOld
      · rcases hNew with ⟨rfl, rfl, rfl, rfl⟩
        exact Nat.le_refl st.epochID
  | hr_o3_complete hLive hState hQuorum =>
      rename_i nStep cStep
      by_cases hFlag : st.nodeFlagRMW nStep
      · simpa [o3CompletePost, hFlag] using hInv.o3_quorum_epoch_le n c t e hQPost
      · simpa [o3CompletePost, hFlag] using hInv.o3_quorum_epoch_le n c t e hQPost

theorem step_o3_quorum_nonself_ack
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st st' : RefState Node Value} {label : RefLabel Node}
    (hInv : RefAgreementInvariant rank initTs st)
    (hStep : HRNext rank st label st') :
    forall n c t, st'.o3Quorum n c t st'.epochID ->
      forall a, st'.live a -> a ≠ c ->
        st'.ackMsgs { sender := a, epochID := st'.epochID, ts := t } := by
  intro n c t hQPost a hLivePost hNe
  cases hStep with
  | hr_write hLive hState =>
      exact hInv.o3_quorum_nonself_ack n c t
        (by simpa [writePost, startUpdatePost] using hQPost) a
        (by simpa [writePost, startUpdatePost] using hLivePost) hNe
  | hr_rmw hLive hState =>
      exact hInv.o3_quorum_nonself_ack n c t
        (by simpa [rmwPost, startUpdatePost] using hQPost) a
        (by simpa [rmwPost, startUpdatePost] using hLivePost) hNe
  | hr_write_replay hLive hState hEpoch hMissing hFlag =>
      exact hInv.o3_quorum_nonself_ack n c t
        (by simpa [writeReplayPost] using hQPost) a
        (by simpa [writeReplayPost] using hLivePost) hNe
  | hr_rmw_replay hLive hState hEpoch hMissing hFlag =>
      exact hInv.o3_quorum_nonself_ack n c t
        (by simpa [rmwReplayPost] using hQPost) a
        (by simpa [rmwReplayPost] using hLivePost) hNe
  | hr_rcv_ack hLive hMsg hEpoch hSender hFresh hTs hState =>
      exact hInv.o3_quorum_nonself_ack n c t
        (by simpa [receiveAckPost] using hQPost) a
        (by simpa [receiveAckPost] using hLivePost) hNe
  | hr_send_vals_rmw hLive hFlag hState hAll =>
      exact hInv.o3_quorum_nonself_ack n c t
        (by simpa [sendValsPost] using hQPost) a
        (by simpa [sendValsPost] using hLivePost) hNe
  | hr_send_vals_write hLive hFlag hState hAll =>
      exact hInv.o3_quorum_nonself_ack n c t
        (by simpa [sendValsPost] using hQPost) a
        (by simpa [sendValsPost] using hLivePost) hNe
  | hr_rcv_write_inv hLive hMsg hEpoch hSender hKind =>
      rename_i recv msg
      unfold receiveWriteInvPost at hQPost hLivePost ⊢
      split
      · rename_i hGreater
        have hOldAck := hInv.o3_quorum_nonself_ack n c t
          (by simpa [hGreater] using hQPost) a
          (by simpa [hGreater] using hLivePost) hNe
        simpa [hGreater, addAck] using Or.inl hOldAck
      · rename_i hNotGreater
        have hOldAck := hInv.o3_quorum_nonself_ack n c t
          (by simpa [hNotGreater] using hQPost) a
          (by simpa [hNotGreater] using hLivePost) hNe
        simpa [hNotGreater, addAck] using Or.inl hOldAck
  | hr_rcv_rmw_inv hLive hMsg hEpoch hSender hKind =>
      rename_i recv msg
      unfold receiveRmwInvPost at hQPost hLivePost ⊢
      split
      · rename_i hGreater
        have hOldAck := hInv.o3_quorum_nonself_ack n c t
          (by simpa [hGreater] using hQPost) a
          (by simpa [hGreater] using hLivePost) hNe
        simpa [hGreater, addAck] using Or.inl hOldAck
      · rename_i hNotGreater
        split
        · rename_i hEq
          have hOldQ : st.o3Quorum n c t st.epochID := by
            change st.o3Quorum n c t st.epochID at hQPost
            exact hQPost
          have hOldLive : st.live a := by
            change st.live a at hLivePost
            exact hLivePost
          have hOldAck := hInv.o3_quorum_nonself_ack n c t hOldQ a hOldLive hNe
          change (addAck st.ackMsgs (sendAckMsg st.epochID recv msg.ts))
            { sender := a, epochID := st.epochID, ts := t }
          exact Or.inl hOldAck
        · rename_i hEq
          have hOldQ : st.o3Quorum n c t st.epochID := by
            change st.o3Quorum n c t st.epochID at hQPost
            exact hQPost
          have hOldLive : st.live a := by
            change st.live a at hLivePost
            exact hLivePost
          exact hInv.o3_quorum_nonself_ack n c t hOldQ a hOldLive hNe
  | hr_rcv_val hLive hVal hState =>
      unfold receiveValPost at hQPost hLivePost ⊢
      split
      · rename_i hEq
        exact hInv.o3_quorum_nonself_ack n c t
          (by simpa [hEq] using hQPost) a
          (by simpa [hEq] using hLivePost) hNe
      · rename_i hEq
        exact hInv.o3_quorum_nonself_ack n c t
          (by simpa [hEq] using hQPost) a
          (by simpa [hEq] using hLivePost) hNe
  | hr_follower_replay hLive hState hDead =>
      exact hInv.o3_quorum_nonself_ack n c t
        (by simpa [followerReplayPost] using hQPost) a
        (by simpa [followerReplayPost] using hLivePost) hNe
  | hr_node_failure hLive =>
      have hOldQ : st.o3Quorum n c t (st.epochID + 1) := by
        simpa [nodeFailurePost] using hQPost
      have hImpossible : st.epochID + 1 <= st.epochID :=
        hInv.o3_quorum_epoch_le n c t (st.epochID + 1) hOldQ
      omega
  | hr_o3_observe hLive hQuorum =>
      simp [o3ObservePost, addO3] at hQPost
      rcases hQPost with hOld | hNew
      · exact hInv.o3_quorum_nonself_ack n c t hOld a hLivePost hNe
      · rcases hNew with ⟨rfl, rfl, rfl, rfl⟩
        exact hQuorum.2.2 a hLivePost hNe
  | hr_o3_complete hLive hState hQuorum =>
      rename_i nStep cStep
      by_cases hFlag : st.nodeFlagRMW nStep
      · exact hInv.o3_quorum_nonself_ack n c t
          (by simpa [o3CompletePost, hFlag] using hQPost) a
          (by simpa [o3CompletePost, hFlag] using hLivePost) hNe
      · exact hInv.o3_quorum_nonself_ack n c t
          (by simpa [o3CompletePost, hFlag] using hQPost) a
          (by simpa [o3CompletePost, hFlag] using hLivePost) hNe

theorem step_o3_current_quorum_advanced
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st st' : RefState Node Value} {label : RefLabel Node}
    (hInv : RefAgreementInvariant rank initTs st)
    (hStep : HRNext rank st label st') :
    forall n c t, st'.o3Quorum n c t st'.epochID ->
      forall a, st'.live a -> tsLe rank t (st'.nodeTS a) := by
  have hMono := step_nodeTS_monotone hStep
  have hLiveOld := step_live_old_of_new hStep
  intro n c t hQPost a hLivePost
  have hOldAdvanced :
      st.o3Quorum n c t st.epochID ->
        tsLe rank t (st'.nodeTS a) := by
    intro hQOld
    exact tsLe_trans rank
      (hInv.o3_current_quorum_advanced n c t hQOld a (hLiveOld a hLivePost))
      (hMono a)
  cases hStep with
  | hr_write hLive hState =>
      exact hOldAdvanced (by simpa [writePost, startUpdatePost] using hQPost)
  | hr_rmw hLive hState =>
      exact hOldAdvanced (by simpa [rmwPost, startUpdatePost] using hQPost)
  | hr_write_replay hLive hState hEpoch hMissing hFlag =>
      exact hOldAdvanced (by simpa [writeReplayPost] using hQPost)
  | hr_rmw_replay hLive hState hEpoch hMissing hFlag =>
      exact hOldAdvanced (by simpa [rmwReplayPost] using hQPost)
  | hr_rcv_ack hLive hMsg hEpoch hSender hFresh hTs hState =>
      exact hOldAdvanced (by simpa [receiveAckPost] using hQPost)
  | hr_send_vals_rmw hLive hFlag hState hAll =>
      exact hOldAdvanced (by simpa [sendValsPost] using hQPost)
  | hr_send_vals_write hLive hFlag hState hAll =>
      exact hOldAdvanced (by simpa [sendValsPost] using hQPost)
  | hr_rcv_write_inv hLive hMsg hEpoch hSender hKind =>
      exact hOldAdvanced (by
        unfold receiveWriteInvPost at hQPost
        split at hQPost <;> simpa using hQPost)
  | hr_rcv_rmw_inv hLive hMsg hEpoch hSender hKind =>
      exact hOldAdvanced (by
        unfold receiveRmwInvPost at hQPost
        split at hQPost
        · simpa using hQPost
        · split at hQPost <;> simpa using hQPost)
  | hr_rcv_val hLive hVal hState =>
      exact hOldAdvanced (by
        unfold receiveValPost at hQPost
        split at hQPost <;> simpa using hQPost)
  | hr_follower_replay hLive hState hDead =>
      exact hOldAdvanced (by simpa [followerReplayPost] using hQPost)
  | hr_node_failure hLive =>
      have hImpossible : st.epochID + 1 <= st.epochID := by
        have hOldQ : st.o3Quorum n c t (st.epochID + 1) := by
          simpa [nodeFailurePost] using hQPost
        exact hInv.o3_quorum_epoch_le n c t (st.epochID + 1) hOldQ
      omega
  | hr_o3_observe hLive hQuorum =>
      simp [o3ObservePost, addO3] at hQPost
      rcases hQPost with hOld | hNew
      · exact hOldAdvanced hOld
      · rcases hNew with ⟨rfl, rfl, rfl, rfl⟩
        exact o3_ack_quorum_advanced hInv hQuorum a hLivePost
  | hr_o3_complete hLive hState hQuorum =>
      rename_i nStep cStep
      by_cases hFlag : st.nodeFlagRMW nStep
      · exact hOldAdvanced (by simpa [o3CompletePost, hFlag] using hQPost)
      · exact hOldAdvanced (by simpa [o3CompletePost, hFlag] using hQPost)

theorem ref_agreement_preserved
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st st' : RefState Node Value} {label : RefLabel Node}
    (hInv : RefAgreementInvariant rank initTs st)
    (hStep : HRNext rank st label st') :
    RefAgreementInvariant rank initTs st' := by
  refine {
    init_le_live := step_init_le_live hInv hStep
    ack_msg_advanced := step_ack_msg_advanced hInv hStep
    rcved_ack_advanced := step_rcved_ack_advanced hInv hStep
    val_msg_committed := step_val_msg_committed hInv hStep
    committed_live_advanced := step_committed_live_advanced hInv hStep
    valid_committed := step_valid_committed hInv hStep
    write_or_replay_last_current := step_write_or_replay_last_current hInv hStep
    o3_quorum_epoch_le := step_o3_quorum_epoch_le hInv hStep
    o3_quorum_nonself_ack := step_o3_quorum_nonself_ack hInv hStep
    o3_current_quorum_advanced := step_o3_current_quorum_advanced hInv hStep
  }

def HConsistent {Node : Type uRefNode} {Value : Type uRefValue}
    (st : RefState Node Value) : Prop :=
  forall k s,
    st.live k -> st.live s ->
    st.nodeState k = HState.hs_valid ->
    st.nodeState s = HState.hs_valid ->
    st.nodeTS k = st.nodeTS s

def HRSemanticsRMW {Node : Type uRefNode} {Value : Type uRefValue}
    (st : RefState Node Value) : Prop :=
  (forall x y,
    st.committedRMWs x -> st.committedWrites y ->
      x.version ≠ y.version /\ x.version ≠ y.version - 1) /\
  (forall x y,
    st.committedRMWs x -> st.committedRMWs y ->
      x.version ≠ y.version \/ x.tieBreaker = y.tieBreaker)

def RmwWriteGap {Node : Type uRefNode}
    (rmw write : Timestamp Node) : Prop :=
  rmw.version ≠ write.version /\ rmw.version ≠ write.version - 1

def RmwSameVersionOK {Node : Type uRefNode}
    (x y : Timestamp Node) : Prop :=
  x.version ≠ y.version \/ x.tieBreaker = y.tieBreaker

def RmwCommitSafe {Node : Type uRefNode} {Value : Type uRefValue}
    (st : RefState Node Value) (t : Timestamp Node) : Prop :=
  (forall y, st.committedWrites y -> RmwWriteGap t y) /\
  (forall y, st.committedRMWs y -> RmwSameVersionOK t y)

def WriteCommitSafe {Node : Type uRefNode} {Value : Type uRefValue}
    (st : RefState Node Value) (t : Timestamp Node) : Prop :=
  forall x, st.committedRMWs x -> RmwWriteGap x t

def RmwActiveSafe {Node : Type uRefNode} {Value : Type uRefValue}
    (st : RefState Node Value) (t : Timestamp Node) : Prop :=
  (forall a, st.live a -> st.nodeFlagRMW a = false ->
    RmwWriteGap t (st.nodeTS a)) /\
  (forall a, st.live a -> st.nodeFlagRMW a = true ->
    RmwSameVersionOK (st.nodeTS a) t) /\
  (forall m, st.rmsgs m -> m.epochID = st.epochID ->
    m.kind = OpKind.write -> RmwWriteGap t m.ts) /\
  (forall m, st.rmsgs m -> m.epochID = st.epochID ->
    m.kind = OpKind.rmw -> RmwSameVersionOK m.ts t)

def WriteActiveSafe {Node : Type uRefNode} {Value : Type uRefValue}
    (st : RefState Node Value) (t : Timestamp Node) : Prop :=
  (forall a, st.live a -> st.nodeFlagRMW a = true ->
    RmwWriteGap (st.nodeTS a) t) /\
  (forall m, st.rmsgs m -> m.epochID = st.epochID ->
    m.kind = OpKind.rmw -> RmwWriteGap m.ts t)

theorem version_le_of_tsLe {Node : Type uRefNode} (rank : NodeRank Node)
    {a b : Timestamp Node} :
    tsLe rank a b -> a.version <= b.version := by
  intro h
  unfold tsLe at h
  rcases h with hVersion | hVersion
  · omega
  · omega

theorem rmw_write_gap_to_succ_succ_of_le
    {Node : Type uRefNode} {rank : NodeRank Node}
    {rmw base : Timestamp Node} {writer : Node} :
    tsLe rank rmw base ->
      RmwWriteGap rmw (tsOf base.version.succ.succ writer) := by
  intro hLe
  have hVersionLe : rmw.version <= base.version :=
    version_le_of_tsLe rank hLe
  constructor
  · intro hBad
    simp [tsOf] at hBad
    omega
  · intro hBad
    simp [tsOf] at hBad
    omega

theorem rmw_write_gap_from_succ_of_le
    {Node : Type uRefNode} {rank : NodeRank Node}
    {base write : Timestamp Node} {writer : Node} :
    tsLe rank write base ->
      RmwWriteGap (tsOf base.version.succ writer) write := by
  intro hLe
  have hVersionLe : write.version <= base.version :=
    version_le_of_tsLe rank hLe
  constructor
  · intro hBad
    simp [tsOf] at hBad
    omega
  · intro hBad
    have hSubLe : write.version - 1 <= write.version := Nat.sub_le _ _
    simp [tsOf] at hBad
    omega

theorem rmw_same_version_ok_from_succ_of_le
    {Node : Type uRefNode} {rank : NodeRank Node}
    {base rmw : Timestamp Node} {writer : Node} :
    tsLe rank rmw base ->
      RmwSameVersionOK (tsOf base.version.succ writer) rmw := by
  intro hLe
  have hVersionLe : rmw.version <= base.version :=
    version_le_of_tsLe rank hLe
  left
  intro hBad
  simp [tsOf] at hBad
  omega

theorem send_vals_rmw_preserves_rmw_semantics
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {st : RefState Node Value} {n : Node}
    (hSem : HRSemanticsRMW st)
    (hFlag : st.nodeFlagRMW n = true)
    (hSafe : RmwCommitSafe st (st.nodeTS n)) :
    HRSemanticsRMW (sendValsPost st n) := by
  constructor
  · intro x y hx hy
    simp [sendValsPost, hFlag, addCommitted] at hx hy
    rcases hx with hOld | hNew
    · exact hSem.1 x y hOld hy
    · subst hNew
      exact hSafe.1 y hy
  · intro x y hx hy
    simp [sendValsPost, hFlag, addCommitted] at hx hy
    rcases hx with hOldX | hNewX
    · rcases hy with hOldY | hNewY
      · exact hSem.2 x y hOldX hOldY
      · subst hNewY
        rcases hSafe.2 x hOldX with hVersion | hTie
        · exact Or.inl (fun hEq => hVersion hEq.symm)
        · exact Or.inr hTie.symm
    · subst hNewX
      rcases hy with hOldY | hNewY
      · exact hSafe.2 y hOldY
      · subst hNewY
        exact Or.inr rfl

theorem send_vals_write_preserves_rmw_semantics
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {st : RefState Node Value} {n : Node}
    (hSem : HRSemanticsRMW st)
    (hFlag : st.nodeFlagRMW n = false)
    (hSafe : WriteCommitSafe st (st.nodeTS n)) :
    HRSemanticsRMW (sendValsPost st n) := by
  constructor
  · intro x y hx hy
    simp [sendValsPost, hFlag, addCommitted] at hx hy
    rcases hy with hOld | hNew
    · exact hSem.1 x y hx hOld
    · subst hNew
      exact hSafe x hx
  · intro x y hx hy
    simp [sendValsPost, hFlag, addCommitted] at hx hy
    exact hSem.2 x y hx hy

theorem o3_complete_rmw_preserves_rmw_semantics
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {st : RefState Node Value} {n : Node}
    (hSem : HRSemanticsRMW st)
    (hFlag : st.nodeFlagRMW n = true)
    (hSafe : RmwCommitSafe st (st.nodeTS n)) :
    HRSemanticsRMW (o3CompletePost st n) := by
  constructor
  · intro x y hx hy
    simp [o3CompletePost, hFlag, addCommitted] at hx hy
    rcases hx with hOld | hNew
    · exact hSem.1 x y hOld hy
    · subst hNew
      exact hSafe.1 y hy
  · intro x y hx hy
    simp [o3CompletePost, hFlag, addCommitted] at hx hy
    rcases hx with hOldX | hNewX
    · rcases hy with hOldY | hNewY
      · exact hSem.2 x y hOldX hOldY
      · subst hNewY
        rcases hSafe.2 x hOldX with hVersion | hTie
        · exact Or.inl (fun hEq => hVersion hEq.symm)
        · exact Or.inr hTie.symm
    · subst hNewX
      rcases hy with hOldY | hNewY
      · exact hSafe.2 y hOldY
      · subst hNewY
        exact Or.inr rfl

theorem o3_complete_write_preserves_rmw_semantics
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {st : RefState Node Value} {n : Node}
    (hSem : HRSemanticsRMW st)
    (hFlag : st.nodeFlagRMW n = false)
    (hSafe : WriteCommitSafe st (st.nodeTS n)) :
    HRSemanticsRMW (o3CompletePost st n) := by
  constructor
  · intro x y hx hy
    simp [o3CompletePost, hFlag, addCommitted] at hx hy
    rcases hy with hOld | hNew
    · exact hSem.1 x y hx hOld
    · subst hNew
      exact hSafe x hx
  · intro x y hx hy
    simp [o3CompletePost, hFlag, addCommitted] at hx hy
    exact hSem.2 x y hx hy

structure RefRmwInvariant
    {Node : Type uRefNode} {Value : Type uRefValue}
    (st : RefState Node Value) : Prop where
  semantic :
    HRSemanticsRMW st
  rmsg_epoch_le :
    forall m, st.rmsgs m -> m.epochID <= st.epochID
  rmsg_write_safe :
    forall m x, st.rmsgs m -> m.epochID = st.epochID ->
      m.kind = OpKind.write -> st.committedRMWs x -> RmwWriteGap x m.ts
  rmsg_rmw_write_safe :
    forall m y, st.rmsgs m -> m.epochID = st.epochID ->
      m.kind = OpKind.rmw -> st.committedWrites y -> RmwWriteGap m.ts y
  rmsg_rmw_unique :
    forall m x, st.rmsgs m -> m.epochID = st.epochID ->
      m.kind = OpKind.rmw -> st.committedRMWs x -> RmwSameVersionOK m.ts x
  live_write_safe :
    forall x n, st.committedRMWs x -> st.live n ->
      st.nodeFlagRMW n = false -> RmwWriteGap x (st.nodeTS n)
  live_rmw_write_safe :
    forall n y, st.live n -> st.nodeFlagRMW n = true ->
      st.committedWrites y -> RmwWriteGap (st.nodeTS n) y
  live_rmw_unique :
    forall n x, st.live n -> st.nodeFlagRMW n = true ->
      st.committedRMWs x -> RmwSameVersionOK (st.nodeTS n) x

theorem rmw_self_commit_safe
    {Node : Type uRefNode} {Value : Type uRefValue}
    {st : RefState Node Value} {n : Node}
    (hRmw : RefRmwInvariant st)
    (hLive : st.live n)
    (hFlag : st.nodeFlagRMW n = true) :
    RmwCommitSafe st (st.nodeTS n) := by
  constructor
  · intro y hy
    exact hRmw.live_rmw_write_safe n y hLive hFlag hy
  · intro x hx
    exact hRmw.live_rmw_unique n x hLive hFlag hx

theorem write_self_commit_safe
    {Node : Type uRefNode} {Value : Type uRefValue}
    {st : RefState Node Value} {n : Node}
    (hRmw : RefRmwInvariant st)
    (hLive : st.live n)
    (hFlag : st.nodeFlagRMW n = false) :
    WriteCommitSafe st (st.nodeTS n) := by
  intro x hx
  exact hRmw.live_write_safe x n hx hLive hFlag

theorem local_write_preserves_ref_rmw_invariant
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st : RefState Node Value} {n : Node} {v : Value}
    (hAgree : RefAgreementInvariant rank initTs st)
    (hRmw : RefRmwInvariant st)
    (hLive : st.live n) :
    RefRmwInvariant (writePost st n v) := by
  refine {
    semantic := by
      simpa [writePost, startUpdatePost] using hRmw.semantic
    rmsg_epoch_le := ?_
    rmsg_write_safe := ?_
    rmsg_rmw_write_safe := ?_
    rmsg_rmw_unique := ?_
    live_write_safe := ?_
    live_rmw_write_safe := ?_
    live_rmw_unique := ?_
  }
  · intro m hm
    simp [writePost, startUpdatePost, addRInv, sendRInvMsg] at hm
    rcases hm with hm | hm
    · exact hRmw.rmsg_epoch_le m hm
    · subst hm
      exact Nat.le_refl st.epochID
  · intro m x hm hEpoch hKind hx
    simp [writePost, startUpdatePost, addRInv, sendRInvMsg] at hm hEpoch hKind ⊢
    rcases hm with hm | hm
    · exact hRmw.rmsg_write_safe m x hm hEpoch hKind hx
    · subst hm
      have hOldLe : tsLe rank x (st.nodeTS n) :=
        hAgree.committed_live_advanced x n (Or.inl hx) hLive
      exact rmw_write_gap_to_succ_succ_of_le
        (rank := rank) (rmw := x) (base := st.nodeTS n)
        (writer := n) hOldLe
  · intro m y hm hEpoch hKind hy
    simp [writePost, startUpdatePost, addRInv, sendRInvMsg] at hm hEpoch hKind
    rcases hm with hm | hm
    · exact hRmw.rmsg_rmw_write_safe m y hm hEpoch hKind hy
    · subst hm
      simp at hKind
  · intro m x hm hEpoch hKind hx
    simp [writePost, startUpdatePost, addRInv, sendRInvMsg] at hm hEpoch hKind
    rcases hm with hm | hm
    · exact hRmw.rmsg_rmw_unique m x hm hEpoch hKind hx
    · subst hm
      simp at hKind
  · intro x a hx hLiveA hFlagA
    by_cases hA : a = n
    · subst a
      have hOldLe : tsLe rank x (st.nodeTS n) :=
        hAgree.committed_live_advanced x n (Or.inl hx) hLive
      simpa [writePost, startUpdatePost, replace] using
        rmw_write_gap_to_succ_succ_of_le
          (rank := rank) (rmw := x) (base := st.nodeTS n)
          (writer := n) hOldLe
    · have hOldLive : st.live a := by
        simpa [writePost, startUpdatePost] using hLiveA
      have hOldFlag : st.nodeFlagRMW a = false := by
        simpa [writePost, startUpdatePost, replace, hA] using hFlagA
      have hOldSafe : RmwWriteGap x (st.nodeTS a) :=
        hRmw.live_write_safe x a hx hOldLive hOldFlag
      simpa [writePost, startUpdatePost, replace, hA] using hOldSafe
  · intro a y hLiveA hFlagA hy
    by_cases hA : a = n
    · subst a
      simp [writePost, startUpdatePost, replace] at hFlagA
    · have hOldLive : st.live a := by
        simpa [writePost, startUpdatePost] using hLiveA
      have hOldFlag : st.nodeFlagRMW a = true := by
        simpa [writePost, startUpdatePost, replace, hA] using hFlagA
      have hOldSafe : RmwWriteGap (st.nodeTS a) y :=
        hRmw.live_rmw_write_safe a y hOldLive hOldFlag hy
      simpa [writePost, startUpdatePost, replace, hA] using hOldSafe
  · intro a x hLiveA hFlagA hx
    by_cases hA : a = n
    · subst a
      simp [writePost, startUpdatePost, replace] at hFlagA
    · have hOldLive : st.live a := by
        simpa [writePost, startUpdatePost] using hLiveA
      have hOldFlag : st.nodeFlagRMW a = true := by
        simpa [writePost, startUpdatePost, replace, hA] using hFlagA
      have hOldSafe : RmwSameVersionOK (st.nodeTS a) x :=
        hRmw.live_rmw_unique a x hOldLive hOldFlag hx
      simpa [writePost, startUpdatePost, replace, hA] using hOldSafe

theorem local_rmw_preserves_ref_rmw_invariant
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st : RefState Node Value} {n : Node} {v : Value}
    (hAgree : RefAgreementInvariant rank initTs st)
    (hRmw : RefRmwInvariant st)
    (hLive : st.live n) :
    RefRmwInvariant (rmwPost st n v) := by
  refine {
    semantic := by
      simpa [rmwPost, startUpdatePost] using hRmw.semantic
    rmsg_epoch_le := ?_
    rmsg_write_safe := ?_
    rmsg_rmw_write_safe := ?_
    rmsg_rmw_unique := ?_
    live_write_safe := ?_
    live_rmw_write_safe := ?_
    live_rmw_unique := ?_
  }
  · intro m hm
    simp [rmwPost, startUpdatePost, addRInv, sendRInvMsg] at hm
    rcases hm with hm | hm
    · exact hRmw.rmsg_epoch_le m hm
    · subst hm
      exact Nat.le_refl st.epochID
  · intro m x hm hEpoch hKind hx
    simp [rmwPost, startUpdatePost, addRInv, sendRInvMsg] at hm hEpoch hKind
    rcases hm with hm | hm
    · exact hRmw.rmsg_write_safe m x hm hEpoch hKind hx
    · subst hm
      simp at hKind
  · intro m y hm hEpoch hKind hy
    simp [rmwPost, startUpdatePost, addRInv, sendRInvMsg] at hm hEpoch hKind ⊢
    rcases hm with hm | hm
    · exact hRmw.rmsg_rmw_write_safe m y hm hEpoch hKind hy
    · subst hm
      have hOldLe : tsLe rank y (st.nodeTS n) :=
        hAgree.committed_live_advanced y n (Or.inr hy) hLive
      exact rmw_write_gap_from_succ_of_le
        (rank := rank) (base := st.nodeTS n) (write := y)
        (writer := n) hOldLe
  · intro m x hm hEpoch hKind hx
    simp [rmwPost, startUpdatePost, addRInv, sendRInvMsg] at hm hEpoch hKind ⊢
    rcases hm with hm | hm
    · exact hRmw.rmsg_rmw_unique m x hm hEpoch hKind hx
    · subst hm
      have hOldLe : tsLe rank x (st.nodeTS n) :=
        hAgree.committed_live_advanced x n (Or.inl hx) hLive
      exact rmw_same_version_ok_from_succ_of_le
        (rank := rank) (base := st.nodeTS n) (rmw := x)
        (writer := n) hOldLe
  · intro x a hx hLiveA hFlagA
    by_cases hA : a = n
    · subst a
      simp [rmwPost, startUpdatePost, replace] at hFlagA
    · have hOldLive : st.live a := by
        simpa [rmwPost, startUpdatePost] using hLiveA
      have hOldFlag : st.nodeFlagRMW a = false := by
        simpa [rmwPost, startUpdatePost, replace, hA] using hFlagA
      have hOldSafe : RmwWriteGap x (st.nodeTS a) :=
        hRmw.live_write_safe x a hx hOldLive hOldFlag
      simpa [rmwPost, startUpdatePost, replace, hA] using hOldSafe
  · intro a y hLiveA hFlagA hy
    by_cases hA : a = n
    · subst a
      have hOldLe : tsLe rank y (st.nodeTS n) :=
        hAgree.committed_live_advanced y n (Or.inr hy) hLive
      simpa [rmwPost, startUpdatePost, replace] using
        rmw_write_gap_from_succ_of_le
          (rank := rank) (base := st.nodeTS n) (write := y)
          (writer := n) hOldLe
    · have hOldLive : st.live a := by
        simpa [rmwPost, startUpdatePost] using hLiveA
      have hOldFlag : st.nodeFlagRMW a = true := by
        simpa [rmwPost, startUpdatePost, replace, hA] using hFlagA
      have hOldSafe : RmwWriteGap (st.nodeTS a) y :=
        hRmw.live_rmw_write_safe a y hOldLive hOldFlag hy
      simpa [rmwPost, startUpdatePost, replace, hA] using hOldSafe
  · intro a x hLiveA hFlagA hx
    by_cases hA : a = n
    · subst a
      have hOldLe : tsLe rank x (st.nodeTS n) :=
        hAgree.committed_live_advanced x n (Or.inl hx) hLive
      simpa [rmwPost, startUpdatePost, replace] using
        rmw_same_version_ok_from_succ_of_le
          (rank := rank) (base := st.nodeTS n) (rmw := x)
          (writer := n) hOldLe
    · have hOldLive : st.live a := by
        simpa [rmwPost, startUpdatePost] using hLiveA
      have hOldFlag : st.nodeFlagRMW a = true := by
        simpa [rmwPost, startUpdatePost, replace, hA] using hFlagA
      have hOldSafe : RmwSameVersionOK (st.nodeTS a) x :=
        hRmw.live_rmw_unique a x hOldLive hOldFlag hx
      simpa [rmwPost, startUpdatePost, replace, hA] using hOldSafe

theorem write_replay_preserves_ref_rmw_invariant
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {st : RefState Node Value} {n : Node}
    (hRmw : RefRmwInvariant st)
    (hLive : st.live n)
    (hFlag : st.nodeFlagRMW n = false) :
    RefRmwInvariant (writeReplayPost st n) := by
  refine {
    semantic := by simpa [writeReplayPost] using hRmw.semantic
    rmsg_epoch_le := ?_
    rmsg_write_safe := ?_
    rmsg_rmw_write_safe := ?_
    rmsg_rmw_unique := ?_
    live_write_safe := ?_
    live_rmw_write_safe := ?_
    live_rmw_unique := ?_
  }
  · intro m hm
    simp [writeReplayPost, addRInv, sendRInvMsg] at hm
    rcases hm with hm | hm
    · exact hRmw.rmsg_epoch_le m hm
    · subst hm
      exact Nat.le_refl st.epochID
  · intro m x hm hEpoch hKind hx
    simp [writeReplayPost, addRInv, sendRInvMsg] at hm hEpoch hKind
    rcases hm with hm | hm
    · exact hRmw.rmsg_write_safe m x hm hEpoch hKind hx
    · subst hm
      exact hRmw.live_write_safe x n hx hLive hFlag
  · intro m y hm hEpoch hKind hy
    simp [writeReplayPost, addRInv, sendRInvMsg] at hm hEpoch hKind
    rcases hm with hm | hm
    · exact hRmw.rmsg_rmw_write_safe m y hm hEpoch hKind hy
    · subst hm
      simp at hKind
  · intro m x hm hEpoch hKind hx
    simp [writeReplayPost, addRInv, sendRInvMsg] at hm hEpoch hKind
    rcases hm with hm | hm
    · exact hRmw.rmsg_rmw_unique m x hm hEpoch hKind hx
    · subst hm
      simp at hKind
  · intro x a hx hLiveA hFlagA
    exact hRmw.live_write_safe x a hx hLiveA hFlagA
  · intro a y hLiveA hFlagA hy
    exact hRmw.live_rmw_write_safe a y hLiveA hFlagA hy
  · intro a x hLiveA hFlagA hx
    exact hRmw.live_rmw_unique a x hLiveA hFlagA hx

theorem rmw_replay_preserves_ref_rmw_invariant
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {st : RefState Node Value} {n : Node}
    (hRmw : RefRmwInvariant st)
    (hLive : st.live n)
    (hFlag : st.nodeFlagRMW n = true) :
    RefRmwInvariant (rmwReplayPost st n) := by
  refine {
    semantic := by simpa [rmwReplayPost] using hRmw.semantic
    rmsg_epoch_le := ?_
    rmsg_write_safe := ?_
    rmsg_rmw_write_safe := ?_
    rmsg_rmw_unique := ?_
    live_write_safe := ?_
    live_rmw_write_safe := ?_
    live_rmw_unique := ?_
  }
  · intro m hm
    simp [rmwReplayPost, addRInv, sendRInvMsg] at hm
    rcases hm with hm | hm
    · exact hRmw.rmsg_epoch_le m hm
    · subst hm
      exact Nat.le_refl st.epochID
  · intro m x hm hEpoch hKind hx
    simp [rmwReplayPost, addRInv, sendRInvMsg] at hm hEpoch hKind
    rcases hm with hm | hm
    · exact hRmw.rmsg_write_safe m x hm hEpoch hKind hx
    · subst hm
      simp at hKind
  · intro m y hm hEpoch hKind hy
    simp [rmwReplayPost, addRInv, sendRInvMsg] at hm hEpoch hKind
    rcases hm with hm | hm
    · exact hRmw.rmsg_rmw_write_safe m y hm hEpoch hKind hy
    · subst hm
      exact hRmw.live_rmw_write_safe n y hLive hFlag hy
  · intro m x hm hEpoch hKind hx
    simp [rmwReplayPost, addRInv, sendRInvMsg] at hm hEpoch hKind
    rcases hm with hm | hm
    · exact hRmw.rmsg_rmw_unique m x hm hEpoch hKind hx
    · subst hm
      exact hRmw.live_rmw_unique n x hLive hFlag hx
  · intro x a hx hLiveA hFlagA
    exact hRmw.live_write_safe x a hx hLiveA hFlagA
  · intro a y hLiveA hFlagA hy
    exact hRmw.live_rmw_write_safe a y hLiveA hFlagA hy
  · intro a x hLiveA hFlagA hx
    exact hRmw.live_rmw_unique a x hLiveA hFlagA hx

theorem receive_ack_preserves_ref_rmw_invariant
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {st : RefState Node Value} {n sender : Node}
    (hRmw : RefRmwInvariant st) :
    RefRmwInvariant (receiveAckPost st n sender) := by
  refine {
    semantic := by simpa [receiveAckPost] using hRmw.semantic
    rmsg_epoch_le := ?_
    rmsg_write_safe := ?_
    rmsg_rmw_write_safe := ?_
    rmsg_rmw_unique := ?_
    live_write_safe := ?_
    live_rmw_write_safe := ?_
    live_rmw_unique := ?_
  }
  · exact hRmw.rmsg_epoch_le
  · exact hRmw.rmsg_write_safe
  · exact hRmw.rmsg_rmw_write_safe
  · exact hRmw.rmsg_rmw_unique
  · intro x a hx hLiveA hFlagA
    exact hRmw.live_write_safe x a hx hLiveA hFlagA
  · intro a y hLiveA hFlagA hy
    exact hRmw.live_rmw_write_safe a y hLiveA hFlagA hy
  · intro a x hLiveA hFlagA hx
    exact hRmw.live_rmw_unique a x hLiveA hFlagA hx

theorem receive_val_preserves_ref_rmw_invariant
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {st : RefState Node Value} {n : Node} {t : Timestamp Node}
    (hRmw : RefRmwInvariant st) :
    RefRmwInvariant (receiveValPost st n t) := by
  unfold receiveValPost
  split
  · refine {
      semantic := by simpa using hRmw.semantic
      rmsg_epoch_le := ?_
      rmsg_write_safe := ?_
      rmsg_rmw_write_safe := ?_
      rmsg_rmw_unique := ?_
      live_write_safe := ?_
      live_rmw_write_safe := ?_
      live_rmw_unique := ?_
    }
    · exact hRmw.rmsg_epoch_le
    · exact hRmw.rmsg_write_safe
    · exact hRmw.rmsg_rmw_write_safe
    · exact hRmw.rmsg_rmw_unique
    · intro x a hx hLiveA hFlagA
      exact hRmw.live_write_safe x a hx hLiveA hFlagA
    · intro a y hLiveA hFlagA hy
      exact hRmw.live_rmw_write_safe a y hLiveA hFlagA hy
    · intro a x hLiveA hFlagA hx
      exact hRmw.live_rmw_unique a x hLiveA hFlagA hx
  · exact hRmw

theorem follower_replay_preserves_ref_rmw_invariant
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {st : RefState Node Value} {n : Node}
    (hRmw : RefRmwInvariant st)
    (hLive : st.live n) :
    RefRmwInvariant (followerReplayPost st n) := by
  refine {
    semantic := by simpa [followerReplayPost] using hRmw.semantic
    rmsg_epoch_le := ?_
    rmsg_write_safe := ?_
    rmsg_rmw_write_safe := ?_
    rmsg_rmw_unique := ?_
    live_write_safe := ?_
    live_rmw_write_safe := ?_
    live_rmw_unique := ?_
  }
  · intro m hm
    simp [followerReplayPost, addRInv, sendRInvMsg] at hm
    rcases hm with hm | hm
    · exact hRmw.rmsg_epoch_le m hm
    · subst hm
      exact Nat.le_refl st.epochID
  · intro m x hm hEpoch hKind hx
    simp [followerReplayPost, addRInv, sendRInvMsg] at hm hEpoch hKind
    rcases hm with hm | hm
    · exact hRmw.rmsg_write_safe m x hm hEpoch hKind hx
    · subst hm
      by_cases hFlag : st.nodeFlagRMW n
      · simp [hFlag] at hKind
      · have hFlagFalse : st.nodeFlagRMW n = false := by
          simpa using hFlag
        exact hRmw.live_write_safe x n hx hLive hFlagFalse
  · intro m y hm hEpoch hKind hy
    simp [followerReplayPost, addRInv, sendRInvMsg] at hm hEpoch hKind
    rcases hm with hm | hm
    · exact hRmw.rmsg_rmw_write_safe m y hm hEpoch hKind hy
    · subst hm
      by_cases hFlag : st.nodeFlagRMW n
      · have hFlagTrue : st.nodeFlagRMW n = true := by
          simpa using hFlag
        exact hRmw.live_rmw_write_safe n y hLive hFlagTrue hy
      · simp [hFlag] at hKind
  · intro m x hm hEpoch hKind hx
    simp [followerReplayPost, addRInv, sendRInvMsg] at hm hEpoch hKind
    rcases hm with hm | hm
    · exact hRmw.rmsg_rmw_unique m x hm hEpoch hKind hx
    · subst hm
      by_cases hFlag : st.nodeFlagRMW n
      · have hFlagTrue : st.nodeFlagRMW n = true := by
          simpa using hFlag
        exact hRmw.live_rmw_unique n x hLive hFlagTrue hx
      · simp [hFlag] at hKind
  · intro x a hx hLiveA hFlagA
    exact hRmw.live_write_safe x a hx hLiveA hFlagA
  · intro a y hLiveA hFlagA hy
    exact hRmw.live_rmw_write_safe a y hLiveA hFlagA hy
  · intro a x hLiveA hFlagA hx
    exact hRmw.live_rmw_unique a x hLiveA hFlagA hx

theorem node_failure_preserves_ref_rmw_invariant
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {st : RefState Node Value} {n : Node}
    (hRmw : RefRmwInvariant st) :
    RefRmwInvariant (nodeFailurePost st n) := by
  refine {
    semantic := by simpa [nodeFailurePost] using hRmw.semantic
    rmsg_epoch_le := ?_
    rmsg_write_safe := ?_
    rmsg_rmw_write_safe := ?_
    rmsg_rmw_unique := ?_
    live_write_safe := ?_
    live_rmw_write_safe := ?_
    live_rmw_unique := ?_
  }
  · intro m hm
    exact Nat.le_succ_of_le (hRmw.rmsg_epoch_le m (by
      simpa [nodeFailurePost] using hm))
  · intro m x hm hEpoch hKind hx
    have hOldMsg : st.rmsgs m := by
      simpa [nodeFailurePost] using hm
    have hOldLe : m.epochID <= st.epochID := hRmw.rmsg_epoch_le m hOldMsg
    simp [nodeFailurePost] at hEpoch
    omega
  · intro m y hm hEpoch hKind hy
    have hOldMsg : st.rmsgs m := by
      simpa [nodeFailurePost] using hm
    have hOldLe : m.epochID <= st.epochID := hRmw.rmsg_epoch_le m hOldMsg
    simp [nodeFailurePost] at hEpoch
    omega
  · intro m x hm hEpoch hKind hx
    have hOldMsg : st.rmsgs m := by
      simpa [nodeFailurePost] using hm
    have hOldLe : m.epochID <= st.epochID := hRmw.rmsg_epoch_le m hOldMsg
    simp [nodeFailurePost] at hEpoch
    omega
  · intro x a hx hLiveA hFlagA
    have hOldLiveAndNe : st.live a ∧ a ≠ n := by
      simpa [nodeFailurePost, removeLive] using hLiveA
    exact hRmw.live_write_safe x a hx hOldLiveAndNe.1 hFlagA
  · intro a y hLiveA hFlagA hy
    have hOldLiveAndNe : st.live a ∧ a ≠ n := by
      simpa [nodeFailurePost, removeLive] using hLiveA
    exact hRmw.live_rmw_write_safe a y hOldLiveAndNe.1 hFlagA hy
  · intro a x hLiveA hFlagA hx
    have hOldLiveAndNe : st.live a ∧ a ≠ n := by
      simpa [nodeFailurePost, removeLive] using hLiveA
    exact hRmw.live_rmw_unique a x hOldLiveAndNe.1 hFlagA hx

theorem o3_observe_preserves_ref_rmw_invariant
    {Node : Type uRefNode} {Value : Type uRefValue}
    {st : RefState Node Value} {n c : Node} {t : Timestamp Node}
    (hRmw : RefRmwInvariant st) :
    RefRmwInvariant (o3ObservePost st n c t) := by
  refine {
    semantic := by simpa [o3ObservePost] using hRmw.semantic
    rmsg_epoch_le := ?_
    rmsg_write_safe := ?_
    rmsg_rmw_write_safe := ?_
    rmsg_rmw_unique := ?_
    live_write_safe := ?_
    live_rmw_write_safe := ?_
    live_rmw_unique := ?_
  }
  · exact hRmw.rmsg_epoch_le
  · exact hRmw.rmsg_write_safe
  · exact hRmw.rmsg_rmw_write_safe
  · exact hRmw.rmsg_rmw_unique
  · intro x a hx hLiveA hFlagA
    exact hRmw.live_write_safe x a hx hLiveA hFlagA
  · intro a y hLiveA hFlagA hy
    exact hRmw.live_rmw_write_safe a y hLiveA hFlagA hy
  · intro a x hLiveA hFlagA hx
    exact hRmw.live_rmw_unique a x hLiveA hFlagA hx

theorem receive_write_inv_preserves_ref_rmw_invariant
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {st : RefState Node Value} {n : Node}
    {m : RInvMsg Node Value}
    (hRmw : RefRmwInvariant st)
    (hMsg : st.rmsgs m)
    (hEpoch : m.epochID = st.epochID)
    (hKind : m.kind = OpKind.write) :
    RefRmwInvariant (receiveWriteInvPost rank st n m) := by
  unfold receiveWriteInvPost
  split
  · rename_i hGreater
    refine {
      semantic := by simpa using hRmw.semantic
      rmsg_epoch_le := ?_
      rmsg_write_safe := ?_
      rmsg_rmw_write_safe := ?_
      rmsg_rmw_unique := ?_
      live_write_safe := ?_
      live_rmw_write_safe := ?_
      live_rmw_unique := ?_
    }
    · exact hRmw.rmsg_epoch_le
    · exact hRmw.rmsg_write_safe
    · exact hRmw.rmsg_rmw_write_safe
    · exact hRmw.rmsg_rmw_unique
    · intro x a hx hLiveA hFlagA
      by_cases hA : a = n
      · subst a
        simpa [replace] using hRmw.rmsg_write_safe m x hMsg hEpoch hKind hx
      · have hOldFlag : st.nodeFlagRMW a = false := by
          simpa [replace, hA] using hFlagA
        simpa [replace, hA] using
          hRmw.live_write_safe x a hx hLiveA hOldFlag
    · intro a y hLiveA hFlagA hy
      by_cases hA : a = n
      · subst a
        simp [replace] at hFlagA
      · have hOldFlag : st.nodeFlagRMW a = true := by
          simpa [replace, hA] using hFlagA
        simpa [replace, hA] using
          hRmw.live_rmw_write_safe a y hLiveA hOldFlag hy
    · intro a x hLiveA hFlagA hx
      by_cases hA : a = n
      · subst a
        simp [replace] at hFlagA
      · have hOldFlag : st.nodeFlagRMW a = true := by
          simpa [replace, hA] using hFlagA
        simpa [replace, hA] using
          hRmw.live_rmw_unique a x hLiveA hOldFlag hx
  · rename_i hNotGreater
    refine {
      semantic := by simpa using hRmw.semantic
      rmsg_epoch_le := hRmw.rmsg_epoch_le
      rmsg_write_safe := hRmw.rmsg_write_safe
      rmsg_rmw_write_safe := hRmw.rmsg_rmw_write_safe
      rmsg_rmw_unique := hRmw.rmsg_rmw_unique
      live_write_safe := ?_
      live_rmw_write_safe := ?_
      live_rmw_unique := ?_
    }
    · intro x a hx hLiveA hFlagA
      exact hRmw.live_write_safe x a hx hLiveA hFlagA
    · intro a y hLiveA hFlagA hy
      exact hRmw.live_rmw_write_safe a y hLiveA hFlagA hy
    · intro a x hLiveA hFlagA hx
      exact hRmw.live_rmw_unique a x hLiveA hFlagA hx

theorem receive_rmw_inv_preserves_ref_rmw_invariant
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {st : RefState Node Value} {n : Node}
    {m : RInvMsg Node Value}
    (hRmw : RefRmwInvariant st)
    (hLive : st.live n)
    (hMsg : st.rmsgs m)
    (hEpoch : m.epochID = st.epochID)
    (hKind : m.kind = OpKind.rmw) :
    RefRmwInvariant (receiveRmwInvPost rank st n m) := by
  unfold receiveRmwInvPost
  split
  · rename_i hGreater
    refine {
      semantic := by simpa using hRmw.semantic
      rmsg_epoch_le := ?_
      rmsg_write_safe := ?_
      rmsg_rmw_write_safe := ?_
      rmsg_rmw_unique := ?_
      live_write_safe := ?_
      live_rmw_write_safe := ?_
      live_rmw_unique := ?_
    }
    · exact hRmw.rmsg_epoch_le
    · exact hRmw.rmsg_write_safe
    · exact hRmw.rmsg_rmw_write_safe
    · exact hRmw.rmsg_rmw_unique
    · intro x a hx hLiveA hFlagA
      by_cases hA : a = n
      · subst a
        simp [replace] at hFlagA
      · have hOldFlag : st.nodeFlagRMW a = false := by
          simpa [replace, hA] using hFlagA
        simpa [replace, hA] using
          hRmw.live_write_safe x a hx hLiveA hOldFlag
    · intro a y hLiveA hFlagA hy
      by_cases hA : a = n
      · subst a
        simpa [replace] using hRmw.rmsg_rmw_write_safe m y hMsg hEpoch hKind hy
      · have hOldFlag : st.nodeFlagRMW a = true := by
          simpa [replace, hA] using hFlagA
        simpa [replace, hA] using
          hRmw.live_rmw_write_safe a y hLiveA hOldFlag hy
    · intro a x hLiveA hFlagA hx
      by_cases hA : a = n
      · subst a
        simpa [replace] using hRmw.rmsg_rmw_unique m x hMsg hEpoch hKind hx
      · have hOldFlag : st.nodeFlagRMW a = true := by
          simpa [replace, hA] using hFlagA
        simpa [replace, hA] using
          hRmw.live_rmw_unique a x hLiveA hOldFlag hx
  · rename_i hNotGreater
    split
    · rename_i hEq
      refine {
        semantic := by simpa [hEq] using hRmw.semantic
        rmsg_epoch_le := hRmw.rmsg_epoch_le
        rmsg_write_safe := hRmw.rmsg_write_safe
        rmsg_rmw_write_safe := hRmw.rmsg_rmw_write_safe
        rmsg_rmw_unique := hRmw.rmsg_rmw_unique
        live_write_safe := ?_
        live_rmw_write_safe := ?_
        live_rmw_unique := ?_
      }
      · intro x a hx hLiveA hFlagA
        exact hRmw.live_write_safe x a hx hLiveA hFlagA
      · intro a y hLiveA hFlagA hy
        exact hRmw.live_rmw_write_safe a y hLiveA hFlagA hy
      · intro a x hLiveA hFlagA hx
        exact hRmw.live_rmw_unique a x hLiveA hFlagA hx
    · rename_i hEq
      refine {
        semantic := by simpa [hEq] using hRmw.semantic
        rmsg_epoch_le := ?_
        rmsg_write_safe := ?_
        rmsg_rmw_write_safe := ?_
        rmsg_rmw_unique := ?_
        live_write_safe := ?_
        live_rmw_write_safe := ?_
        live_rmw_unique := ?_
      }
      · intro msg hMsgPost
        simp [hEq, addRInv, sendRInvMsg] at hMsgPost
        rcases hMsgPost with hOld | hNew
        · exact hRmw.rmsg_epoch_le msg hOld
        · subst hNew
          exact Nat.le_refl st.epochID
      · intro msg x hMsgPost hEpochPost hKindPost hx
        simp [hEq, addRInv, sendRInvMsg] at hMsgPost hEpochPost hKindPost
        rcases hMsgPost with hOld | hNew
        · exact hRmw.rmsg_write_safe msg x hOld hEpochPost hKindPost hx
        · subst hNew
          by_cases hFlag : st.nodeFlagRMW n
          · simp [hFlag] at hKindPost
          · have hFlagFalse : st.nodeFlagRMW n = false := by
              simpa using hFlag
            exact hRmw.live_write_safe x n hx hLive hFlagFalse
      · intro msg y hMsgPost hEpochPost hKindPost hy
        simp [hEq, addRInv, sendRInvMsg] at hMsgPost hEpochPost hKindPost
        rcases hMsgPost with hOld | hNew
        · exact hRmw.rmsg_rmw_write_safe msg y hOld hEpochPost hKindPost hy
        · subst hNew
          by_cases hFlag : st.nodeFlagRMW n
          · have hFlagTrue : st.nodeFlagRMW n = true := by
              simpa using hFlag
            exact hRmw.live_rmw_write_safe n y hLive hFlagTrue hy
          · simp [hFlag] at hKindPost
      · intro msg x hMsgPost hEpochPost hKindPost hx
        simp [hEq, addRInv, sendRInvMsg] at hMsgPost hEpochPost hKindPost
        rcases hMsgPost with hOld | hNew
        · exact hRmw.rmsg_rmw_unique msg x hOld hEpochPost hKindPost hx
        · subst hNew
          by_cases hFlag : st.nodeFlagRMW n
          · have hFlagTrue : st.nodeFlagRMW n = true := by
              simpa using hFlag
            exact hRmw.live_rmw_unique n x hLive hFlagTrue hx
          · simp [hFlag] at hKindPost
      · intro x a hx hLiveA hFlagA
        exact hRmw.live_write_safe x a hx hLiveA hFlagA
      · intro a y hLiveA hFlagA hy
        exact hRmw.live_rmw_write_safe a y hLiveA hFlagA hy
      · intro a x hLiveA hFlagA hx
        exact hRmw.live_rmw_unique a x hLiveA hFlagA hx

theorem send_vals_rmw_preserves_ref_rmw_invariant
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {st : RefState Node Value} {n : Node}
    (hRmw : RefRmwInvariant st)
    (hLive : st.live n)
    (hFlag : st.nodeFlagRMW n = true)
    (hActive : RmwActiveSafe st (st.nodeTS n)) :
    RefRmwInvariant (sendValsPost st n) := by
  refine {
    semantic := send_vals_rmw_preserves_rmw_semantics
      hRmw.semantic hFlag (rmw_self_commit_safe hRmw hLive hFlag)
    rmsg_epoch_le := ?_
    rmsg_write_safe := ?_
    rmsg_rmw_write_safe := ?_
    rmsg_rmw_unique := ?_
    live_write_safe := ?_
    live_rmw_write_safe := ?_
    live_rmw_unique := ?_
  }
  · intro m hm
    exact hRmw.rmsg_epoch_le m (by
      simpa [sendValsPost] using hm)
  · intro m x hm hEpoch hKind hx
    have hMsgOld : st.rmsgs m := by
      simpa [sendValsPost] using hm
    simp [sendValsPost, hFlag, addCommitted] at hx
    rcases hx with hxOld | hxNew
    · exact hRmw.rmsg_write_safe m x hMsgOld hEpoch hKind hxOld
    · subst hxNew
      exact hActive.2.2.1 m hMsgOld hEpoch hKind
  · intro m y hm hEpoch hKind hy
    exact hRmw.rmsg_rmw_write_safe m y
      (by simpa [sendValsPost] using hm) hEpoch hKind (by
        simpa [sendValsPost, hFlag] using hy)
  · intro m x hm hEpoch hKind hx
    have hMsgOld : st.rmsgs m := by
      simpa [sendValsPost] using hm
    simp [sendValsPost, hFlag, addCommitted] at hx
    rcases hx with hxOld | hxNew
    · exact hRmw.rmsg_rmw_unique m x hMsgOld hEpoch hKind hxOld
    · subst hxNew
      exact hActive.2.2.2 m hMsgOld hEpoch hKind
  · intro x a hx hLiveA hFlagA
    have hLiveOld : st.live a := by
      simpa [sendValsPost] using hLiveA
    have hFlagOld : st.nodeFlagRMW a = false := by
      simpa [sendValsPost] using hFlagA
    simp [sendValsPost, hFlag, addCommitted] at hx
    rcases hx with hxOld | hxNew
    · exact hRmw.live_write_safe x a hxOld hLiveOld hFlagOld
    · subst hxNew
      exact hActive.1 a hLiveOld hFlagOld
  · intro a y hLiveA hFlagA hy
    exact hRmw.live_rmw_write_safe a y
      (by simpa [sendValsPost] using hLiveA)
      (by simpa [sendValsPost] using hFlagA)
      (by simpa [sendValsPost, hFlag] using hy)
  · intro a x hLiveA hFlagA hx
    have hLiveOld : st.live a := by
      simpa [sendValsPost] using hLiveA
    have hFlagOld : st.nodeFlagRMW a = true := by
      simpa [sendValsPost] using hFlagA
    simp [sendValsPost, hFlag, addCommitted] at hx
    rcases hx with hxOld | hxNew
    · exact hRmw.live_rmw_unique a x hLiveOld hFlagOld hxOld
    · subst hxNew
      exact hActive.2.1 a hLiveOld hFlagOld

theorem send_vals_write_preserves_ref_rmw_invariant
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {st : RefState Node Value} {n : Node}
    (hRmw : RefRmwInvariant st)
    (hLive : st.live n)
    (hFlag : st.nodeFlagRMW n = false)
    (hActive : WriteActiveSafe st (st.nodeTS n)) :
    RefRmwInvariant (sendValsPost st n) := by
  refine {
    semantic := send_vals_write_preserves_rmw_semantics
      hRmw.semantic hFlag (write_self_commit_safe hRmw hLive hFlag)
    rmsg_epoch_le := ?_
    rmsg_write_safe := ?_
    rmsg_rmw_write_safe := ?_
    rmsg_rmw_unique := ?_
    live_write_safe := ?_
    live_rmw_write_safe := ?_
    live_rmw_unique := ?_
  }
  · intro m hm
    exact hRmw.rmsg_epoch_le m (by
      simpa [sendValsPost] using hm)
  · intro m x hm hEpoch hKind hx
    exact hRmw.rmsg_write_safe m x
      (by simpa [sendValsPost] using hm) hEpoch hKind
      (by simpa [sendValsPost, hFlag] using hx)
  · intro m y hm hEpoch hKind hy
    have hMsgOld : st.rmsgs m := by
      simpa [sendValsPost] using hm
    simp [sendValsPost, hFlag, addCommitted] at hy
    rcases hy with hyOld | hyNew
    · exact hRmw.rmsg_rmw_write_safe m y hMsgOld hEpoch hKind hyOld
    · subst hyNew
      exact hActive.2 m hMsgOld hEpoch hKind
  · intro m x hm hEpoch hKind hx
    exact hRmw.rmsg_rmw_unique m x
      (by simpa [sendValsPost] using hm) hEpoch hKind
      (by simpa [sendValsPost, hFlag] using hx)
  · intro x a hx hLiveA hFlagA
    exact hRmw.live_write_safe x a
      (by simpa [sendValsPost, hFlag] using hx)
      (by simpa [sendValsPost] using hLiveA)
      (by simpa [sendValsPost] using hFlagA)
  · intro a y hLiveA hFlagA hy
    have hLiveOld : st.live a := by
      simpa [sendValsPost] using hLiveA
    have hFlagOld : st.nodeFlagRMW a = true := by
      simpa [sendValsPost] using hFlagA
    simp [sendValsPost, hFlag, addCommitted] at hy
    rcases hy with hyOld | hyNew
    · exact hRmw.live_rmw_write_safe a y hLiveOld hFlagOld hyOld
    · subst hyNew
      exact hActive.1 a hLiveOld hFlagOld
  · intro a x hLiveA hFlagA hx
    exact hRmw.live_rmw_unique a x
      (by simpa [sendValsPost] using hLiveA)
      (by simpa [sendValsPost] using hFlagA)
      (by simpa [sendValsPost, hFlag] using hx)

theorem o3_complete_rmw_preserves_ref_rmw_invariant
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {st : RefState Node Value} {n : Node}
    (hRmw : RefRmwInvariant st)
    (hLive : st.live n)
    (hFlag : st.nodeFlagRMW n = true)
    (hActive : RmwActiveSafe st (st.nodeTS n)) :
    RefRmwInvariant (o3CompletePost st n) := by
  refine {
    semantic := o3_complete_rmw_preserves_rmw_semantics
      hRmw.semantic hFlag (rmw_self_commit_safe hRmw hLive hFlag)
    rmsg_epoch_le := ?_
    rmsg_write_safe := ?_
    rmsg_rmw_write_safe := ?_
    rmsg_rmw_unique := ?_
    live_write_safe := ?_
    live_rmw_write_safe := ?_
    live_rmw_unique := ?_
  }
  · intro m hm
    exact hRmw.rmsg_epoch_le m (by
      simpa [o3CompletePost] using hm)
  · intro m x hm hEpoch hKind hx
    have hMsgOld : st.rmsgs m := by
      simpa [o3CompletePost] using hm
    simp [o3CompletePost, hFlag, addCommitted] at hx
    rcases hx with hxOld | hxNew
    · exact hRmw.rmsg_write_safe m x hMsgOld hEpoch hKind hxOld
    · subst hxNew
      exact hActive.2.2.1 m hMsgOld hEpoch hKind
  · intro m y hm hEpoch hKind hy
    exact hRmw.rmsg_rmw_write_safe m y
      (by simpa [o3CompletePost] using hm) hEpoch hKind (by
        simpa [o3CompletePost, hFlag] using hy)
  · intro m x hm hEpoch hKind hx
    have hMsgOld : st.rmsgs m := by
      simpa [o3CompletePost] using hm
    simp [o3CompletePost, hFlag, addCommitted] at hx
    rcases hx with hxOld | hxNew
    · exact hRmw.rmsg_rmw_unique m x hMsgOld hEpoch hKind hxOld
    · subst hxNew
      exact hActive.2.2.2 m hMsgOld hEpoch hKind
  · intro x a hx hLiveA hFlagA
    have hLiveOld : st.live a := by
      simpa [o3CompletePost] using hLiveA
    have hFlagOld : st.nodeFlagRMW a = false := by
      simpa [o3CompletePost] using hFlagA
    simp [o3CompletePost, hFlag, addCommitted] at hx
    rcases hx with hxOld | hxNew
    · exact hRmw.live_write_safe x a hxOld hLiveOld hFlagOld
    · subst hxNew
      exact hActive.1 a hLiveOld hFlagOld
  · intro a y hLiveA hFlagA hy
    exact hRmw.live_rmw_write_safe a y
      (by simpa [o3CompletePost] using hLiveA)
      (by simpa [o3CompletePost] using hFlagA)
      (by simpa [o3CompletePost, hFlag] using hy)
  · intro a x hLiveA hFlagA hx
    have hLiveOld : st.live a := by
      simpa [o3CompletePost] using hLiveA
    have hFlagOld : st.nodeFlagRMW a = true := by
      simpa [o3CompletePost] using hFlagA
    simp [o3CompletePost, hFlag, addCommitted] at hx
    rcases hx with hxOld | hxNew
    · exact hRmw.live_rmw_unique a x hLiveOld hFlagOld hxOld
    · subst hxNew
      exact hActive.2.1 a hLiveOld hFlagOld

theorem o3_complete_write_preserves_ref_rmw_invariant
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {st : RefState Node Value} {n : Node}
    (hRmw : RefRmwInvariant st)
    (hLive : st.live n)
    (hFlag : st.nodeFlagRMW n = false)
    (hActive : WriteActiveSafe st (st.nodeTS n)) :
    RefRmwInvariant (o3CompletePost st n) := by
  refine {
    semantic := o3_complete_write_preserves_rmw_semantics
      hRmw.semantic hFlag (write_self_commit_safe hRmw hLive hFlag)
    rmsg_epoch_le := ?_
    rmsg_write_safe := ?_
    rmsg_rmw_write_safe := ?_
    rmsg_rmw_unique := ?_
    live_write_safe := ?_
    live_rmw_write_safe := ?_
    live_rmw_unique := ?_
  }
  · intro m hm
    exact hRmw.rmsg_epoch_le m (by
      simpa [o3CompletePost] using hm)
  · intro m x hm hEpoch hKind hx
    exact hRmw.rmsg_write_safe m x
      (by simpa [o3CompletePost] using hm) hEpoch hKind
      (by simpa [o3CompletePost, hFlag] using hx)
  · intro m y hm hEpoch hKind hy
    have hMsgOld : st.rmsgs m := by
      simpa [o3CompletePost] using hm
    simp [o3CompletePost, hFlag, addCommitted] at hy
    rcases hy with hyOld | hyNew
    · exact hRmw.rmsg_rmw_write_safe m y hMsgOld hEpoch hKind hyOld
    · subst hyNew
      exact hActive.2 m hMsgOld hEpoch hKind
  · intro m x hm hEpoch hKind hx
    exact hRmw.rmsg_rmw_unique m x
      (by simpa [o3CompletePost] using hm) hEpoch hKind
      (by simpa [o3CompletePost, hFlag] using hx)
  · intro x a hx hLiveA hFlagA
    exact hRmw.live_write_safe x a
      (by simpa [o3CompletePost, hFlag] using hx)
      (by simpa [o3CompletePost] using hLiveA)
      (by simpa [o3CompletePost] using hFlagA)
  · intro a y hLiveA hFlagA hy
    have hLiveOld : st.live a := by
      simpa [o3CompletePost] using hLiveA
    have hFlagOld : st.nodeFlagRMW a = true := by
      simpa [o3CompletePost] using hFlagA
    simp [o3CompletePost, hFlag, addCommitted] at hy
    rcases hy with hyOld | hyNew
    · exact hRmw.live_rmw_write_safe a y hLiveOld hFlagOld hyOld
    · subst hyNew
      exact hActive.1 a hLiveOld hFlagOld
  · intro a x hLiveA hFlagA hx
    exact hRmw.live_rmw_unique a x
      (by simpa [o3CompletePost] using hLiveA)
      (by simpa [o3CompletePost] using hFlagA)
      (by simpa [o3CompletePost, hFlag] using hx)

theorem ref_rmw_preserved_given_active
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st st' : RefState Node Value} {label : RefLabel Node}
    (hAgree : RefAgreementInvariant rank initTs st)
    (hRmw : RefRmwInvariant st)
    (hStep : HRNext rank st label st')
    (hSendRmwActive :
      forall n, st.live n -> st.nodeFlagRMW n = true ->
        stateCanSendVal st n -> receivedAllAcks st n ->
        RmwActiveSafe st (st.nodeTS n))
    (hSendWriteActive :
      forall n, st.live n -> st.nodeFlagRMW n = false ->
        stateCanSendVal st n -> receivedAllAcks st n ->
        WriteActiveSafe st (st.nodeTS n))
    (hO3RmwActive :
      forall n c, st.live n -> st.nodeFlagRMW n = true ->
        st.nodeState n ≠ HState.hs_valid ->
        st.o3Quorum n c (st.nodeTS n) st.epochID ->
        RmwActiveSafe st (st.nodeTS n))
    (hO3WriteActive :
      forall n c, st.live n -> st.nodeFlagRMW n = false ->
        st.nodeState n ≠ HState.hs_valid ->
        st.o3Quorum n c (st.nodeTS n) st.epochID ->
        WriteActiveSafe st (st.nodeTS n)) :
    RefRmwInvariant st' := by
  cases hStep with
  | hr_write hLive hState =>
      exact local_write_preserves_ref_rmw_invariant hAgree hRmw hLive
  | hr_rmw hLive hState =>
      exact local_rmw_preserves_ref_rmw_invariant hAgree hRmw hLive
  | hr_write_replay hLive hState hEpoch hMissing hFlag =>
      exact write_replay_preserves_ref_rmw_invariant hRmw hLive hFlag
  | hr_rmw_replay hLive hState hEpoch hMissing hFlag =>
      exact rmw_replay_preserves_ref_rmw_invariant hRmw hLive hFlag
  | hr_rcv_ack hLive hMsg hEpoch hSender hFresh hTs hState =>
      exact receive_ack_preserves_ref_rmw_invariant hRmw
  | hr_send_vals_rmw hLive hFlag hState hAll =>
      exact send_vals_rmw_preserves_ref_rmw_invariant hRmw hLive hFlag
        (hSendRmwActive _ hLive hFlag hState hAll)
  | hr_send_vals_write hLive hFlag hState hAll =>
      exact send_vals_write_preserves_ref_rmw_invariant hRmw hLive hFlag
        (hSendWriteActive _ hLive hFlag hState hAll)
  | hr_rcv_write_inv hLive hMsg hEpoch hSender hKind =>
      exact receive_write_inv_preserves_ref_rmw_invariant hRmw hMsg hEpoch hKind
  | hr_rcv_rmw_inv hLive hMsg hEpoch hSender hKind =>
      exact receive_rmw_inv_preserves_ref_rmw_invariant hRmw hLive hMsg hEpoch hKind
  | hr_rcv_val hLive hVal hState =>
      exact receive_val_preserves_ref_rmw_invariant hRmw
  | hr_follower_replay hLive hState hDead =>
      exact follower_replay_preserves_ref_rmw_invariant hRmw hLive
  | hr_node_failure hLive =>
      exact node_failure_preserves_ref_rmw_invariant hRmw
  | hr_o3_observe hLive hQuorum =>
      exact o3_observe_preserves_ref_rmw_invariant hRmw
  | hr_o3_complete hLive hState hQuorum =>
      rename_i n c
      by_cases hFlag : st.nodeFlagRMW n
      · exact o3_complete_rmw_preserves_ref_rmw_invariant hRmw hLive hFlag
          (hO3RmwActive n c hLive hFlag hState hQuorum)
      · have hFlagFalse : st.nodeFlagRMW n = false := by
          simpa using hFlag
        exact o3_complete_write_preserves_ref_rmw_invariant hRmw hLive hFlagFalse
          (hO3WriteActive n c hLive hFlagFalse hState hQuorum)

structure RefSafety {Node : Type uRefNode} {Value : Type uRefValue}
    (st : RefState Node Value) : Prop where
  consistent : HConsistent st
  rmw_semantics : HRSemanticsRMW st

def initState
    {Node : Type uRefNode} {Value : Type uRefValue}
    (initNode : Node) (initValue : Value) : RefState Node Value where
  epochID := 0
  live := fun _ => True
  nodeTS := fun _ => { version := 0, tieBreaker := initNode }
  nodeValue := fun _ => initValue
  nodeState := fun _ => HState.hs_valid
  nodeRcvedAcks := fun _ _ => False
  nodeLastWriter := fun _ => initNode
  nodeLastWriteTS := fun _ => { version := 0, tieBreaker := initNode }
  nodeWriteEpochID := fun _ => 0
  nodeFlagRMW := fun _ => false
  parentOf := fun _ => { version := 0, tieBreaker := initNode }
  tsKind := fun t k => t = { version := 0, tieBreaker := initNode } /\ k = OpKind.write
  tsValue := fun t v => t = { version := 0, tieBreaker := initNode } /\ v = initValue
  rmsgs := fun _ => False
  ackMsgs := fun _ => False
  valMsgs := fun m => m.ts = { version := 0, tieBreaker := initNode }
  committedRMWs := fun _ => False
  committedWrites := fun t => t = { version := 0, tieBreaker := initNode }
  o3Quorum := fun _ _ _ _ => False

inductive Reachable
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (rank : NodeRank Node) (initNode : Node) (initValue : Value) :
    RefState Node Value -> Prop where
  | init :
      Reachable rank initNode initValue
        (initState (Node := Node) (Value := Value) initNode initValue)
  | step {st st' : RefState Node Value} {label : RefLabel Node} :
      Reachable rank initNode initValue st ->
      HRNext rank st label st' ->
      Reachable rank initNode initValue st'

theorem init_safety
    {Node : Type uRefNode} {Value : Type uRefValue}
    (initNode : Node) (initValue : Value) :
    RefSafety (initState (Node := Node) (Value := Value) initNode initValue) := by
  refine {
    consistent := ?_
    rmw_semantics := ?_
  }
  · intro k s _hk _hs _hvk _hvs
    rfl
  · constructor
    · intro x y hx _hy
      cases hx
    · intro x y hx _hy
      cases hx

theorem init_rmw_invariant
    {Node : Type uRefNode} {Value : Type uRefValue}
    (initNode : Node) (initValue : Value) :
    RefRmwInvariant
      (initState (Node := Node) (Value := Value) initNode initValue) := by
  refine {
    semantic := by
      constructor
      · intro x y hx _hy
        cases hx
      · intro x y hx _hy
        cases hx
    rmsg_epoch_le := ?_
    rmsg_write_safe := ?_
    rmsg_rmw_write_safe := ?_
    rmsg_rmw_unique := ?_
    live_write_safe := ?_
    live_rmw_write_safe := ?_
    live_rmw_unique := ?_
  }
  · intro m hm
    cases hm
  · intro m x hm _hEpoch _hKind _hx
    cases hm
  · intro m y hm _hEpoch _hKind _hy
    cases hm
  · intro m x hm _hEpoch _hKind _hx
    cases hm
  · intro x n hx _hLive _hFlag
    cases hx
  · intro n y _hLive hFlag _hy
    cases hFlag
  · intro n x _hLive hFlag _hx
    cases hFlag

theorem init_agreement_invariant
    {Node : Type uRefNode} {Value : Type uRefValue}
    (rank : NodeRank Node) (initNode : Node) (initValue : Value) :
    RefAgreementInvariant rank (initTimestamp initNode)
      (initState (Node := Node) (Value := Value) initNode initValue) := by
  refine {
    init_le_live := ?_
    ack_msg_advanced := ?_
    rcved_ack_advanced := ?_
    val_msg_committed := ?_
    committed_live_advanced := ?_
    valid_committed := ?_
    write_or_replay_last_current := ?_
    o3_quorum_epoch_le := ?_
    o3_quorum_nonself_ack := ?_
    o3_current_quorum_advanced := ?_
  }
  · intro n _hLive
    exact tsLe_refl rank (initTimestamp initNode)
  · intro m h
    cases h
  · intro n a h
    cases h
  · intro m hm
    left
    simpa [initState, initTimestamp] using hm
  · intro t n hCommitted _hLive
    rcases hCommitted with hRmw | hWrite
    · cases hRmw
    · subst hWrite
      exact tsLe_refl rank (initTimestamp initNode)
  · intro n hValid
    left
    rfl
  · intro n hState
    rcases hState with hWrite | hReplay
    · cases hWrite
    · cases hReplay
  · intro n c t e h
    cases h
  · intro n c t h
    cases h
  · intro n c t h
    cases h

theorem reachable_agreement_invariant
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (rank : NodeRank Node) (initNode : Node) (initValue : Value) :
    forall st, Reachable rank initNode initValue st ->
      RefAgreementInvariant rank (initTimestamp initNode) st := by
  intro st hReach
  induction hReach with
  | init =>
      exact init_agreement_invariant rank initNode initValue
  | step hReach hStep ih =>
      exact ref_agreement_preserved ih hStep

theorem agreement_invariant_consistent
    {Node : Type uRefNode} {Value : Type uRefValue}
    {rank : NodeRank Node} {initTs : Timestamp Node}
    {st : RefState Node Value}
    (hInv : RefAgreementInvariant rank initTs st) :
    HConsistent st := by
  intro k s hk hs hvk hvs
  have hCommittedK : RefCommitted initTs st (st.nodeTS k) :=
    hInv.valid_committed k hvk
  have hCommittedS : RefCommitted initTs st (st.nodeTS s) :=
    hInv.valid_committed s hvs
  have hks : tsLe rank (st.nodeTS k) (st.nodeTS s) :=
    ref_committed_live_advanced hInv (st.nodeTS k) s hCommittedK hs
  have hsk : tsLe rank (st.nodeTS s) (st.nodeTS k) :=
    ref_committed_live_advanced hInv (st.nodeTS s) k hCommittedS hk
  exact tsLe_antisymm rank hks hsk

theorem reachable_consistency
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (rank : NodeRank Node) (initNode : Node) (initValue : Value) :
    forall st, Reachable rank initNode initValue st -> HConsistent st := by
  intro st hReach
  exact agreement_invariant_consistent
    (reachable_agreement_invariant rank initNode initValue st hReach)

theorem failure_increments_epoch
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (st : RefState Node Value) (n : Node) :
    (nodeFailurePost st n).epochID = st.epochID + 1 := by
  rfl

theorem failure_resets_all_acks
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (st : RefState Node Value) (n c a : Node) :
    Not ((nodeFailurePost st n).nodeRcvedAcks c a) := by
  intro h
  exact h

theorem rmw_replay_resets_acks
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (st : RefState Node Value) (n a : Node) :
    Not ((rmwReplayPost st n).nodeRcvedAcks n a) := by
  simp [rmwReplayPost, replaceRelFirst]

theorem write_replay_preserves_acks
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (st : RefState Node Value) (n a : Node) :
    (writeReplayPost st n).nodeRcvedAcks n a = st.nodeRcvedAcks n a := by
  rfl

theorem stale_rmw_inv_ack_messages_iff
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (rank : NodeRank Node) (st : RefState Node Value)
    (n : Node) (m : RInvMsg Node Value) (ack : AckMsg Node)
    (hStale : tsLt rank m.ts (st.nodeTS n)) :
    (receiveRmwInvPost rank st n m).ackMsgs ack <->
      st.ackMsgs ack := by
  unfold receiveRmwInvPost
  have hNotGreater : Not (tsLt rank (st.nodeTS n) m.ts) := by
    intro hGreater
    have hEq : st.nodeTS n = m.ts :=
      tsLe_antisymm rank hGreater.1 hStale.1
    exact hStale.2 hEq.symm
  have hNotEq : st.nodeTS n ≠ m.ts := by
    intro hEq
    exact hStale.2 hEq.symm
  rw [if_neg hNotGreater]
  rw [if_neg hNotEq]

theorem stale_rmw_inv_sends_local_rinv
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (rank : NodeRank Node) (st : RefState Node Value)
    (n : Node) (m : RInvMsg Node Value)
    (hStale : tsLt rank m.ts (st.nodeTS n)) :
    (receiveRmwInvPost rank st n m).rmsgs
      (sendRInvMsg st n (st.nodeTS n) (st.nodeValue n)
        (st.parentOf (st.nodeTS n))
        (if st.nodeFlagRMW n then OpKind.rmw else OpKind.write)) := by
  unfold receiveRmwInvPost
  have hNotGreater : Not (tsLt rank (st.nodeTS n) m.ts) := by
    intro hGreater
    have hEq : st.nodeTS n = m.ts :=
      tsLe_antisymm rank hGreater.1 hStale.1
    exact hStale.2 hEq.symm
  have hNotEq : st.nodeTS n ≠ m.ts := by
    intro hEq
    exact hStale.2 hEq.symm
  rw [if_neg hNotGreater]
  rw [if_neg hNotEq]
  unfold addRInv
  exact Or.inr rfl

theorem o3_quorum_excludes_coordinator_self_ack
    {Node : Type uRefNode} {Value : Type uRefValue}
    (rank : NodeRank Node)
    (st : RefState Node Value) (coordinator : Node) (t : Timestamp Node)
    (hLiveCoordinator : st.live coordinator)
    (hCoordinatorAdvanced : tsLe rank t (st.nodeTS coordinator))
    (hNonSelf :
      forall a, st.live a -> a ≠ coordinator ->
        st.ackMsgs { sender := a, epochID := st.epochID, ts := t }) :
    o3AckQuorum rank st coordinator t := by
  exact ⟨hLiveCoordinator, hCoordinatorAdvanced, hNonSelf⟩

theorem o3_quorum_allows_missing_coordinator_ack
    {Node : Type uRefNode} {Value : Type uRefValue}
    (rank : NodeRank Node)
    (st : RefState Node Value) (coordinator : Node) (t : Timestamp Node)
    (hLiveCoordinator : st.live coordinator)
    (hCoordinatorAdvanced : tsLe rank t (st.nodeTS coordinator))
    (hNonSelf :
      forall a, st.live a -> a ≠ coordinator ->
        st.ackMsgs { sender := a, epochID := st.epochID, ts := t })
    (hNoSelf :
      Not (st.ackMsgs { sender := coordinator, epochID := st.epochID, ts := t })) :
    o3AckQuorum rank st coordinator t := by
  exact ⟨hLiveCoordinator, hCoordinatorAdvanced, hNonSelf⟩

def StepTrace
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    (rank : NodeRank Node) (tr : Nat -> RefState Node Value)
    (labels : Nat -> RefLabel Node) : Prop :=
  forall i, HRNext rank (tr i) (labels i) (tr (i + 1))

def EventuallyFrom {World : Type uRefNode} (p : Nat -> World -> Prop)
    (tr : Nat -> World) (i : Nat) : Prop :=
  exists j, i <= j /\ p j (tr j)

def AlwaysFrom {World : Type uRefNode} (p : Nat -> World -> Prop)
    (tr : Nat -> World) (i : Nat) : Prop :=
  forall j, i <= j -> p j (tr j)

def FairlyScheduled {Node : Type uRefNode} (labels : Nat -> RefLabel Node)
    (label : RefLabel Node) : Prop :=
  forall i, exists j, i <= j /\ labels j = label

def ReadyStuck
    {Node : Type uRefNode} {Value : Type uRefValue}
    (tr : Nat -> RefState Node Value) (n : Node) (i : Nat) : Prop :=
  receivedAllAcks (tr i) n /\
    ((tr i).nodeState n = HState.hs_write \/
      (tr i).nodeState n = HState.hs_replay) /\
    AlwaysFrom
      (fun _ st =>
        receivedAllAcks st n /\
          (st.nodeState n = HState.hs_write \/
            st.nodeState n = HState.hs_replay))
      tr i

theorem fair_send_vals_rules_out_permanent_ready
    {Node : Type uRefNode} {Value : Type uRefValue} [DecidableEq Node]
    {rank : NodeRank Node} {tr : Nat -> RefState Node Value}
    {labels : Nat -> RefLabel Node} {n : Node}
    (hTrace : StepTrace rank tr labels)
    (hFair : FairlyScheduled labels (.sendVals n))
    (hLabelCompletes :
      forall i,
        labels i = RefLabel.sendVals n ->
        Not (receivedAllAcks (tr (i + 1)) n /\
          ((tr (i + 1)).nodeState n = HState.hs_write \/
            (tr (i + 1)).nodeState n = HState.hs_replay))) :
    forall i, Not (ReadyStuck tr n i) := by
  intro i hStuck
  rcases hFair i with ⟨j, hij, hLabel⟩
  have hBadNext :
      receivedAllAcks (tr (j + 1)) n /\
        ((tr (j + 1)).nodeState n = HState.hs_write \/
          (tr (j + 1)).nodeState n = HState.hs_replay) :=
    hStuck.2.2 (j + 1) (Nat.le_trans hij (Nat.le_succ j))
  exact hLabelCompletes j hLabel hBadNext

end Reference

-- Evidence dump for the TLA/paper-aligned reference model.
-- #print HermesRmwO3.Reference.HRNext
-- #print HermesRmwO3.Reference.ref_agreement_preserved
-- #print axioms HermesRmwO3.Reference.ref_agreement_preserved
-- #print HermesRmwO3.Reference.Reachable
-- #print HermesRmwO3.Reference.reachable_agreement_invariant
-- #print axioms HermesRmwO3.Reference.reachable_agreement_invariant
-- #print HermesRmwO3.Reference.reachable_consistency
-- #print axioms HermesRmwO3.Reference.reachable_consistency
-- #print HermesRmwO3.Reference.init_safety
-- #print axioms HermesRmwO3.Reference.init_safety
-- #print HermesRmwO3.Reference.failure_increments_epoch
-- #print axioms HermesRmwO3.Reference.failure_increments_epoch
-- #print HermesRmwO3.Reference.failure_resets_all_acks
-- #print axioms HermesRmwO3.Reference.failure_resets_all_acks
-- #print HermesRmwO3.Reference.rmw_replay_resets_acks
-- #print axioms HermesRmwO3.Reference.rmw_replay_resets_acks
-- #print HermesRmwO3.Reference.write_replay_preserves_acks
-- #print axioms HermesRmwO3.Reference.write_replay_preserves_acks
-- #print HermesRmwO3.Reference.stale_rmw_inv_ack_messages_iff
-- #print axioms HermesRmwO3.Reference.stale_rmw_inv_ack_messages_iff
-- #print HermesRmwO3.Reference.stale_rmw_inv_sends_local_rinv
-- #print axioms HermesRmwO3.Reference.stale_rmw_inv_sends_local_rinv
-- #print HermesRmwO3.Reference.o3_quorum_excludes_coordinator_self_ack
-- #print axioms HermesRmwO3.Reference.o3_quorum_excludes_coordinator_self_ack
-- #print HermesRmwO3.Reference.o3_quorum_allows_missing_coordinator_ack
-- #print axioms HermesRmwO3.Reference.o3_quorum_allows_missing_coordinator_ack
-- #print HermesRmwO3.Reference.fair_send_vals_rules_out_permanent_ready
-- #print axioms HermesRmwO3.Reference.fair_send_vals_rules_out_permanent_ready
-- 
end HermesRmwO3
