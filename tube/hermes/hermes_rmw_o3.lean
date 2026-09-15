import Std

/-!
# Hermes RMW+O3 protocol safety and liveness facts

This file is a Lean 4 companion to `hermes_rmw_o3.ivy`, the single-key Hermes
model with RMW support and the O3 broadcast-ACK optimization enabled.

The `Safety` structure spells out the invariant block from `hermes_rmw_o3.ivy`.
Compared with `hermes_o3.lean`, this final model adds:

* `parentTs`, a ghost timestamp-parent function used by the conflict marker;
* `rmwConflict`, which records that an RMW timestamp has lost to another RMW
  from the same parent;
* O3 liveness properties that finish a remembered ACK quorum or explain it by
  a recorded RMW conflict.

The temporal theorems mirror Ivy's liveness properties. Lean does not run
Ivy's liveness-to-safety tactic, so the fairness and progress facts used by
that tactic are explicit trace hypotheses.
-/

set_option autoImplicit false

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

set_option maxHeartbeats 50000 in
theorem local_rmw_preserves_core_probe
    {Node : Type uNode} {TS : Type uTs} {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {st : State Node TS Value Epoch} {n : Node} {t : TS} {v : Value}
    (hCore : CoreInvariant ord initTs initValue initEpoch st)
    (hLive : st.live n)
    (hNotPending : Not (st.pending n))
    (hState : st.state n = HState.hs_valid)
    (hLt : lt ord (st.curTs n) t)
    (hFresh : Not (st.seenTs t))
    (hSpacing : forall W, st.parent W (st.curTs n) -> Not (st.tsRmw W) -> lt ord t W) :
    CoreInvariant ord initTs initValue initEpoch (localRmwPost st n t v) := by
  cases hCore
  simp [CoreInvariant, localRmwPost, upd, add1, set1, set2FirstSelf,
    add3, addTsValue, addParent, removeTs, addRmwConflicts, lt] at *
  grind [TotalOrder.trans, TotalOrder.antisymm]

end Operational

end HermesRmwO3
