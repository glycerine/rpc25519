import Std

/-!
# Hermes O3 protocol safety and liveness facts

This file is a Lean 4 companion to `hermes_o3.ivy`, the single-key Hermes
model with the O3 broadcast-ACK optimization enabled.

The `Safety` structure spells out the invariant block from `hermes_o3.ivy`.
Compared with `hermes.lean`, this model adds:

* `o3Quorum`, the remembered observation that every live replica broadcast an
  ACK for a coordinator/timestamp pair;
* `o3Try`, the monitor event used by the O3 liveness proof;
* the O3 safety invariant saying an observed O3 quorum contains an ACK from
  every live replica.

The theorem `o3_ack_quorums_eventually_finish` mirrors the Ivy temporal
property of the same name.  Lean does not run Ivy's liveness-to-safety tactic,
so the fairness and progress facts used by that tactic are explicit trace
hypotheses.
-/

set_option autoImplicit false

namespace HermesO3

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
  ord.le x y /\ x ≠ y

theorem le_of_lt {alpha : Type uTs} {ord : TotalOrder alpha} {x y : alpha} :
    lt ord x y -> ord.le x y := by
  intro h
  exact h.1

theorem not_lt_self {alpha : Type uTs} {ord : TotalOrder alpha} (x : alpha) :
    ¬ lt ord x x := by
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
  tsValue : TS -> Value -> Prop
  tsRmw : TS -> Prop
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
  tsValue := fun t v => t = initTs /\ v = initValue
  tsRmw := fun _ => False
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

  -- Persistent network buffers carry valid timestamp metadata.
  inv_write_wf :
    forall S T V, st.invWrite S T V -> st.seenTs T /\ st.tsValue T V /\ ¬ st.tsRmw T
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

  -- Per-node metadata is consistent with timestamp metadata.
  cur_seen :
    forall N, st.seenTs (st.curTs N)
  cur_value_seen :
    forall N, st.tsValue (st.curTs N) (st.curValue N)
  cur_rmw_ts :
    forall N, st.curRmw N -> st.tsRmw (st.curTs N)
  cur_non_rmw_ts :
    forall N, ¬ st.curRmw N -> ¬ st.tsRmw (st.curTs N)
  pending_seen :
    forall N, st.pending N -> st.seenTs (st.pendingTs N)
  pending_self_acked :
    forall N, st.pending N -> st.acked N N
  pending_below_cur :
    forall N, st.pending N -> ord.le (st.pendingTs N) (st.curTs N)
  pending_rmw_ts :
    forall N, st.pending N -> st.pendingRmw N -> st.tsRmw (st.pendingTs N)
  pending_non_rmw_ts :
    forall N, st.pending N -> ¬ st.pendingRmw N -> ¬ st.tsRmw (st.pendingTs N)
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
    forall N A, ¬ st.pending N -> ¬ st.acked N A
  not_pending_not_ready :
    forall N, ¬ st.pending N -> ¬ st.ready N

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
    forall R W B, st.parent R B -> st.parent W B -> st.tsRmw R -> ¬ st.tsRmw W ->
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
    forall N, st.ready N -> st.readyEpoch N ≠ initEpoch
  epoch_done_seen :
    forall E, st.epochDone E -> st.seenEpoch E
  no_complete_try :
    forall N, ¬ st.completeTry N
  no_o3_try :
    forall N, ¬ st.o3Try N
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
    ts_value_init := ⟨rfl, rfl⟩
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
    cur_seen := by
      intro N
      rfl
    cur_value_seen := by
      intro N
      exact ⟨rfl, rfl⟩
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
    AlwaysFrom (fun _ st => ¬ st.epochDone E) tr i

def ReadyEpochLiveness
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (tr : Nat -> State Node TS Value Epoch) : Prop :=
  forall N E,
    GloballyEventually (fun _ st => st.completeTry N) tr ->
      Globally (fun i _ => ¬ ReadyEpochStuck tr N E i) tr

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
        (forall k, i <= k -> k <= j -> ¬ (tr k).epochDone E) ->
        (tr j).ready N /\ (tr j).readyEpoch N = E) :
    ReadyEpochLiveness tr := by
  intro N E fair
  intro i
  intro stuck
  rcases stuck with ⟨readyI, epochI, neverDone⟩
  rcases fair i with ⟨j, ij, completeJ⟩
  have notDoneBetween : forall k, i <= k -> k <= j -> ¬ (tr k).epochDone E := by
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
    (tr i).state N ≠ HState.hs_valid /\
    ¬ (tr i).completed T /\
    ¬ (tr i).tsRmw T /\
    AlwaysFrom
      (fun _ st => st.curTs N = T /\ st.state N ≠ HState.hs_valid /\ ¬ st.completed T)
      tr i

def O3QuorumLiveness
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (tr : Nat -> State Node TS Value Epoch) : Prop :=
  forall N C T,
    GloballyEventually (fun _ st => st.o3Try N) tr ->
      Globally (fun i _ => ¬ O3QuorumStuck tr N C T i) tr

theorem o3_ack_quorums_eventually_finish
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (tr : Nat -> State Node TS Value Epoch)
    (o3_try_finishes_quorum :
      forall i N C T,
        (tr i).o3Try N ->
        (tr i).o3Quorum N C T ->
        (tr i).curTs N = T ->
        (tr i).state N ≠ HState.hs_valid ->
        ¬ (tr i).completed T ->
        ¬ (tr i).tsRmw T ->
        (tr i).completed T \/ (tr i).state N = HState.hs_valid \/ (tr i).curTs N ≠ T)
    (o3_quorum_non_rmw_persists :
      forall i j N C T,
        i <= j ->
        (tr i).o3Quorum N C T ->
        ¬ (tr i).tsRmw T ->
        (tr j).o3Quorum N C T /\ ¬ (tr j).tsRmw T) :
    O3QuorumLiveness tr := by
  intro N C T fair
  intro i
  intro stuck
  rcases stuck with ⟨quorumI, _curI, _stateI, _notCompletedI, nonRmwI, staysBad⟩
  rcases fair i with ⟨j, ij, o3TryJ⟩
  have persisted : (tr j).o3Quorum N C T /\ ¬ (tr j).tsRmw T :=
    o3_quorum_non_rmw_persists i j N C T ij quorumI nonRmwI
  have badJ : (tr j).curTs N = T /\ (tr j).state N ≠ HState.hs_valid /\ ¬ (tr j).completed T :=
    staysBad j ij
  have doneJ :
      (tr j).completed T \/ (tr j).state N = HState.hs_valid \/ (tr j).curTs N ≠ T :=
    o3_try_finishes_quorum j N C T
      o3TryJ persisted.1 badJ.1 badJ.2.1 badJ.2.2 persisted.2
  rcases doneJ with completedJ | validOrMoved
  · exact badJ.2.2 completedJ
  · rcases validOrMoved with validJ | movedJ
    · exact badJ.2.1 validJ
    · exact movedJ badJ.1

end HermesO3
