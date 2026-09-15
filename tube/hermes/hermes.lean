import Std

/-!
# Hermes protocol safety and liveness facts

This file is a Lean 4 companion to `hermes.ivy`.  It keeps the same
single-key vocabulary as the Ivy model: timestamps are an arbitrary total
order, network buffers are persistent relations, and the protocol state records
the local Hermes state for each node.

The main safety object, `Safety`, is a direct Lean spelling of the invariant
block in `hermes.ivy`.  Theorems below prove the externally interesting safety
facts from that invariant bundle:

* live valid replicas agree on timestamp and value;
* blind writes from the same base are above RMW timestamps;
* at most one RMW from a base can complete.

The liveness theorem `ready_epochs_eventually_finish` mirrors Ivy's
`ready_epochs_eventually_finish` temporal property.  Lean does not run Ivy's
`l2s_auto5` tactic, so the fairness/progress obligations that tactic uses are
made explicit as hypotheses over a trace.
-/

set_option autoImplicit false

namespace Hermes

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
deriving DecidableEq, Repr

structure TotalOrder (α : Type uTs) where
  le : α → α → Prop
  refl : ∀ x, le x x
  trans : ∀ {x y z}, le x y → le y z → le x z
  antisymm : ∀ {x y}, le x y → le y x → x = y
  total : ∀ x y, le x y ∨ le y x

def lt {α : Type uTs} (ord : TotalOrder α) (x y : α) : Prop :=
  ord.le x y ∧ x ≠ y

theorem le_of_lt {α : Type uTs} {ord : TotalOrder α} {x y : α} :
    lt ord x y → ord.le x y := by
  intro h
  exact h.1

theorem not_lt_self {α : Type uTs} {ord : TotalOrder α} (x : α) :
    ¬ lt ord x x := by
  intro h
  exact h.2 rfl

theorem eq_of_le_le {α : Type uTs} {ord : TotalOrder α} {x y : α} :
    ord.le x y → ord.le y x → x = y := by
  intro hxy hyx
  exact ord.antisymm hxy hyx

structure State
    (Node : Type uNode) (TS : Type uTs)
    (Value : Type uValue) (Epoch : Type uEpoch) where
  readyTask : LTask
  state : Node → HState
  curTs : Node → TS
  curValue : Node → Value
  curRmw : Node → Prop
  lastWriter : Node → Node
  live : Node → Prop
  pending : Node → Prop
  pendingTs : Node → TS
  pendingRmw : Node → Prop
  acked : Node → Node → Prop
  ready : Node → Prop
  readyEpoch : Node → Epoch
  seenEpoch : Epoch → Prop
  epochDone : Epoch → Prop
  seenTs : TS → Prop
  parent : TS → TS → Prop
  tsValue : TS → Value → Prop
  tsRmw : TS → Prop
  invWrite : Node → TS → Value → Prop
  invRmw : Node → TS → Value → Prop
  ackMsg : Node → Node → TS → Prop
  valMsg : TS → Prop
  completed : TS → Prop
  completeTry : Node → Prop

def initState
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (initTs : TS) (initValue : Value) (initEpoch : Epoch) :
    State Node TS Value Epoch where
  readyTask := LTask.ready_finish
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
  tsValue := fun t v => t = initTs ∧ v = initValue
  tsRmw := fun _ => False
  invWrite := fun _ _ _ => False
  invRmw := fun _ _ _ => False
  ackMsg := fun _ _ _ => False
  valMsg := fun t => t = initTs
  completed := fun t => t = initTs
  completeTry := fun _ => False

structure InitAssumptions {TS : Type uTs} (ord : TotalOrder TS) (initTs : TS) : Prop where
  init_min : ∀ t, ord.le initTs t

structure Safety
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (ord : TotalOrder TS) (initTs : TS) (initValue : Value) (initEpoch : Epoch)
    (σ : State Node TS Value Epoch) : Prop where
  -- Initial distinguished facts.
  seen_epoch_init : σ.seenEpoch initEpoch
  seen_ts_init : σ.seenTs initTs
  completed_init : σ.completed initTs
  ts_value_init : σ.tsValue initTs initValue

  -- Timestamp metadata is functional and well-formed.
  ts_value_functional :
    ∀ T V₁ V₂, σ.tsValue T V₁ → σ.tsValue T V₂ → V₁ = V₂
  parent_functional :
    ∀ T B₁ B₂, σ.parent T B₁ → σ.parent T B₂ → B₁ = B₂
  parent_seen :
    ∀ T B, σ.parent T B → σ.seenTs T ∧ σ.seenTs B ∧ lt ord B T
  ts_value_seen :
    ∀ T V, σ.tsValue T V → σ.seenTs T
  ts_rmw_seen :
    ∀ T, σ.tsRmw T → σ.seenTs T

  -- Persistent network buffers carry valid timestamp metadata.
  inv_write_wf :
    ∀ S T V, σ.invWrite S T V → σ.seenTs T ∧ σ.tsValue T V ∧ ¬ σ.tsRmw T
  inv_rmw_wf :
    ∀ S T V, σ.invRmw S T V → σ.seenTs T ∧ σ.tsValue T V ∧ σ.tsRmw T
  ack_msg_seen :
    ∀ A C T, σ.ackMsg A C T → σ.seenTs T
  ack_msg_advanced :
    ∀ A C T, σ.ackMsg A C T → ord.le T (σ.curTs A)
  val_msg_completed :
    ∀ T, σ.valMsg T → σ.completed T
  completed_seen :
    ∀ T, σ.completed T → σ.seenTs T

  -- Per-node metadata is consistent with timestamp metadata.
  cur_seen :
    ∀ N, σ.seenTs (σ.curTs N)
  cur_value_seen :
    ∀ N, σ.tsValue (σ.curTs N) (σ.curValue N)
  cur_rmw_ts :
    ∀ N, σ.curRmw N → σ.tsRmw (σ.curTs N)
  cur_non_rmw_ts :
    ∀ N, ¬ σ.curRmw N → ¬ σ.tsRmw (σ.curTs N)
  pending_seen :
    ∀ N, σ.pending N → σ.seenTs (σ.pendingTs N)
  pending_self_acked :
    ∀ N, σ.pending N → σ.acked N N
  pending_below_cur :
    ∀ N, σ.pending N → ord.le (σ.pendingTs N) (σ.curTs N)
  pending_rmw_ts :
    ∀ N, σ.pending N → σ.pendingRmw N → σ.tsRmw (σ.pendingTs N)
  pending_non_rmw_ts :
    ∀ N, σ.pending N → ¬ σ.pendingRmw N → ¬ σ.tsRmw (σ.pendingTs N)
  pending_rmw_current :
    ∀ N, σ.pending N → σ.pendingRmw N → σ.pendingTs N = σ.curTs N
  pending_rmw_cur_flag :
    ∀ N, σ.pending N → σ.pendingRmw N → σ.curRmw N
  pending_acked_advanced :
    ∀ N A, σ.pending N → σ.acked N A → ord.le (σ.pendingTs N) (σ.curTs A)
  ready_pending :
    ∀ N, σ.ready N → σ.pending N
  ready_live_acked :
    ∀ N A, σ.ready N → σ.live A → σ.acked N A
  ready_ts_current_or_old :
    ∀ N, σ.ready N → σ.pendingTs N = σ.curTs N ∨ lt ord (σ.pendingTs N) (σ.curTs N)
  ready_current_state :
    ∀ N, σ.ready N → σ.pendingTs N = σ.curTs N →
      σ.state N = HState.hs_write ∨ σ.state N = HState.hs_replay
  ready_old_state :
    ∀ N, σ.ready N → lt ord (σ.pendingTs N) (σ.curTs N) →
      σ.state N = HState.hs_invalid_write ∨
        σ.state N = HState.hs_invalid ∨
        σ.state N = HState.hs_valid
  pending_current_state :
    ∀ N, σ.pending N → σ.pendingTs N = σ.curTs N →
      σ.state N = HState.hs_write ∨ σ.state N = HState.hs_replay
  pending_old_state :
    ∀ N, σ.pending N → lt ord (σ.pendingTs N) (σ.curTs N) →
      σ.state N = HState.hs_invalid_write ∨
        σ.state N = HState.hs_invalid ∨
        σ.state N = HState.hs_valid
  not_pending_not_acked :
    ∀ N A, ¬ σ.pending N → ¬ σ.acked N A
  not_pending_not_ready :
    ∀ N, ¬ σ.pending N → ¬ σ.ready N

  -- Core Hermes safety facts.
  completed_live_advanced :
    ∀ T N, σ.completed T → σ.live N → ord.le T (σ.curTs N)
  valid_completed :
    ∀ N, σ.state N = HState.hs_valid → σ.completed (σ.curTs N)
  valid_live_ts_agree :
    ∀ N₁ N₂,
      σ.live N₁ → σ.live N₂ →
      σ.state N₁ = HState.hs_valid → σ.state N₂ = HState.hs_valid →
      σ.curTs N₁ = σ.curTs N₂
  valid_live_value_agree :
    ∀ N₁ N₂,
      σ.live N₁ → σ.live N₂ →
      σ.state N₁ = HState.hs_valid → σ.state N₂ = HState.hs_valid →
      σ.curValue N₁ = σ.curValue N₂
  write_rmw_spacing :
    ∀ R W B, σ.parent R B → σ.parent W B → σ.tsRmw R → ¬ σ.tsRmw W →
      lt ord R W
  ready_rmw_no_completed_conflict :
    ∀ N B R,
      σ.live N → σ.ready N → σ.pendingRmw N →
      σ.parent (σ.pendingTs N) B → σ.completed R → σ.tsRmw R → σ.parent R B →
      σ.pendingTs N = R
  ready_rmw_same_base :
    ∀ N₁ N₂ B,
      σ.live N₁ → σ.ready N₁ → σ.pendingRmw N₁ →
      σ.parent (σ.pendingTs N₁) B →
      σ.live N₂ → σ.ready N₂ → σ.pendingRmw N₂ →
      σ.parent (σ.pendingTs N₂) B →
      σ.pendingTs N₁ = σ.pendingTs N₂
  completed_rmw_same_base :
    ∀ R₁ R₂ B,
      σ.completed R₁ → σ.completed R₂ →
      σ.tsRmw R₁ → σ.tsRmw R₂ →
      σ.parent R₁ B → σ.parent R₂ B →
      R₁ = R₂

  -- Liveness monitor invariants from the Ivy file.
  ready_live :
    ∀ N, σ.ready N → σ.live N
  ready_epoch_seen :
    ∀ N, σ.ready N → σ.seenEpoch (σ.readyEpoch N)
  ready_epoch_not_init :
    ∀ N, σ.ready N → σ.readyEpoch N ≠ initEpoch
  epoch_done_seen :
    ∀ E, σ.epochDone E → σ.seenEpoch E
  no_complete_try :
    ∀ N, ¬ σ.completeTry N
  ready_task_finish :
    σ.readyTask = LTask.ready_finish

theorem valid_read_timestamps_agree
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {σ : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch σ) :
    ∀ N₁ N₂,
      σ.live N₁ → σ.live N₂ →
      σ.state N₁ = HState.hs_valid → σ.state N₂ = HState.hs_valid →
      σ.curTs N₁ = σ.curTs N₂ := by
  exact h.valid_live_ts_agree

theorem valid_read_values_agree
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {σ : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch σ) :
    ∀ N₁ N₂,
      σ.live N₁ → σ.live N₂ →
      σ.state N₁ = HState.hs_valid → σ.state N₂ = HState.hs_valid →
      σ.curValue N₁ = σ.curValue N₂ := by
  exact h.valid_live_value_agree

theorem valid_read_timestamps_agree_from_core
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {σ : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch σ) :
    ∀ N₁ N₂,
      σ.live N₁ → σ.live N₂ →
      σ.state N₁ = HState.hs_valid → σ.state N₂ = HState.hs_valid →
      σ.curTs N₁ = σ.curTs N₂ := by
  intro N₁ N₂ live₁ live₂ valid₁ valid₂
  have completed₁ : σ.completed (σ.curTs N₁) := h.valid_completed N₁ valid₁
  have completed₂ : σ.completed (σ.curTs N₂) := h.valid_completed N₂ valid₂
  have le₁₂ : ord.le (σ.curTs N₁) (σ.curTs N₂) :=
    h.completed_live_advanced (σ.curTs N₁) N₂ completed₁ live₂
  have le₂₁ : ord.le (σ.curTs N₂) (σ.curTs N₁) :=
    h.completed_live_advanced (σ.curTs N₂) N₁ completed₂ live₁
  exact ord.antisymm le₁₂ le₂₁

theorem valid_read_values_agree_from_core
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {σ : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch σ) :
    ∀ N₁ N₂,
      σ.live N₁ → σ.live N₂ →
      σ.state N₁ = HState.hs_valid → σ.state N₂ = HState.hs_valid →
      σ.curValue N₁ = σ.curValue N₂ := by
  intro N₁ N₂ live₁ live₂ valid₁ valid₂
  have ts_eq : σ.curTs N₁ = σ.curTs N₂ :=
    valid_read_timestamps_agree_from_core h N₁ N₂ live₁ live₂ valid₁ valid₂
  have value₁ : σ.tsValue (σ.curTs N₁) (σ.curValue N₁) := h.cur_value_seen N₁
  have value₂ : σ.tsValue (σ.curTs N₂) (σ.curValue N₂) := h.cur_value_seen N₂
  have value₁' : σ.tsValue (σ.curTs N₂) (σ.curValue N₁) := by
    simpa [ts_eq] using value₁
  exact h.ts_value_functional (σ.curTs N₂) (σ.curValue N₁) (σ.curValue N₂) value₁' value₂

theorem completed_rmw_unique_per_base
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    {ord : TotalOrder TS} {initTs : TS} {initValue : Value} {initEpoch : Epoch}
    {σ : State Node TS Value Epoch}
    (h : Safety ord initTs initValue initEpoch σ) :
    ∀ R₁ R₂ B,
      σ.completed R₁ → σ.completed R₂ →
      σ.tsRmw R₁ → σ.tsRmw R₂ →
      σ.parent R₁ B → σ.parent R₂ B →
      R₁ = R₂ := by
  exact h.completed_rmw_same_base

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
      intro T V₁ V₂ h₁ h₂
      exact h₁.2.trans h₂.2.symm
    parent_functional := by
      intro T B₁ B₂ h
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
      intro N₁ N₂ _live₁ _live₂ _valid₁ _valid₂
      rfl
    valid_live_value_agree := by
      intro N₁ N₂ _live₁ _live₂ _valid₁ _valid₂
      rfl
    write_rmw_spacing := by
      intro R W B hparent _hparentW _hrmw _hnrmw
      cases hparent
    ready_rmw_no_completed_conflict := by
      intro N B R _live hready _hprmw _hparent _hcompleted _hrmw _hparentR
      cases hready
    ready_rmw_same_base := by
      intro N₁ N₂ B _live₁ hready₁ _hprmw₁ _hparent₁ _live₂ _hready₂ _hprmw₂ _hparent₂
      cases hready₁
    completed_rmw_same_base := by
      intro R₁ R₂ B _hcompleted₁ _hcompleted₂ hrmw₁ _hrmw₂ _hparent₁ _hparent₂
      cases hrmw₁
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
    ready_task_finish := rfl
  }

def Globally {World : Type uNode} (p : Nat → World → Prop) (tr : Nat → World) : Prop :=
  ∀ i, p i (tr i)

def EventuallyFrom {World : Type uNode} (p : Nat → World → Prop) (tr : Nat → World)
    (i : Nat) : Prop :=
  ∃ j, i ≤ j ∧ p j (tr j)

def GloballyEventually {World : Type uNode} (p : Nat → World → Prop)
    (tr : Nat → World) : Prop :=
  ∀ i, EventuallyFrom p tr i

def AlwaysFrom {World : Type uNode} (p : Nat → World → Prop) (tr : Nat → World)
    (i : Nat) : Prop :=
  ∀ j, i ≤ j → p j (tr j)

def ReadyEpochStuck
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (tr : Nat → State Node TS Value Epoch) (N : Node) (E : Epoch) (i : Nat) : Prop :=
  (tr i).ready N ∧ (tr i).readyEpoch N = E ∧
    AlwaysFrom (fun _ σ => ¬ σ.epochDone E) tr i

def ReadyEpochLiveness
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (tr : Nat → State Node TS Value Epoch) : Prop :=
  ∀ N E,
    GloballyEventually (fun _ σ => σ.completeTry N) tr →
      Globally (fun i _ => ¬ ReadyEpochStuck tr N E i) tr

theorem ready_epochs_eventually_finish
    {Node : Type uNode} {TS : Type uTs}
    {Value : Type uValue} {Epoch : Type uEpoch}
    (tr : Nat → State Node TS Value Epoch)
    (complete_try_finishes_ready_epoch :
      ∀ i N E,
        (tr i).completeTry N →
        (tr i).ready N →
        (tr i).readyEpoch N = E →
        (tr i).epochDone E)
    (ready_epoch_persists_until_done :
      ∀ i j N E,
        i ≤ j →
        (tr i).ready N →
        (tr i).readyEpoch N = E →
        (∀ k, i ≤ k → k ≤ j → ¬ (tr k).epochDone E) →
        (tr j).ready N ∧ (tr j).readyEpoch N = E) :
    ReadyEpochLiveness tr := by
  intro N E fair
  intro i
  intro stuck
  rcases stuck with ⟨ready_i, epoch_i, never_done⟩
  rcases fair i with ⟨j, ij, complete_j⟩
  have not_done_between : ∀ k, i ≤ k → k ≤ j → ¬ (tr k).epochDone E := by
    intro k ik _kj
    exact never_done k ik
  have ready_j : (tr j).ready N ∧ (tr j).readyEpoch N = E :=
    ready_epoch_persists_until_done i j N E ij ready_i epoch_i not_done_between
  have done_j : (tr j).epochDone E :=
    complete_try_finishes_ready_epoch j N E complete_j ready_j.1 ready_j.2
  exact never_done j ij done_j

end Hermes

-- Evidence dump: print the checked theorem bodies and their axiom dependencies.
#print Hermes.valid_read_timestamps_agree_from_core
#print axioms Hermes.valid_read_timestamps_agree_from_core
#print Hermes.valid_read_values_agree_from_core
#print axioms Hermes.valid_read_values_agree_from_core
#print Hermes.completed_rmw_unique_per_base
#print axioms Hermes.completed_rmw_unique_per_base
#print Hermes.init_safety
#print axioms Hermes.init_safety
#print Hermes.ready_epochs_eventually_finish
#print axioms Hermes.ready_epochs_eventually_finish
