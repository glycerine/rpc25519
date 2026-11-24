----------------------------- MODULE CzarHandoff -----------------------------
EXTENDS tube
VARIABLE step
A == CHOOSE x \in CzMembers : TRUE
B == CHOOSE x \in CzMembers \ {A} : TRUE
C == CHOOSE x \in CzMembers \ {A,B} : TRUE
State == <<step,mcVars>>
Start == MCInit /\ step = 0
Event ==
    CASE step=0 -> CzSubmit(A)
      [] step \in {1,4,16,27,32} -> CzTubeApply(1,0)
      [] step=2 -> CzHandleResult(A)
      [] step \in {3,26,31} -> CzSubmit(B)
      [] step \in {5,28,33} -> CzHandleResult(B)
      [] step \in {6,29} -> CzStartContact(B)
      [] step=7 -> CzCrash(A)
      [] step \in {8,30} -> CzContactFailed(B,FALSE)
      [] step=15 -> CzSubmit(C)
      [] step=17 -> CzHandleResult(C)
      [] step=18 -> CzCrash(C)
      [] OTHER -> CzTick
Advance ==
    IF step < 34 THEN /\ Event /\ UNCHANGED <<coreVars,kvVars,fc,hermes>> /\ step'=step+1
    ELSE UNCHANGED State
HandoffSpec == Start /\ [][Advance]_State
HandoffResult ==
    /\ (step=6 => czCtl[B].learned=1)
    /\ (step=18 => czState[C]="AmCzar" /\ czReg.epoch=2)
    /\ (step=29 => czCtl[B].learned=2 /\ czVers[B].E=1 /\ czVers[B].rem<0)
    /\ (step=32 => czQ[1].prevE=2)
    /\ (step=34 => czState[B]="AmCzar" /\ czReg.epoch=3)
=============================================================================
