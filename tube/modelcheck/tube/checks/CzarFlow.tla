------------------------------- MODULE CzarFlow -------------------------------
EXTENDS tube
VARIABLE step
A == CHOOSE x \in CzMembers : TRUE
B == CHOOSE x \in CzMembers \ {A} : TRUE
State == <<step,mcVars>>
Start == MCInit /\ step = 0
Event ==
    CASE step \in {0,3,27,32} -> CzSubmit(IF step=0 THEN A ELSE B)
      [] step \in {1,4,28,33} -> CzTubeApply(1,0)
      [] step \in {2,5,29,34} -> CzHandleResult(IF step=2 THEN A ELSE B)
      [] step \in {6,12,17} -> CzStartContact(B)
      [] step=7 -> CzDialReady(B)
      [] step \in {8,13} -> CzPingServe(B)
      [] step \in {9,14} -> CzPingReceive(B)
      [] step=18 -> CzContactFailed(B,TRUE)
      [] step=19 -> CzDepose(A)
      [] OTHER -> CzTick
Advance ==
    IF step < 35 THEN /\ Event /\ UNCHANGED <<coreVars,kvVars,fc,hermes>> /\ step'=step+1
    ELSE UNCHANGED State
FlowSpec == Start /\ [][Advance]_State
FlowResult ==
    /\ (step=3 => czState[A]="AmCzar" /\ DOMAIN CzPublishedMembers(A)={A})
    /\ (step=6 => czState[B]="NotCzar" /\ czCtl[B].learned=1)
    /\ (step=10 => DOMAIN CzPublishedMembers(B)={A,B} /\ CzOperatingGrant(B)<=0)
    /\ (step=15 => CzOperatingGrant(B)>0)
    /\ (step=20 => czState[A]="Unknown" /\ czCtl[A].delay>czReg.rem)
    /\ (step=30 => czState[B]="AmCzar" /\ czReg.epoch=2)
    /\ (step=35 => CzOperatingGrant(B)>0 /\ ~czCtl[B].grant.member)
=============================================================================
