------------------------------- MODULE HermesPublication --------------------
EXTENDS tube
VARIABLE step
a == CHOOSE m \in CzMembers : TRUE
b == CHOOSE m \in CzMembers \ {a} : TRUE
R(kind, n) == \E msg \in hermes.wire : msg.kind = kind /\ msg.dst = HermesSelf(n) /\ HermesReceive(msg)
Schedule ==
    CASE step = 0 -> HermesAdvertise(a)
      [] step = 1 -> HermesAdvertise(b)
      [] step = 2 -> CzSubmit(a)
      [] step = 3 -> CzTubeApply(1,0)
      [] step = 4 -> CzHandleResult(a)
      [] step = 5 -> CzSubmit(b)
      [] step = 6 -> CzTubeApply(1,0)
      [] step = 7 -> CzHandleResult(b)
      [] step = 8 -> CzStartContact(b)
      [] step = 9 -> CzDialReady(b)
      [] step = 10 -> CzPingServe(b)
      [] step = 11 -> CzPingReceive(b)
      [] step = 12 -> CzTick
      [] step = 13 -> CzSubmit(a)
      [] step = 14 -> CzTubeApply(1,0)
      [] step = 15 -> CzHandleResult(a)
      [] step = 16 -> CzStartContact(b)
      [] step = 17 -> CzPingServe(b)
      [] step = 18 -> CzPingReceive(b)
      [] step = 19 -> HermesConsumeGrant(b)
      [] step = 20 -> HermesAdopt(a) /\ Cardinality(hermes'.view[a].live) = 1
      [] step = 21 -> HermesAdopt(a) /\ Cardinality(hermes'.view[a].live) = 2
      [] step = 22 -> HermesAdopt(b)
      [] step = 23 -> \E q \in hermes.dial : HermesDial(q) /\ hermes'.opening /= {}
      [] step = 24 -> \E c \in hermes.opening : HermesRegister(c)
      [] step = 25 -> \E c \in hermes.opening : HermesRegister(c)
      [] step = 26 -> HermesConsumeGrant(a)
      [] step = 27 -> HermesAdmit(b,"rmw")
      [] step = 28 -> HermesClient(b)
      [] step = 29 -> R("INV",a)
      [] step = 30 -> R("ACK",b)
      [] step = 31 -> R("VAL",a)
      [] OTHER -> FALSE
wvars == <<vars, step>>
Witness == (Init /\ step=0) /\ [][HermesCzarStep /\ Schedule /\ step'=step+1]_wvars
Goal == step < 32 \/ ~(hermes.committed /\ hermes.key[a].phase = "valid" /\ hermes.key[b].phase = "valid"
                       /\ hermes.key[a].value = 1 /\ hermes.key[b].value = 1)
============================================================================
