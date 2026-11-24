---------------------------- MODULE HermesWitness --------------------------
EXTENDS tube
VARIABLE step
a == CHOOSE n \in CzMembers : TRUE
b == CHOOSE n \in CzMembers \ {a} : TRUE
Receive(kind,at) == \E msg \in hermes.wire : msg.kind = kind /\ msg.dst = HermesSelf(at) /\ HermesReceive(msg)
Lose(kind,at) == \E msg \in hermes.wire : msg.kind = kind /\ msg.dst = HermesSelf(at) /\ HermesDrop(msg)
Schedule ==
    CASE step = 0 -> HermesAdmit(a,"rmw")
      [] step = 1 -> HermesClient(a)
      [] step = 2 -> Receive("INV",b)
      [] step = 3 -> Lose("ACK",a)
      [] step = 4 -> HermesTick
      [] step = 5 -> HermesTimeout(a)
      [] step = 6 -> Receive("INV",b)
      [] step = 7 -> Receive("ACK",a)
      [] step = 8 -> Lose("VAL",b)
      [] step = 9 -> HermesAdmit(b,"read")
      [] step = 10 -> HermesClient(b)
      [] step = 11 -> HermesTick
      [] step = 12 -> HermesTimeout(b)
      [] step = 13 -> Receive("INV",a)
      [] step = 14 -> Receive("ACK",b)
      [] OTHER -> FALSE
ws == <<vars,step>>
Witness == (HermesEstablishedInit /\ step = 0) /\ [][Schedule /\ HermesOnly /\ step'=step+1]_ws
WitnessExpected == step < 15 \/ ~(hermes.committed /\ hermes.key[b].read = 1 /\ hermes.key[a].phase = "valid")
====
