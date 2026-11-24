--------------------------- MODULE HermesExit ----------------------------
EXTENDS tube
NoLateExit == ~\E c \in hermes.exits :
                c \notin hermes.reg /\ HermesSlot(hermes.reg,c.src,c.dst) /= {}
==========================================================================
