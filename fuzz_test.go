package rpc25519

import (
	"testing"
)

func TestFuzzSimnet(t *testing.T) {
	/*
		    // catalogue of choices

			submit := time.Now()
			subAsap := true
			batch := simnet.NewSimnetBatch(subwhen, subAsap)

			simnodeName := "node_0"
			// alter := SHUTDOWN
			// alter := POWERON
			// alter := ISOLATE
			alter := UNISOLATE

			batch.AlterHost(simnodeName, alter)
			dd := &DropDeafSpec{
				UpdateDeafReads:  true,
				DeafReadsNewProb: 0.5,
				UpdateDropSends:  true,
				DropSendsNewProb: 0.5,
			}

			origin := "node_0"
			target := "node_1"
			deliverDroppedSends := false
			batch.FaultCircuit(origin, target, dd, deliverDroppedSends)
			batch.FaultHost(origin, dd, deliverDroppedSends)

			unIsolate := false
			powerOnIfOff := false
			batch.RepairCircuit(origin, unIsolate, powerOnIfOff, deliverDroppedSends)
			allHosts := false
			batch.RepairHost(origin, unIsolate, powerOnIfOff, allHosts, deliverDroppedSends)
			batch.AllHealthy(powerOnIfOff, deliverDroppedSends)
			wholeHost := false
			batch.AlterCircuit(origin, alter, wholeHost)
			batch.GetSimnetSnapshot()

			var tick, minHop, maxHop time.Duration
			var seed [32]byte
			scen := NewScenario(tick, minHop, maxHop, seed)
	*/
}
