Tube (a Raft consensus implementation) Modeling Guidance
---------------------------

Goal

 * Assess correctness risks across Tube especially with respect to connection management
   logic and implementation. Connection management is half the implementation, but is
   not covered by traditional RAFT specifications at all.
   
Scope

 * In scope: everything is in scope above the standard Go librarys.
 
 * Out of scope: the Go standard library packages.
 
Priority Questions

 * Can a sequence of crashes, network failures, assymetric network card failures, reboots, 
   reconfigurations, leader elections, cluster membership changes, or disk storage failures
   cause loss of data (without the bounds of the RAFT guarantees that a majority of nodes
   in the cluster continues to operate without failures in that majority) or loss of
   liveness or violate one of the 5 classic RAFT invariants?
   
