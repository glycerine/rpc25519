//
// Promela model of the basic Paxos algorithm.
// (just Synod, a single round of agreement on one value).
//
// This is the optimized version of the promela model.
// It runs much faster, especially when compiled with -DBITSTATE.
//
// * Extracted from the "Model Checking Paxos in Spin" paper
// * by Giorgio Delzanno, Michele Tatarek, and  Riccardo Traverso.
// * https://arxiv.org/pdf/1408.5962
//
// That work was Copyright(C) 2014 by these authors, and
// licensed under the Creative Commons Attribution License (CC-BY).
//
// * ...and then heavily altered, updated, and fixed
// * to the point where it is only distantly similar/might not
// * even be recognizable from the originally presented form.
//
// This version: Copyright(C) 2025 by Jason E. Aten, Ph.D.
// License (kept the same): Creative Commons Attribution License (CC-BY).

/* simple paxos, one round of Synod (herein):
   Proposer -> Prepare -> Acceptors
   Acceptors -> Promise -> Proposer
   Proposer -> Accept -> Acceptors
   Acceptors -> Learn -> Learners

   MultiPaxos:
   Leader -> Prepare -> Acceptors  (once, at start)
   Acceptors -> Promise -> Leader
   Leader -> Accept -> Acceptors   (for each value)
   Acceptors -> Learn -> Learners
*/

#define ACCEPTORS 3
#define PROPOSERS 3

// Normal (should not be faulty).
#define MAJORITY (ACCEPTORS / 2 + 1)
//
// Try to get a fault detected:
//
// MAJORITY==1 should be faulty if PROPOSERS and ACCEPTORS >= 2
//define MAJORITY 1
/*
global vars:
	chan prepare (=1):	len 2:	 [1,2,], [2,2,],
	chan promise (=2):	len 4:	 [2,-1,-1,], [2,-1,-1,], [2,-1,-1,], [2,-1,-1,],
	chan accept (=3):	len 0:	
	chan learn (=4):	len 0:	
*/

#define MAX (ACCEPTORS*PROPOSERS)

typedef mex{
  byte rnd;   // current round number (only consider votes on this round/ballot!)
  short prnd; // previously commited round number
  short pval; // previously commited value
}

chan prepare = [MAX] of {byte, byte}; // to acceptor. recipient identity, ballot.
chan promise = [MAX] of {mex}; 
chan accept = [MAX] of {byte, byte, short};
chan learn = [MAX] of {short, short, short};

inline bprepare(round){
  byte j = 1;
  do
    ::(j <= ACCEPTORS) ->
        prepare !! j, round -> j++;
    ::else -> break;
  od
}

inline baccept(round, v) {
  byte k = 1;
  for(k : 1 .. ACCEPTORS){
    accept !! k,round,v; 
  }
}


inline recv_proposer(round, count, h, v, hr, hval) {
  d_step {
    promise ?? eval(round), h, v ->   
    if :: count < MAJORITY -> count ++;
       :: else
    fi;
    if :: h > hr -> // taking the highest of the committed values from acceptors.
           hr = h;
           hval = v
       :: else
    fi;
    h = 0; v = 0;
  }
}

//
// Section 6 of paper, optimized versions of the above.
//

// a proposer routine
inline occ(i, pr, count, hr, hv, crnd) {
 d_step{
  printf("i = %d, len(promise)=%d; len(prepare)=%d\n", i, len(promise), len(prepare)); // i = 0, len(promise) = 0, len(prepare) = 4. always promise=0?!?
  do
   //i=0;
   :: i < len(promise) -> 
     promise ? pr; promise ! pr;
     if
       :: pr.rnd == crnd ->
          count++;
          if
            :: pr.prnd > hr ->
               hr = pr.prnd; hv= pr.pval;
            :: else
          fi;
       :: else
     fi;
     i++;
  :: else ->
     pr.prnd =0; pr.pval =0; pr.rnd =0; i=0;
     break;
 od;
 }
}

// "[In the Section 6 optimized version, an] accept 
// message is broadcasted to the acceptors 
// when majority is detected. This is implemented
// in the test procedure defined as follows."
// (used in qt).
inline test(count, hr, hv, myval, crnd, aux) {
if
  :: count >=MAJORITY ->
     aux =(hr < 0 -> myval : hv); // conditional expression
     // unreached, strange, so try inlining manually:
     // baccept(crnd, aux);
     int k = 1; // unreached.
     do
       ::(k <= ACCEPTORS) ->
          printf("k = %d in baccept inline, ACCEPTORs=%d\n", k, ACCEPTORS);
          accept !! k,crnd,aux;
          k++;
       ::else -> break;
     od
     break;
  :: else
fi;
}

// a proposer routine
inline qt(i, pr, count, hr, hv, myval,crnd, aux) {
  atomic {
    occ(i, pr, count, hr, hv, crnd);
    test(count, hr, hv, myval, crnd, aux); 
    hv= -1; hr = -1; count = 0; aux = 0; 
  }
}

proctype proposer_optimized(short crnd; short myval) {
  short aux, hr = -1, hv = -1;
  short rnd;
  short prnd, pval;
  byte count =0, i = 0;
  mex pr;
  d_step {
  
    // bagh!?! spin: optimized.pml:272, Error: goto S_146_0 breaks from d_step seq
    // somehow breaks the inliner, so do it manually below.
    // bprepare(crnd);

    byte j = 1;
    do
     ::(j <= ACCEPTORS) -> 
        prepare !! j, crnd;
        printf("sent on prepare j=%d, crnd=%d\n", j, crnd);
        j++;
     :: else -> break;
    od
    // end of manually unrolled bpreare(crnd);
  } // end of d_step
end:  do
    :: qt(i, pr, count, hr, hv, myval, crnd, aux);
  od
}


// optimized version of acceptor 
proctype acceptor_optimized(int id) {
  printf("top of acceptor_optimized, id=%d; len(prepare)=%d\n", id, len(prepare));
  short crnd = -1; // max ballot seen.
  short prnd = -1, pval = -1;
  short aval, rnd;
  do
    :: d_step {
         prepare ?? <eval(id), rnd> ->
         printf("acceptor id=%d sees prepare len %d: rnd=%d; crnd=%d\n", id, len(prepare), rnd, crnd); // len 4, 1, 1. should hold 1,1,2,2
         if
           ::(rnd > crnd) -> crnd = rnd;
              //printf("crnd is now %d\n", crnd);
           :: else
         fi;
         rnd = 0
       }
       // we have to send back on promise, right???
       promise !! crnd, prnd, pval;
       //printf("prepare ?? was run, and promise ! too\n");
    :: d_step {
         accept ?? eval(id), rnd, aval ->
         if
           ::(rnd >=crnd) -> // ballot in rnd is >= promised, so commit.
             crnd=rnd;
             prnd=rnd;
             pval=aval;
             learn !! id, crnd, aval;
             printf("acceptor id=%d replied on learn\n", id);
           :: else
         fi;
         rnd = 0; aval = 0;
       }
       break;
  od
  printf("end of acceptor_optimized id = %d\n", id);
}


inline read_learn_chan_and_assert(id, rnd, lval, lastval, mcount) {
  d_step {
    learn ?? id, rnd, lval ->
       printf("read_learn read from learn: id=%d, rnd=%d, lval=%d\n", id, rnd, lval);
    if
      :: mcount [rnd -1] < MAJORITY ->
           mcount [rnd -1]++;
           printf("read_learn: id=%d, rnd=%d, %d < MAJ\n", id, rnd, mcount[rnd-1]);
      :: else
    fi;
    if
      :: mcount [rnd -1] >= MAJORITY ->
           printf("read_learn: id=%d, mcount=%d >= MAJ\n", id, mcount[rnd-1]);      
         if :: (lastval >= 0 && lastval != lval) ->
                 printf("assert error: lastval: %d != lval: %d\n", lastval, lval);
                 assert(false); // equiv to assert(lastval == lval)
            :: (lastval == -1) -> lastval = lval;
            :: else
         fi
         done = true;  // Exit after learning a value
      :: else
    fi;
    id = 0; rnd = 0; lval = 0;
  }
}

// Active learner process that checks for
// consistency (asserts inside read_learn_chan_and_assert).
active proctype learner_assert_consistency() {
  short lastval = -1, id, rnd, lval;
  byte mcount[PROPOSERS];
  bool done = false;
end:  do
    :: read_learn_chan_and_assert(id, rnd, lval, lastval, mcount);
    :: done -> break;
  od
  printf("learner_assert_consistency is done.\n");
}

init {
  atomic{  
    int j = 1;
    for (j : 1 .. PROPOSERS) {
      //run proposer(j, j);
      run proposer_optimized(j, j);
    }

    int k = 1;
    for (k : 1 .. ACCEPTORS) {
      //run acceptor(k);
      run acceptor_optimized(k);
    }

    // if using learner_assert_consistency, do not also run this:
    //run learner();
  };
}
