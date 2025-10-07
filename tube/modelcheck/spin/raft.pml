/*
from https://github.com/namasikanam/raft-spin/

    Author: Xingyu Xie

MIT License

Copyright (c) 2021 Xingyu Xie

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/

#define MAX_TERM 3 // 1 to 3
#define MAX_LOG 2 // 0 to 1
#define MAX_SERVER // 1 to 3

#define NIL 10 // a number that won't be used

// message channels
typedef AppendEntry {
    byte term, leaderCommit, index, prevLogTerm
};
typedef AppendEntryChannels {
    chan ch[3] = [1] of { AppendEntry };
};
AppendEntryChannels ae_ch[3];

typedef AppendEntryResponse {
    byte term;
    bool success
};
typedef AppendEntryResponseChannels {
    chan ch[3] = [1] of { AppendEntryResponse };
};
AppendEntryResponseChannels aer_ch[3];

typedef RequestVote {
    byte term, lastLogIndex, lastLogTerm
};
typedef RequestVoteChannels {
    chan ch[3] = [1] of { RequestVote };
};
RequestVoteChannels rv_ch[3];

typedef RequestVoteResponse {
    byte term;
    bool voteGranted
};
typedef RequestVoteResponseChannels {
    chan ch[3] = [1] of { RequestVoteResponse };
};
RequestVoteResponseChannels rvr_ch[3];

// The following variables are actually local,
// we move them globally, because SPIN doesn't support
// that represent LTL with local variables.
mtype:State = { leader, candidate, follower };
mtype:State state[3];
byte currentTerm[3];
typedef Logs {
    byte log[2];
};
Logs log[3];
byte commitIndex[3];

// If commitIndex reaches MAX_LOG, the whole system is nearly full.
// There's no need to run further.
proctype server(byte serverId) {
    state[serverId] = follower;
    byte votedFor = NIL;
    
    // helpers
    byte i;

    bool votesGranted[3];
    RequestVote rv;
    byte lastLogTerm, lastLogIndex;
    RequestVoteResponse rvr;
    bool logOk;

    AppendEntry ae;
    AppendEntryResponse aer;

    do // main loop
    :: // timeout
        (state[serverId] == candidate || state[serverId] == follower) ->
            atomic {
                state[serverId] = candidate;
                currentTerm[serverId] = currentTerm[serverId] + 1;

end_max_term:   if // end if the limit is reached. Note that MAX_TERM is reachable here, which just shows the design intention
                :: (currentTerm[serverId] <= MAX_TERM) -> skip
                fi

                votedFor = serverId;
                votesGranted[0] = 0; votesGranted[1] = 0; votesGranted[2] = 0;
                votesGranted[serverId] = 1;
            }
    :: // restart
        state[serverId] = follower
    :: // request vote
        (state[serverId] == candidate) ->
            atomic {
                rv.term = currentTerm[serverId];
                if
                :: (log[serverId].log[0] == 0) ->
                    rv.lastLogTerm = 0;
                    rv.lastLogIndex = 0
                :: (log[serverId].log[0] != 0 && log[serverId].log[1] == 0) ->
                    rv.lastLogTerm = log[serverId].log[0];
                    rv.lastLogIndex = 1
                :: (log[serverId].log[0] != 0 && log[serverId].log[1] != 0) ->
                    rv.lastLogTerm = log[serverId].log[1];
                    rv.lastLogIndex = 2
                fi

                if
                :: (serverId != 0) ->
end_rv_0:           rv_ch[serverId].ch[0]!rv
                :: (serverId != 1) ->
end_rv_1:           rv_ch[serverId].ch[1]!rv
                :: (serverId != 2) ->
end_rv_2:           rv_ch[serverId].ch[2]!rv
                fi
            }
    :: // become leader
        (state[serverId] == candidate && (votesGranted[0] + votesGranted[1] + votesGranted[2] > 1)) ->
            state[serverId] = leader;
    :: // handle RequestVote
        (rv_ch[0].ch[serverId]?[rv] || rv_ch[1].ch[serverId]?[rv] || rv_ch[2].ch[serverId]?[rv]) ->
            atomic {
                // calculate the id of the sender
                if
                :: rv_ch[0].ch[serverId]?rv -> i = 0
                :: rv_ch[1].ch[serverId]?rv -> i = 1
                :: rv_ch[2].ch[serverId]?rv -> i = 2
                fi
                assert(i != serverId);
                // update terms
                if
                :: (rv.term > currentTerm[serverId]) ->
                    currentTerm[serverId] = rv.term;
                    state[serverId] = follower;
                    votedFor = NIL
                :: (rv.term <= currentTerm[serverId]) ->
                    skip
                fi

                if
                :: (log[serverId].log[0] == 0) ->
                    lastLogTerm = 0;
                    lastLogIndex = 0
                :: (log[serverId].log[0] != 0 && log[serverId].log[1] == 0) ->
                    lastLogTerm = log[serverId].log[0];
                    lastLogIndex = 1
                :: (log[serverId].log[0] != 0 && log[serverId].log[1] != 0) ->
                    lastLogTerm = log[serverId].log[1];
                    lastLogIndex = 2
                fi

                logOk = rv.lastLogTerm > lastLogTerm || rv.lastLogTerm == lastLogTerm && rv.lastLogIndex >= lastLogIndex;
                rvr.voteGranted = rv.term == currentTerm[serverId] && logOk && (votedFor == NIL || votedFor == i);

                rvr.term = currentTerm[serverId];
                if
                :: rvr.voteGranted -> votedFor = i
                :: !rvr.voteGranted -> skip
                fi
end_rvr:        rvr_ch[serverId].ch[i]!rvr
            }
    :: // handle RequestVoteResponse
        (rvr_ch[0].ch[serverId]?[rvr] || rvr_ch[1].ch[serverId]?[rvr] || rvr_ch[2].ch[serverId]?[rvr]) ->
            atomic {
                // calculate the id of the sender
                if
                :: rvr_ch[0].ch[serverId]?rvr -> i = 0
                :: rvr_ch[1].ch[serverId]?rvr -> i = 1
                :: rvr_ch[2].ch[serverId]?rvr -> i = 2
                fi
                assert(i != serverId);

                if
                :: (rvr.term > currentTerm[serverId]) -> // update terms
                    currentTerm[serverId] = rvr.term;
                    state[serverId] = follower;
                    votedFor = NIL
                :: (rvr.term == currentTerm[serverId] && rvr.voteGranted) ->
                    votesGranted[i] = 1
                :: !(rvr.term > currentTerm[serverId]) && !(rvr.term == currentTerm[serverId] && rvr.voteGranted) ->
                    skip
                fi
            }

    :: // append entries
        (state[serverId] == leader) ->
            atomic {
                if
                :: (serverId != 0) -> i = 0
                :: (serverId != 1) -> i = 1
                :: (serverId != 2) -> i = 2
                fi

                ae.term = currentTerm[serverId];
                ae.leaderCommit = commitIndex[serverId];
                if
                :: (log[serverId].log[0] != log[i].log[0]) ->
                    ae.index = 0
                :: (log[serverId].log[1] != 0 && log[serverId].log[0] == log[i].log[0] && log[serverId].log[1] != log[i].log[1]) ->
                    ae.index = 1
                    ae.prevLogTerm = log[i].log[0]
                :: ae.index = NIL
                fi
end_ae:         ae_ch[serverId].ch[i]!ae
            }
    :: // handle AppendEntry
        (ae_ch[0].ch[serverId]?[ae] || ae_ch[1].ch[serverId]?[ae] || ae_ch[2].ch[serverId]?[ae]) ->
            atomic {
                // calculate the id of the sender
                if
                :: ae_ch[0].ch[serverId]?ae -> i = 0
                :: ae_ch[1].ch[serverId]?ae -> i = 1
                :: ae_ch[2].ch[serverId]?ae -> i = 2
                fi
                assert(i != serverId);

                // update terms
                if
                :: (ae.term > currentTerm[serverId]) ->
                    currentTerm[serverId] = ae.term;
                    state[serverId] = follower;
                    votedFor = NIL
                :: (ae.term <= currentTerm[serverId]) ->
                    skip
                fi
                assert(ae.term <= currentTerm[serverId]);

                // return to follower state
                if
                :: (ae.term == currentTerm[serverId] && state[serverId] == candidate) ->
                    state[serverId] = follower;
                    votedFor = NIL
                :: (ae.term != currentTerm[serverId] || state[serverId] != candidate) ->
                    skip
                fi
                assert(!(ae.term == currentTerm[serverId]) || (state[serverId] == follower));
                
                logOk = ae.index == 0 || (ae.index == 1 && ae.prevLogTerm == log[serverId].log[0]);
                aer.term = currentTerm[serverId];
                if
                :: (ae.term < currentTerm[i] || ae.term == currentTerm[serverId] && state[serverId] == follower && !logOk) -> // reject request
                    aer.success = 0;
end_aer_rej:        aer_ch[serverId].ch[i]!aer
                :: (ae.term == currentTerm[serverId] && state[serverId] == follower && logOk) ->
                    aer.success = 1;

                    log[serverId].log[ae.index] = ae.term;

                    // Direct assignment is admissible here.
                    // Because our MAX_LOG is small enough (2).
                    // leaderCommit is either 0 or 1.
                    // If leaderCommit is 0, commitIndex of the server must be 0.
                    // If leaderCommit is 1, commitIndex of the server can be 0.
                    commitIndex[serverId] = ae.leaderCommit;

end_aer_acc:        aer_ch[serverId].ch[i]!aer
                fi
            }
    :: // handle AppendEntryResponse
        (aer_ch[0].ch[serverId]?[aer] || aer_ch[1].ch[serverId]?[aer] || aer_ch[2].ch[serverId]?[aer]) ->
            atomic {
                // calculate the id of the sender
                if
                :: aer_ch[0].ch[serverId]?aer -> i = 0
                :: aer_ch[1].ch[serverId]?aer -> i = 1
                :: aer_ch[2].ch[serverId]?aer -> i = 2
                fi
                assert(i != serverId);

                if
                :: (aer.term > currentTerm[serverId]) -> // update terms
                    currentTerm[serverId] = aer.term;
                    state[serverId] = follower;
                    votedFor = NIL
                :: (aer.term < currentTerm[serverId]) ->
                    skip
                :: (aer.term == currentTerm[serverId] && aer.success && state[serverId] == leader) ->
                    // advance commit index
                    // as we only have 3 servers
                    // one success AppendEntry means committed

end_commitIndex:    if // end if commitIndex reaches the limit
                    :: (commitIndex[serverId] == 0 && log[i].log[0] == log[serverId].log[0]) ->
                        commitIndex[serverId] = 1
                    // this is a little tricky
                    // we do NOT skip if commitIndex[serverId] should be 2
                    :: (commitIndex[serverId] == 1 && !(log[serverId].log[1] != 0 && log[i].log[1] == log[serverId].log[1])) ->
                        skip; // actually this case won't be reached
                    fi
                :: (aer.term == currentTerm[serverId] && !(aer.success && state[serverId] == leader)) ->
                    skip
                fi
            }
    :: // client request
        (state[serverId] == leader && log[serverId].log[1] == 0) ->
            if
            :: log[serverId].log[0] == 0 ->
                log[serverId].log[0] = currentTerm[serverId]
            :: log[serverId].log[0] != 0 ->
                log[serverId].log[1] = currentTerm[serverId]
            fi 
    od
};

init {
    run server(0);
    run server(1);
    run server(2)
}

/*
ltl electionSafety {
    always !(
        (state[0] == leader && state[1] == leader && currentTerm[0] == currentTerm[1])
        || (state[0] == leader && state[2] == leader && currentTerm[0] == currentTerm[2])
        || (state[1] == leader && state[2] == leader && currentTerm[1] == currentTerm[2])
    )
}
*/
/* // jea: leave just one, try to get compile.
// for scalability of SPIN, we split the huge complete formula into small formulas
ltl leaderAppendOnly00 {
    always (
        state[0] == leader implies (
            (log[0].log[0] == 0)
            || ((log[0].log[0] == 1) weakuntil (state[0] != leader))
            || ((log[0].log[0] == 2) weakuntil (state[0] != leader))
            || ((log[0].log[0] == 3) weakuntil (state[0] != leader))
        )
    )
}
ltl leaderAppendOnly01 {
    always (
        state[0] == leader implies (
            (log[0].log[1] == 0)
            || ((log[0].log[1] == 1) weakuntil (state[0] != leader))
            || ((log[0].log[1] == 2) weakuntil (state[0] != leader))
            || ((log[0].log[1] == 3) weakuntil (state[0] != leader))
        )
    )
}
ltl leaderAppendOnly10 {
    always (
        state[1] == leader implies (
            (log[1].log[0] == 0)
            || ((log[1].log[0] == 1) weakuntil (state[1] != leader))
            || ((log[1].log[0] == 2) weakuntil (state[1] != leader))
            || ((log[1].log[0] == 3) weakuntil (state[1] != leader))
        )
    )
}
ltl leaderAppendOnly11 {
    always (
        state[1] == leader implies (
            (log[1].log[1] == 0)
            || ((log[1].log[1] == 1) weakuntil (state[1] != leader))
            || ((log[1].log[1] == 2) weakuntil (state[1] != leader))
            || ((log[1].log[1] == 3) weakuntil (state[1] != leader))
        )
    )
}
ltl leaderAppendOnly20 {
    always (
        state[2] == leader implies (
            (log[2].log[0] == 0)
            || ((log[2].log[0] == 1) weakuntil (state[2] != leader))
            || ((log[2].log[0] == 2) weakuntil (state[2] != leader))
            || ((log[2].log[0] == 3) weakuntil (state[2] != leader))
        )
    )
}
ltl leaderAppendOnly21 {
    always (
        state[2] == leader implies (
            (log[2].log[1] == 0)
            || ((log[2].log[1] == 1) weakuntil (state[2] != leader))
            || ((log[2].log[1] == 2) weakuntil (state[2] != leader))
            || ((log[2].log[1] == 3) weakuntil (state[2] != leader))
        )
    )
}

ltl logMatching {
    always (
        ((log[0].log[1] != 0 && log[0].log[1] == log[1].log[1])
            implies (log[0].log[0] == log[1].log[0]))
        && ((log[0].log[1] != 0 && log[0].log[1] == log[2].log[1])
            implies (log[0].log[0] == log[2].log[0]))
        && ((log[1].log[1] != 0 && log[1].log[1] == log[2].log[1])
            implies (log[1].log[0] == log[2].log[0]))
    )
}
*/

// Here we know that committed entries won't be changed anymore,
// based on this observation:
// Every server may become a leader in the future
ltl leaderCompleteness {
    always (
        (
            (commitIndex[0] == 1) implies
                always (
                    ((state[1] == leader) implies (log[0].log[0] == log[1].log[0]))
                    && ((state[2] == leader) implies (log[0].log[0] == log[2].log[0]))
                )
        ) && (
            (commitIndex[1] == 1) implies
                always (
                    ((state[0] == leader) implies (log[1].log[0] == log[0].log[0]))
                    && ((state[2] == leader) implies (log[1].log[0] == log[2].log[0]))
                )
        ) && (
            (commitIndex[2] == 1) implies
                always (
                    ((state[0] == leader) implies (log[2].log[0] == log[0].log[0]))
                    && ((state[1] == leader) implies (log[2].log[0] == log[1].log[0]))
                )
        )
    )
}
/*
ltl stateMachineSafety {
    always (
        ((commitIndex[0] == 1 && commitIndex[1] == 1) implies (log[0].log[0] == log[1].log[0]))
        && ((commitIndex[0] == 1 && commitIndex[2] == 1) implies (log[0].log[0] == log[2].log[0]))
        && ((commitIndex[1] == 1 && commitIndex[2] == 1) implies (log[1].log[0] == log[2].log[0]))
    )
}
*/