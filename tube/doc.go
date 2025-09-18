package tube

// On names
//
// A tube is smaller version of a raft, but is
// still a fun way to enjoy a river.
//
// The tube connects people, providing transport
// for lots of people.
//
// Why was the name Raft chosen?
//
// John Ousterhout says,
// at 1:00:10 in https://www.youtube.com/watch?v=vYp4LYbnnW8
//
// 1. Stands for RAFT = Replicated And Fault Tolerant
//
// 2. A raft is something you can build out of a collection of logs.
//
// 3. A raft is something you can use to get away from the island of Paxos.
//
// Enjoy.

// Client interaction notes from
// https://thesquareplanet.com/blog/students-guide-to-raft/
/*
https://thesquareplanet.com/blog/students-guide-to-raft/#duplicate-detection

As soon as you have clients retry operations in the face of errors, you need some kind of duplicate detection scheme – if a client sends an APPEND to your server, doesn’t hear back, and re-sends it to the next server, your apply() function needs to ensure that the APPEND isn’t executed twice. To do so, you need some kind of unique identifier for each client request, so that you can recognize if you have seen, and more importantly, applied, a particular operation in the past. Furthermore, this state needs to be a part of your state machine so that all your Raft servers eliminate the same duplicates.

There are many ways of assigning such identifiers. One simple and fairly efficient one is to give each client a unique identifier, and then have them tag each request with a monotonically increasing sequence number. If a client re-sends a request, it re-uses the same sequence number. Your server keeps track of the latest sequence number it has seen for each client, and simply ignores any operation that it has already seen.

https://thesquareplanet.com/blog/students-guide-to-raft/#hairy-corner-cases

If your implementation follows the general outline given above, there are at least two subtle issues you are likely to run into that may be hard to identify without some serious debugging. To save you some time, here they are:

Re-appearing indices: Say that your Raft library has some method Start() that takes a command, and return the index at which that command was placed in the log (so that you know when to return to the client, as discussed above). You might assume that you will never see Start() return the same index twice, or at the very least, that if you see the same index again, the command that first returned that index must have failed. It turns out that neither of these things are true, even if no servers crash.

Consider the following scenario with five servers, S1 through S5. Initially, S1 is the leader, and its log is empty.

Two client operations (C1 and C2) arrive on S1
Start() return 1 for C1, and 2 for C2.
S1 sends out an AppendEntries to S2 containing C1 and C2, but all its other messages are lost.
S3 steps forward as a candidate.
S1 and S2 won’t vote for S3, but S3, S4, and S5 all will, so S3 becomes the leader.
Another client request, C3 comes in to S3.
S3 calls Start() (which returns 1)
S3 sends an AppendEntries to S1, who discards C1 and C2 from its log, and adds C3.
S3 fails before sending AppendEntries to any other servers.
S1 steps forward, and because its log is up-to-date, it is elected leader.
Another client request, C4, arrives at S1
S1 calls Start(), which returns 2 (which was also returned for Start(C2).
All of S1’s AppendEntries are dropped, and S2 steps forward.
S1 and S3 won’t vote for S2, but S2, S4, and S5 all will, so S2 becomes leader.
A client request C5 comes in to S2
S2 calls Start(), which returns 3.
S2 successfully sends AppendEntries to all the servers, which S2 reports back to the servers by including an updated leaderCommit = 3 in the next heartbeat.
Since S2’s log is [C1 C2 C5], this means that the entry that committed (and was applied at all servers, including S1) at index 2 is C2. This despite the fact that C4 was the last client operation to have returned index 2 at S1.

The four-way deadlock: All credit for finding this goes to Steven Allen, another 6.824 TA. He found the following nasty four-way deadlock that you can easily get into when building applications on top of Raft.

Your Raft code, however it is structured, likely has a Start()-like function that allows the application to add new commands to the Raft log. It also likely has a loop that, when commitIndex is updated, calls apply() on the application for every element in the log between lastApplied and commitIndex. These routines probably both take some lock a. In your Raft-based application, you probably call Raft’s Start() function somewhere in your RPC handlers, and you have some code somewhere else that is informed whenever Raft applies a new log entry. Since these two need to communicate (i.e., the RPC method needs to know when the operation it put into the log completes), they both probably take some lock b.

In Go, these four code segments probably look something like this:

---------
Foundation DB batches of (commits? commands?) at
once sounded pretty essential.


*/
