package rpc25519

// simnet.go discussion/learnings:
//
// Question: Should I call synctest.Wait(), then Sleep()?
// OR should I call time.Sleep(), then synctest.Wait()?
// Short answer: time.Sleep() then syntest.Wait().
// Rationale: after Sleep(), the only goroutines that
// are awake at this moment in faketime are the ones
// who should be awake, and after Wait() we are
// guaranteed to be the only goroutine that is
// running.
//
// When I first read https://go.dev/blog/synctest it
// looked backwards to me. What is the point of
// doing a sleep and THEN doing a synctest.Wait?
// Isn't the sleep wasted? I've just woken up
// again after the sleep, so any number of other
// goroutines could also have woken up too... how
// can I reason about anything now with all
// the possible concurrency?
//
// Let's further ask:
// Does fake time ONLY advance when all goro are blocked?
//
// Does the package synctest give that guarantee?
//
// Can time advance in some (any) other manner? Is
// this other way guaranteed to be the ONLY other way?
//
// Let's make assumptions about the things that
// were vague and not spelled out in the blog post.
//
// Let's assume for a moment that time can ONLY advance when
// all goro are blocked? The adjective durably
// was not applied to the blog post statement,
// "Time advances in the bubble when all goroutines are blocked."
// so let's further assume that he meant _durably_ blocked.
// Although if @neild meant durably he probably would
// have said durably (maybe?)
//
// jea: since we were blocked, other goro can
// run until blocked in the background; in
// fact, time won't advance until they are
// blocked. So once we get to here, we
// know:
// 1) all other goroutines were blocked
// until a nanosecond before now.
// 2) We are now awake, but also any
// other goro that is due to wake at
// this moment is also now awake as well.
//
// Still under those assumptions, now ask:
// What does the synctest.Wait() do for us?
//
//synctest.Wait()
//
// okay, we now know that all _other_ goroutines
// that woke up when we did just now
// have also finished their business and are
// now durably blocked. We are the only
// one that has any more business to do
// at this time point. So this is useful in
// that we can now presume to "go last" in
// this nanosecond, with the assurance that
// we won't miss a send we were supposed
// to get at this point in time. During
// this nanosecond, the order of who does
// what is undefined, and lots of other
// goro could be doing stuff before the
// synctest.Wait(). But, now, after the
// synctest.Wait, we know they are all done,
// and we are the only one who will
// operate until the next time we sleep.
// In effect, we have frozen time just
// at the end of this nanosecond. We
// can adjust our queues, match sends and reads,
// set new timers, process expired timers,
// change node state to partitioned, etc,
// with the full knowledge that we are
// not operating concurrently with anyone
// else, but just after all of them have
// done any business they are going to
// do at the top of this nanosecond.
// We are now "at the bottom" of the nanosecond.

// What if we did the wait, then slept?
// After the wait, we also know that
// everyone else is durably blocked.
// So in fact, we don't need to sleep
// ourselves to know that nobody else
// will be doing anything concurrently.
// It is guaranteed that we are the last,
// no matter what time it is. In fact,
// we might want to have other goroutines
// do stuff for us now, respond to us,
// and take any actions they are going
// to take, get blocked on timers or
// reads or sends. They can do so, operating
// independently and concurrently with us,
// until the next sleep or synctest.Wait(),
// *IF* we want to be operating with them
// concurrently (MUCH more non-deterministic!)
// then this is fine. BUT if we want the
// determinism guarantee that we are
// "going last" and have seen everyone
// else's poker hand before placing our bets,
// then we will want to do the Sleep then
// Wait approach.
//
// Let's write a test to check that we are
// indeed "last" after the double shot of
// sleep + sycntest.Wait? How can we know
// if there is anyone else "active"? simple:
// block ourselves with a select{}. If we
// were the only one's active, then the
// synctest system will panic, reporting deadlock.
//
// Possibly very interesting, the blog says,
//
// "The Run function starts a goroutine in a new
// bubble. It returns when every goroutine in
// the bubble has exited. It panics if the bubble
// is durably blocked and _cannot be unblocked
// by advancing time_." (emphasis mine)
//
// To me this implies that we can use the panic
// as a poor man's model checker for some cases
// of the temporal logic invariants "never happens", or
// "always happens".
//
// What about concurrent calls to synctest.Wait?
// Well, they seem like they would be useful...
// letting all goroutines accumulate against
// the same barrier... but then when you
// come out the other side, waking from syntest.Wait,
// now multiple goroutines could be active
// at once again!  This is exactly what we
// wanted to avoid. We want exclusive access
// while time is frozen "in between two clock ticks"
// to change lots of state before we resume
// the clock for other goroutines again. So
// this is less than useful--it actively hurts
// our aims.
//
// "Operating on a bubbled channel from outside
// the bubble panics."
// This sounds like a great way to "poison"
// a channel! Like if you have to pass from
// bubble, out into "enemy" territory, and
// then back into the safe bubble zone,
// you get the runtime's help: the runtime
// insures that nobody else will read from
// your channel.
//
// The converse might also be useful: since
// a select on an "outside" channel prevents
// synctest.Wait() from returning, this
// could be useful (maybe?) if a worker/node
// wants to prevent the schduler from
// blocking it too early while it does
// some sends and receives on channels.
// I don't see why this would be needed at
// the moment, but keep in the back
// pocket of ideas for the future. The
// outside channel could be a global
// system "scheduler" control mechanism
// if it wanted to pause an instance
// of the scheduler, for example.
// I did a POC for that in the subdirectory
// play/pausable_scheduler_demo_test.go

// comment on the sleep, wait sequence in simnet.go:
//
// Advance time by one tick [in the heart of the scheduler].
//
// This will wake up and run any goroutines
// that should be run before our sleep timer
// goes off, as it should, BUT... when they
// try to contact us, they will block since
// we are asleep. This is actually a good thing!
//
// It delays their action from continuing
// until our time.Sleep() timer expires and
// we are woken to service them. Thus all
// simulated service will occur at this
// new "now" point in faketime.
//
// Once we return from the sleep,
// every goroutine that should be asleep "now"
// will be asleep. The only active ones are
// those that are doing something they should
// be at this new "now" time. We let them
// finish first, before us, by calling
// synctest.Wait(). Once synctest.Wait()
// returns, we know with certainly that we
// are the only non-durably-blocked goroutine.
// Notice the synctest package forbids ever
// having two outstanding synctest.Wait()
// calls at once:
// "[synctest.Wait] panics if two goroutines
// in the same bubble call Wait at the same time."
//		time.Sleep(s.scenario.tick)

//		if s.useSynctest && globalUseSynctest {
// synctestWait_LetAllOtherGoroFinish is defined in
// simnet_synctest.go for synctest is on, and in
// simnet_nosynctest.go as a no-op when off.

//vv("about to call synctestWait_LetAllOtherGoroFinish")

// "Goroutines in the bubble started by Run use a
// fake clock. Within the bubble, functions in the
// time package operate on the fake clock.
// Time advances in the bubble when all goroutines are blocked."
// -- https://go.dev/blog/synctest

// synctest.Wait when its tag in force. Otherwise a no-op.
//			synctestWait_LetAllOtherGoroFinish()
//vv("back from synctest.Wait() goro = %v", GoroNumber())

// synctestWait_LetAllOtherGoroFinish is defined in
// simnet_synctest.go for synctest is on, and in
// simnet_nosynctest.go as a no-op when off.

// graph kept in simnet.circuits:
//
// circuits[A][B] is the very cyclic (bi-directed?) graph
// of the network.
//
// The simnode.name is the key of the circuits map.
// Both client and server names are keys in circuits.
//
// Each A has a bi-directional network "socket" to each of circuits[A].
//
// circuits[A][B] is A's connection to B; that owned by A.
//
// circuits[B][A] is B's connection to A; that owned by B.
//
// The system guarantees that the keys of circuits
// (the names of all simnodes) are unique strings,
// by rejecting any already taken names (panic-ing on attempted
// registration) during the moral equivalent of
// server Bind/Listen and client Dial. The go map
// is not a multi-map anyway, but that is just
// an implementation detail that happens to provide extra
// enforcement. Even if we change the map out
// later, each simnode name in the network must be unique.
//
// Users specify their own names for modeling
// convenience, but assist them by enforcing
// global name uniqueness.
//
// See handleServerRegistration() and
// handleClientRegistration() where these
// panics enforce uniqueness.
//
// Technically, if the simnode is backed by
// rpc25519, each simnode has both a rpc25519.Server
// and a set of rpc25519.Clients, one client per
// outgoing initated connection, and so there are
// redundant paths to get a message from
// A to B through the network. This is necessary
// because only the Client in TCP is the active initator,
// and can only talk to one Server. A Server
// can always talk to any number of Clients,
// but typically must begin passively and
// cannot initiate a connection to another
// server. On TCP, rpc25519 enables active grid
// creation. Each peer runs a server, and
// servers establish the grid by automatically creating an
// internal auto-Client when the server (peer)
// wishes to initate contact with another server (peer).
// The QUIC version is... probably similar;
// since QUIC was slower I have not thought
// about it in a while--but QUIC can use the
// same port for client and server (to simplify
// diagnostics).
//
// The simnet tries to not care about these detail,
// and the rpc25519 peer system is symmetric by design.
// Thus it will forward a message to the peer no
// matter where it lives (be it technically on an
// rpc25519.Client or rpc25519.Server).
//
// The isCli flag distinguishes whether a given
// simnode is on a Client or Server when
// it matters, but we try to minimize its use.
// It should not matter for the most part; in
// the ckt.go code there is even a note that
// the peerAPI.isCli can be misleading with auto-Clients
// in play. The simnode.isCli however should be
// accurate (we think).
//
// Each peer-to-peer connection is a network
// simnode that can send and read messages
// to exactly one other network simnode.
//
// Even during "partition" of the network,
// or when modeling faulty links or network cards,
// in general we want to be maintain the
// most general case of a fully connected
// network, where any peer can talk to any
// other peer in the network; like the internet.
// I think of a simnet as the big single
// ethernet switch that all circuits plug into.
//
// When modeling faults, we try to keep the circuits network
// as static as possible, and set .deafRead
// or .dropSend flags to model faults.
// Reboot/restart should not heal net/net card faults.
//
// To recap, both clinode.name and srvnode.name are keys
// in the circuits map. So circuits[clinode.name] returns
// the map of who clinode is connected to.
//
// In other words, the value of the map circuits[A]
// is another map, which is the set of circuits that A
// is connected to by the simconn circuits[A][B].
