jsync: an rsync-like protocol
======

By implementing an rsync-like protocol over
top of rpc25519, we exercise the system,
flushing out bugs and making sure the
system can be used for real work. It also
provides a medium sized example of using
rpc25519.

The jsync-protocol itself may also be useful, in
and of itself, in applications.
