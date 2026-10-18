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

On Unix, jsync preserves file and directory permission bits. When the receiving
process runs as root, it maps source owner and group names to destination
accounts, including symlink ownership. User and group IDs may differ between
hosts. Missing or unknown usernames fall back to UID 0 with setuid cleared;
missing or unknown group names fall back to GID 0 with setgid cleared.
Account maps are loaded once per process from `/etc/passwd` and `/etc/group`
and sent once at the beginning of a transfer to a root taker. Per-file metadata
contains only `OwnershipConveyed`, `UID`, and `GID`; these are all zero and
omitted from the wire for unprivileged takers. The receiver is `jsrv` for a
push and `jcp` for a pull.
Unprivileged receivers skip ownership changes. Ownership is applied before
permissions so legitimate setuid/setgid bits survive `chown`.

The root regression test can be run from the repository root with:

```sh
sudo go test ./jsync -run '^TestRoot' -count=1
```

These tests skip when not run as root. `TestPreservesPermissions` and
`TestNonRootReceiverSkipsOwnership` cover ordinary unprivileged receivers.
