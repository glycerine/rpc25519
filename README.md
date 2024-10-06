edwardsRPC: Edwards curve ed25519 based identity RPC for Go/golang
==========

TLS-1.3 over TCP; TLS-1.3 over QUIC as transports.
----------------


How to KYC or Know Your Clients
------------------------

How do we identify our clients in an RPC situation?

We have clients and servers in RPC. For that matter,
how do we authenticate the server too?

The identity of an both client and server, either
end of a connection, is established
with a private-key/public-key pair in which the
remote party proves posession of a private-key and
we can confirm that the associated public-key has been signed
by our certificate authority. Public keys signed in
this way are more commonly called certificates or "certs".

An email address for convenience of identification
of jobs can be attached to the public key (certificate).

This is a much saner approach that tying an agent
or query or work-load identity to a specific machine, 
domain name, or machine port.

All these things are artifacts of the network design
and not reliable identitiers. Only access to the
private key corresponding to our cert should convey identity
during a TLS (Diffie-Hellman) handshake.

TOFU or not
-----------

New clients can be accepted, or rejected, depending
on the writability of the known_client_keys file.

Trust on first use, or TOFU, is used if the 
known_client_keys file is writable. 
When the file is writable and a new client arrives,
their public key and email (if available)
are stored. This is similar to how the classic
~/.ssh/known_hosts file works, but with a twist.

The twist is that once the desired identities have
been recorded, the file can be used to reject
any new or unknown certs from new or unkown clients.

If the file has been made unwritable (say with chmod -w known_client_keys),
then we are "locked down". In this case, TOFU is not allowed and only existing
keys will be accepted. Any others whose public keys are not in the
known_client_hosts file will be dropped during the TLS handshake.

These unknown clients are usually just attackers that should
in fact be rejected soundly. In case of misconfiguration
however, all clients should be prepared to timeout as, if
they are not on the list, they will be
shunned and shown no further attention or packets. Without
a timeout, they may well hang indefinitely waiting for
network activity.


encryption details
----------

Modern Ed25519 keys are used with TLS-1.3. The TLS_CHACHA20_POLY1305_SHA256
cipher suite is the only one configured. 

generating keys with emails attached
---------------

Replace froggy@example.com in the openssl-san.cnf with the
email of the person running the job, so they can be
contacted if there is problem. 

Then run the gen.sh script to generate a directory
called 'static' that will hold the key pairs.
A self-signing certficiate authority private key
will be place in my-keep-private-dir.

The Go code in selfcert/ will allow programmatic
creation of new identities. It is the equivalent
of the gen.sh script.

the private parts, embedded
---------------------------

On running make, by default the 'static' directory will be embedded
in client and server executables. This means that
the binary executables contain sensitive private
keys, and should be kept confidential and secure.

While this is convenient for deployment, just a single
binary, it does make it difficult to rotate keys.
Normally one would just compile a new binary with
new keys, but if one wishes to rotate keys only,
then there is also the ability to read keys
from disk rather from the embedded files system.

The certificate authority private key is never included
in the binary. However, the private keys for the
client and server must be available in order to
handshake, and so they are included. This means
that anyone possessing the binary (just as anyone
possessing the private key files deployed on disk)
could use that private key to man-in-the-middle
the communication. Mostly this is moot: if you
don't trust the computer you are running the
binary on, you equally cannot trust the private
key sitting on the filesystem. Hence there is 
generally no loss of security from embedding the keys into
the binary, as long as one protects the binary as
would one's private keys; do not publish to the world
the compiled binary with the embedded keys.

A localhost by any other name would not smell as sweet
------------------------------------

The server name will always be 'localhost' on these
certificates, and this is critical to allowing
our processes to run on any machine. By doing so,
we leverage the SNI technology[1], 
to break the troublesome adhesion of domain
name, port and certificate. SNI was
developed to let a single IP address host many 
different web sites. We want multiple
IP addresses to run many different jobs, and
to be able to migrate jobs between hosts.
Brilliantly, SNI lets us move our clients and servers
between machines without having to re-issue
the certificates. The certs are always
issued to 'localhost' and clients always
request ServerName 'localhost' in their
tls.Config.ServerName field.

[1]  https://en.wikipedia.org/wiki/Server_Name_Indication ,
