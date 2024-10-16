rpc25519: Edwards curve ed25519 based identity RPC for Go/golang
==========

Motivation: I needed a small, simple, and compact RPC system
with modern, strong cryptography. Other RPC systems were too
sprawling, had bugs their devs would not address, or could
not be properly or easily secured. To that end, `rpc25519` only uses
ed25519 Edwards curve based public-key cryptography. Moreover
it supports QUIC as a transport, which is very fast even
though it is always encrypted, due to its 0-RTT implementation
and the mature [quic-go](https://github.com/quic-go/quic-go) 
implementation of the protocol.

In the broadest of strokes:

`rpc25519` is a Remote Procedure Call system with two APIs.

We offer both a traditional [net/rpc](https://pkg.go.dev/net/rpc) 
style API, and generic []byte oriented API for carrying
untyped []byte payloads (in `Message.JobSerz`).

Using the rpc25519.Message based API:

 * `Server.Register1Func()` registers one-way (no reply) callbacks on the server; and
 * `Server.Register2Func()` register traditional two-way callbacks.

Using the net/rpc API:

 * `Server.Register()` registers structs with callback methods on them.

See [the net/rpc docs for full guidance on using that API](https://pkg.go.dev/net/rpc).

* Extended method types:

Callback methods in the `net/rpc` style traditionally look like this first
`NoContext` example. We allow a context.Context as an additional first
parameter. The ctx will have an "HDR" value set on it giving a pointer to
the `rpc25519.HDR` header from the incoming Message. 

~~~
func (s *Service) NoContext(args *Args, reply *Reply) error

func (s *Service) GetsContext(ctx context.Context, args *Args, reply *Reply) error {
	if hdr := ctx.Value("HDR"); hdr != nil {
		h, ok := hdr.(*rpc25519.HDR)
		if ok {
			fmt.Printf("WantsContext called with HDR = '%v'; "+
			    "HDR.Nc.RemoteAddr() gives '%v'; HDR.Nc.LocalAddr() gives '%v'\n", 
			    h.String(), h.Nc.RemoteAddr(), h.Nc.LocalAddr())
		}
	} else {
		fmt.Println("HDR not found")
	}
}
~~~

TODO: The client does not transmit context to server at the moment.

The net/rpc API is implemented as a layer on top of the rpc25519.Message
based API. Both can be used concurrently if desired.

Server push is available too. Use [Client.GetReadIncomingCh](https://pkg.go.dev/github.com/glycerine/rpc25519#Client.GetReadIncomingCh) or [Client.GetReads](https://pkg.go.dev/github.com/glycerine/rpc25519#Client.GetReads) on the client side to received server initiated messages.

In the following we'll look at choice of transport, why
public-key certs are preferred, and how to use the included `selfy`
tool to easily generate self-signed certificates.

TLS-v1.3 over TCP
----------------

Three transports are available: TLS-v1.3 over TCP, 
plain TCP, and QUIC which uses TLS-v1.3 over UDP.

QUIC is so much faster than even plain TCP, it
should probably be your default choice. 

The only difficulty comes to IPv6 networks.
They are not very friendly to QUIC. Typically they have
minimal 1280 MTU, so we'll have a problem if any kind of 
VPN layer is in use. Tailscale/Wireguard
add an extra 28 bytes of ICMP/IP headers and try to keep packets 1:1,
thus going over the MTU and not really working. IPv6
cannot fragment packets in the network, so they just
get rejected outright. This is my surmize. I could
be misunderstanding the root cause. The fact remains
that in testing over IPv6 VPN links, rpc25519's QUIC
nodes could not talk to each other.

Context: https://github.com/tailscale/tailscale/issues/2633

Comments/links: https://gist.github.com/jj1bdx/1adac3e305d0fb6dee90dd5b909513ed

At the moment we will detect and panic in the
case an IPv6 network is used to warn you
of this issue.

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
of jobs can be listed in the certificate.

This is a much saner approach than tying a
work-load identity to a specific machine, 
domain name, or machine port. These incidental
details are artifacts of the network design, easily
spoofed or phished, and are not reliable or desirable 
identitiers. Only access to the private key corresponding to our cert should convey identity
during a TLS handshake. The later part of the handshake 
verifies that the key was signed by our CA. 
This should suffice.

Such a requirement maintains the strongest security but still allows
elastic clouds to grow and shrink and migrate
the workloads of both clients and servers. It allows
for sane development and deployment on any hardware a developer
can access with ssh, without compromising on encryption on
either end.


TOFU or not
-----------

By creating (touch will do) a known_client_keys file on the server
directory where the server is running, you activate
the key tracking system on the server.

Similarly, by touching the known_server_keys file
in the directory where the client is running, you
tell the client to record the server certificates
it has seen.

Without these files, no history of seen certs is
recorded. While certs are still checked (assuming
you have left SkipVerifyKeys at the default false 
value in the config), they are not remembered.

In a typical use case, touch the file to record
the certs in use, then make the known keys file
read-only to block out any new certs.

This works because clients are accepted or rejected depending
on the writability of the known_client_keys file,
and the presence of their certificates in that file.

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
cipher suite is the only one configured. This is similar
to the crypto suite used in Wireguard (TailScale), 
the only difference being that Wireguard uses
Blake2s as a hash function (apparently faster than SHA-256
when hardware support SHA-extensions/SHA-NI instructions are not available).
The ChaCha20, Poly1305, and Curve25519 parts are the same.

security posture for both extremes
----------------

The strength of security is controlled by the Config options
to NewServer() and NewClient().

See the cli.go file. 

By default security is very strong, requiring TLS-1.3 and valid
signed client certs, but allowing TOFU for previously unseen clients
who come bearing valid certs; those signed by our CA.

This allows one to test one's initial setup with a minimum of fuss. 

Further hardening (into a virtual fortress) can then be 
accomplished by making read-only the set of already
seen clients; with `chmod -w known_client_keys`.

With this done, only those clients who we have already
seen will be permitted in; and these are clients bearing
proper certs signed by our CA private key; this will be
verified during the TLS handshake. Any others will be rejected.

On the other extreme, setting TCPonly_no_TLS to true means
we will use only TCP, no TLS at all, and everything will
be transmitted in clear text.

In the middle of the road, 
setting Config.SkipVerifyKeys to true means the server will act
like an HTTPS web server: any client with any key
will be allowed to talk to the server, but TLS-1.3
will encrypt the traffic. Whenever TLS is used,
it is always TLS-1.3 and set to the
tls.TLS_CHACHA20_POLY1305_SHA256 cipher suite. Thus
this setting is of no use for authenticating clients; it
should almost never be used since it opens up your
server to the world. But for web servers, this may be desirable.

Note that both sides of the connection (client and server) must agree to
-skip-verify (Config.SkipVerifyKeys = true); otherwise the signing
authority (CA) must agree. Under the default, SkipVerifyKeys = false, 
by signing only your own keys, with
your own CA, that you keep private from the world, you maintain 
very tight control over access to your server.

We only create and only accept Ed25519 keys (with SkipVerifyKeys off/false,
the default). (See `selfy` below, or the included gen.sh script
to create keys).


~~~
// Config says who to contact (for a client), or
// where to listen (for a server); and sets how
// strong a security posture we adopt.
type Config struct {

	// ServerAddr host:port of the rpc25519.Server to contact.
	ServerAddr string

	// false means TLS-1.3 secured. true here means do TCP only.
	TCPonly_no_TLS bool

	// path to certs/ like certificate directory 
	// on the live filesystem. If left
	// empty then the embedded certs/ from build-time, those 
	// copied from the on-disk certs/ directory and baked 
	// into the executable as a virtual file system with
	// the go:embed directive are used.
	CertPath string

	// SkipVerifyKeys true allows any incoming
	// key to be signed by
	// any CA; it does not have to be ours. Obviously
	// this discards almost all access control; it
	// should rarely be used unless communication
	// with the any random agent/hacker/public person
	// is desired.
	SkipVerifyKeys bool
	
	// This is not a Config option, but creating
	// the known key file on the client/server is
	// typically the last security measure in hardening.
	//
	// If known_client_keys exists on the server,
	// then we will read from it.
	// Likewise, if known_server_keys exists on
	// the client, then we will read from it.
	//
	// If the known keys file is read-only: Read-only
	// means we are in lockdown mode and no unknown
	// client certs will be accepted, even if they
	// have been properly signed by our CA.
	//
	// If the known keys file is writable then we are
	// Trust On First Use mode, and new remote parties
	// are recorded in the file if their certs are valid (signed
	// by us/our CA).
	//
	// Note if the known_client_keys is read-only, it
	// had better not be empty or nobody will be
	// able to contact us. The server will notice
	// this and crash since why bother being up.

	...
}
~~~


The `selfy` tool: create new keys quickly; view certificates
---------------------

Certificates are great, but making them has traditionally
been a massive pain. That is why I wrote `selfy`.

The `selfy` command is an easy way to create private keys, certificates,
and self-signed certficate authories. It is vastly more usable than
mountain of complexity that is `openssl`, but more limited in scope.
In our opinion, this a good thing.

If you are in a hurry to get started, the most basic 
use of `selfy` is to create a new ed25519 key-pair:

~~~
$ selfy -k name_of_your_identity -e your@email.here.org
~~~

With other tools you would need to already have a CA (Certificate Authority).

But we've got your back. If you lack a CA in the default directory (see -p below), then,
for your convenience, a self-signed CA will be auto-generated for you.
It will then be used to sign the new cert. Hence the above
command is all that a typical developer needs to get started. Yay(!)

If you want to first create a CA manually, you can do that
with the `-ca` flag. The  `-p` flag will let you put it somewhere
other than the default directory.

~~~
selfy -ca # make a new self-signed Certificate Authority
~~~

Your newly created cert can be viewed with the `selfy -v` flag, just give it the
path to your .crt file.

~~~
$ selfy -v certs/name_of_your_identity.crt
~~~

By default, the CA is stored in the `./my-keep-private-dir/ca.{crt,key}`, 
while the -k named identifying cert is stored in `certs/name.crt`.
The corresponding private key is stored in `certs/name.key`.

Update: we have added pass-phrase protection to the private keys by default.
In order to forgo this protection and use the original behavior, supply the
`selfy --nopass` flag.

~~~
$ selfy -h

Usage of selfy:
  -ca
    	create a new self-signed certificate authority (1st of 2 
		steps in making new certs). Written to the -p directory 
		(see selfy -p flag, where -p is for private).
		The CA uses ed25519 keys too.
		
  -e string
    	email to write into the certificate (who to contact 
		about this job) (strongly encouraged when making 
		new certs! defaults to name@host if not given)
		
  -k string
    	2nd of 2 steps: -k {key_name} ; create a new ed25519 
		key pair (key and cert), and save it to this name. 
		The pair will be saved under the -o directory; 
		we strongly suggest you also use the 
		-e your_email@actually.org flag to 
		describe the job and/or provide the owner's email 
		to contact when needed. A CA will be auto-generated 
		if none is found in the -p directory, which has 
		a default name which warns the user to protect it.

  -nopass
    	by default we request a password and use 
		it with Argon2id to encrypt private key files (CA & identity). 
		Setting -nopass means we generate an un-encrypted 
		private key; this is not recommended.

  -o string
    	directory to save newly created certs into. (default "certs")
		
  -p string
    	directory to find the CA in. If doing -ca, we will save 
		the newly created Certificate Authority (CA) private 
		key to this directory. If doing -k to create a new key, 
		we'll look for the CA here. (default "my-keep-private-dir")  
		
  -v string
    	path to cert to view. Similar output as the openssl 
		command: 'openssl x509 -in certs/client.crt  -text -noout', 
		which you could use instead; just replace certs/client.crt 
		with the path to your cert.
~~~

The `openssl` commands in the included gen.sh script do the same things as
`selfy` does, but it is more painful to incorporate an email because
you have to modify the `openssl-san.cnf` file to do so each time.

generating keys with emails inside
---------------

See the `selfy -e` flag above for details.

Delightfully, email addresses can be stored 
in certificates! 

This provides a fantastically convenient way to identify the
job and/or the owner of the job. Importantly,
if you are supporting work loads from many
people, this can be critical in telling who
needs to know when something goes wrong with
a job.


A localhost by any other name would not smell as sweet
------------------------------------

The server name will always be 'localhost' on `selfy`
generated certificates, and this is critical to allowing
our processes to run on any machine. By doing so,
we leverage the SNI technology[1], 
to break the troublesome adhesion of IP address
and certificate. SNI was
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

This is much more like the convenience and
usability `ssh`. To me `ssh` has 
always been so much easier to deal with than
certificates. rpc25519 aims for
usability on par with `ssh`. Leveraging
SNI is one way we get there.

[1]  https://en.wikipedia.org/wiki/Server_Name_Indication ,

-----------
Author: Jason E. Aten, Ph.D.

License: See the LICENSE file for the 3-clause BSD-style license; the same as Go.
