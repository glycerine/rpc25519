rpc25519: ed25519 based RPC for Go/golang
==========

* Recent News (2024 December 28): request cancellation support with context.Context

Recently implemented: context.Context based cancellation. 

details:

If the client closes the cancelJobCh that 
was supplied to a previous SendAndGetReply()
call, a cancellation request will be sent to the server. 

On the server, the HDR.Ctx will be set, having be created
by context.WithCancel; so the HDR.Ctx.Done() channel can
be honored.

Similarly, for traditional net/rpc calls, the Client.Go()
and Client.Call() methods will respect the optional octx
context and transmit a cancellation request to the 
server. On the server, the net/rpc methods registered must
(naturally) be using the first-parameter ctx form of registration in
order to be cancellation aware.

Any client-side blocked calls will return on cancellation.
The msg.LocalErr for the []byte oriented Message API
will be set. For net/rpc API users, the Client.Go()
returned call will have the call.Error set. 
Users of Client.Call() will see the returned error set.

In all cases, the error will be ErrCancelReqSent if the call was
in flight on the server, but will be ErrDone if 
the original call had not been transmitted yet.

Note that context-based cancellation requires the cooperation
of the registered server side call implementations.
Go does not support goroutine cancelation, so the
remote methods must implement and honor context awareness in order
for the remote cancellation message to have effect.

# overview

Motivation: I needed a small, simple, and compact RPC system
with modern, strong cryptography 
for [goq](https://github.com/glycerine/goq). 
To that end, `rpc25519` uses only
ed25519 keys for its public-key cryptography. A
pre-shared-key layer can also be configured
for post-quantum security.

Excitedly, I am delighted to report this package also
supports [QUIC as a transport](https://en.wikipedia.org/wiki/QUIC). 
QUIC is very fast even
though it is always encrypted. This is due to its 0-RTT design
and the mature [quic-go](https://github.com/quic-go/quic-go) 
implementation of the protocol. QUIC allows a local
client and server in the same process to share a UDP port.
This feature can be useful for conserving ports
and connecting across networks.

After tuning and hardening, the UDP/QUIC versus TCP/TLS 
decision is not really difficult if the client
is new every time. In our measurements, TLS (over TCP) has
both better connection latency and better throughput
than QUIC (over UDP). QUIC does not get to take advantage of
its optimization for 0-RTT re-connection under
these circumstances (when the client is new). 
If you have clients that frequently re-connect after 
loosing network connectivity,
then measure QUIC versus TLS/TCP in your application. 
Otherwise, for performance, prefer TLS over TCP. The
latency of TLS is better and, moreover, the
throughput of TLS can be much better (4-5x greater).
If client port re-use and conservation is a needed, then
QUIC may be your only choice.

The [rpc25519 package docs are here](https://pkg.go.dev/github.com/glycerine/rpc25519). 

Benchmarks versus other rpc systems are here: https://github.com/glycerine/rpcx-benchmark

~~~
# to install/get started: 
#   *warning: ~/go/bin/{srv,cli,selfy,greenpack} are written
#

 git clone https://github.com/glycerine/greenpack ## pre-req
 cd greenpack; make; cd ..
 git clone https://github.com/glycerine/rpc25519
 cd rpc25519;  make
 
 # make test keys and CA: saved to certs/ and my-keep-private-dir/
 ./selfy -k client -nopass; ./selfy -k node -nopass 
 ./selfy -gensym psk.binary ## saved to my-keep-private-dir/psk.binary
 make run && make runq  ## verify TLS over TCP and QUIC
 
~~~

For getting started, see the small example programs here: https://github.com/glycerine/rpc25519/tree/master/cmd . These illustrate client (`cli`), server (`srv`), and QUIC port sharing 
by a client and a server (`samesame`). The tests in srv_test.go and 
cli_test.go also make great starting points.

overview
--------

[`rpc25519`](https://github.com/glycerine/rpc25519) is a Remote Procedure Call (RPC) system with two APIs.

We offer both a traditional [net/rpc](https://pkg.go.dev/net/rpc) 
style API, and a generic []byte oriented API for carrying
user typed or self describing []byte payloads (in `Message.JobSerz`). 

As of v1.1.0, the `net/rpc` API has been updated to use 
[greenpack encoding](https://pkg.go.dev/github.com/glycerine/greenpack2)
rather than gob encoding, to provide a self-describing, 
evolvable serialization format. Greenpack allows fields to be added
or deprecated over time and is multi-language compatible.
We re-used net/rpc's client-facing API layer, and
wired it into/on top of our native []byte slice `Message` transport infrastructure.
(The LICENSE file reflects this code re-use.) Instead
of taking `any` struct, arguments and responses must now have greenpack
generated methods. Typically this means adding
`//go:generate greenpack` to the files that define the
structs that will go over the wire, and running `go generate`.

`rpc25519` was built originally for the distributed job management 
use-case, and so uses TLS/QUIC directly. It does not use http,
except perhaps in the very first contact:
like `net/rpc`, there is limited support for 
http CONNECT based hijacking; see the
cfg.HTTPConnectRequired flag. Nonetheless, this protocol remains
distinct from http. In particular, note that the connection hijacking does
not work with (https/http2/http3) encrypted protocols.

The generic byte-slice API is designed to work smoothly 
with our [greenpack serialization format](https://github.com/glycerine/greenpack)
that requires no extra IDL file. See the https://github.com/glycerine/rpc25519/blob/master/hdr.go#L18 file herein, for example.

Using the rpc25519.Message based API:

 * [`Server.Register1Func()`](https://pkg.go.dev/github.com/glycerine/rpc25519#Server.Register1Func) registers one-way (no reply) callbacks on the server. They look like this:

~~~
  func ExampleOneWayFunc(req *Message) { ... }
~~~

 * [`Server.Register2Func()`](https://pkg.go.dev/github.com/glycerine/rpc25519#Server.Register2Func) registers traditional two-way callbacks. They look like this:

~~~
  func ExampleTwoWayFunc(req *Message, reply *Message) error { ... }
~~~
The central Message struct itself is simple.
~~~
  type Message struct {

   // HDR contains header information. See hdr.go.
   HDR HDR `zid:"0"`

   // JobSerz is the "body" of the message.
   // The user provides and interprets this.
   JobSerz []byte `zid:"1"`

   // JobErrs returns error information from 
   // user-defined callback functions. If a 
   // TwoWayFunc returns a non-nil error, its
   // err.Error() will be set here.
   JobErrs string `zid:"2"`

   // LocalErr is not serialized on the wire.
   // It communicates only local (client/server side) 
   // API information. For example, Server.SendMessage() or
   // Client.SendAndGetReply() can read it after
   // DoneCh has been received on.
   //
   // Callback functions should convey 
   // errors in JobErrs (by returning an error); 
   // or in-band within JobSerz.
   LocalErr error `msg:"-"`

   // DoneCh will receive this Message itself when the call completes.
   // It must be buffered, with at least capacity 1.
   // NewMessage() automatically allocates DoneCh correctly and
   // should always be used when creating a new Message.
   DoneCh chan *Message `msg:"-"`
}
~~~

Using the net/rpc API:

 * [`Server.Register()`](https://pkg.go.dev/github.com/glycerine/rpc25519#Server.Register) registers structs with callback methods on them. For a struct called `Service`, this method would be identified and registered:

~~~
  func (s *Service) NoContext(args *Args, reply *Reply) error { ... }
~~~

See [the net/rpc docs for full guidance on using that API](https://pkg.go.dev/net/rpc).

* Extended method types:

Callback methods in the [net/rpc](https://pkg.go.dev/net/rpc) 
style traditionally look like the `NoContext` method above. 
We also allow a ctx context.Context as an additional first
parameter. This is an extension to what `net/rpc` provides.
The ctx will have an "HDR" value set on it giving a pointer to
the `rpc25519.HDR` header from the incoming Message. 

~~~
func (s *Service) GetsContext(ctx context.Context, args *Args, reply *Reply) error {
if hdr := ctx.Value("HDR"); hdr != nil {
     h, ok := hdr.(*rpc25519.HDR)
     if ok {
        fmt.Printf("GetsContext called with HDR = '%v'; "+
           "HDR.Nc.RemoteAddr() gives '%v'; HDR.Nc.LocalAddr() gives '%v'\n", 
           h.String(), h.Nc.RemoteAddr(), h.Nc.LocalAddr())
      }
   } else {
      fmt.Println("HDR not found")
   }
   ...
   return nil
}
~~~

The net/rpc API is implemented as a layer on top of the rpc25519.Message
based API. Both can be used concurrently if desired.

In the Message API, server push is available. Use [Client.GetReadIncomingCh](https://pkg.go.dev/github.com/glycerine/rpc25519#Client.GetReadIncomingCh) or [Client.GetReads](https://pkg.go.dev/github.com/glycerine/rpc25519#Client.GetReads) on the client side to receive server initiated messages. To push from the server (in a callback func), see [Server.SendMessage](https://pkg.go.dev/github.com/glycerine/rpc25519#Server.SendMessage). An live application [example of server push is here](https://github.com/glycerine/goq/blob/master/xs.go#L186), in the `ServerCallbackMgr.pushToClient()` method.

See [the full source for my distributed job-queuing server `goq`](https://github.com/glycerine/goq)
as an example application that uses most all features of this, the `rpc25519` package.

In the following we'll look at choice of transport, why
public-key certs are preferred, and how to use the included `selfy`
tool to easily generate self-signed certificates.

TLS-v1.3 over TCP
----------------

Three transports are available: TLS-v1.3 over TCP, 
plain TCP, and QUIC which uses TLS-v1.3 over UDP.

QUIC is so much faster than even plain TCP, it
should probably be your default choice.

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

An email address for convenience in identifying 
jobs can be listed in the certificate.

This is a much saner approach than tying a
work-load identity to a specific machine, 
domain name, or machine port. Access to the 
private key corresponding to our cert should convey identity
during a TLS handshake. The later part of the handshake 
verifies that the key was signed by our CA. 
This suffices. We may also want to
reject based on IP address to block off clearly 
irrelevant traffic; both to for DDos mitigation 
and to keep our CPU cycles low, but these
are second order optimizations. The crytographic
proof is the central identifying factor.

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

`Config.PreSharedKeyPath` allows specifying a 32 byte pre-shared key
file for further security. An additional, independently keyed
layer of ChaCha20-Poly1305 stream cipher/AEAD will be applied 
by mixing that key into the shared secret from an ephemeral 
Elliptic Curve Diffie-Hellman handshake. The same pre-shared-key must be pre-installed 
on both client and server.


The pre-shared-key traffic is "tunnelled" or runs 
inside the outer encryption layer. Thus a different
symmetric encryption scheme could be wired in without
much difficulty.

The pre-shared-key file format is just raw random binary bytes. See
the srv_test.go `Test011_PreSharedKey_over_TCP` test in
https://github.com/glycerine/rpc25519/blob/master/srv_test.go#L297
for an example of using `NewChaCha20CryptoRandKey()`
to generate a key programmatically. Or just use
~~~
selfy -gensym my_pre_shared_key.binary
~~~
on the command line. For safety, `selfy -gensym` will 
not over-write an existing file. If you want 
to change keys, `mv` the old key out of the way first.

security posture for both extremes
----------------

The strength of security is controlled by the Config options
to NewServer() and NewClient(). This section was written
before we added the second symmetric encryption by
pre-shared key option. All comments below about 
lack of security (e.g. in TCPonly_no_TLS = true mode)
should be read modulo the pre-shared-key stuff: 
assume there is no 2nd layer.

See the cli.go file and the Config struct there;
also copied below.

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
type Config struct {

   // ServerAddr host:port where the server should listen.
   ServerAddr string

   // optional. Can be used to suggest that the
   // client use a specific host:port. NB: For QUIC, by default, the client and
   // server will share the same port if they are in the same process.
   // In that case this setting will definitely be ignored.
   ClientHostPort string

   // Who the client should contact
   ClientDialToHostPort string

   // TCP false means TLS-1.3 secured. 
   // So true here means do TCP only; with no encryption.
   TCPonly_no_TLS bool

   // UseQUIC cannot be true if TCPonly_no_TLS is true.
   UseQUIC bool

   // path to certs/ like certificate
   // directory on the live filesystem.
   CertPath string

   // SkipVerifyKeys true allows any incoming
   // key to be signed by
   // any CA; it does not have to be ours. Obviously
   // this discards almost all access control; it
   // should rarely be used unless communication
   // with the any random agent/hacker/public person
   // is desired.
   SkipVerifyKeys bool

   // default "client" means use certs/client.crt and certs/client.key
   ClientKeyPairName string 
   
   // default "node" means use certs/node.crt and certs/node.key
   ServerKeyPairName string 

   // PreSharedKeyPath locates an optional pre-shared
   // binary that  must be 32 bytes long.
   // If supplied, this key will be used in a symmetric 
   // encryption layer inside the outer TLS encryption.
   PreSharedKeyPath string

   // These are timeouts for connection and transport tuning.
   // The defaults of 0 mean wait forever.
   ConnectTimeout time.Duration
   ReadTimeout    time.Duration
   WriteTimeout   time.Duration

   ...
   
   // This is not a Config option, but creating
   // the known_{server,client}_keys file on the client/server is
   // typically the last security measure in hardening.
   //
   // If known_client_keys exists in the server's directory,
   // then we will read from it.
   // Likewise, if known_server_keys exists in
   // the client's directory, then we will read from it.
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
   
}
~~~


The `selfy` tool: create new keys quickly; view certificates
---------------------

Certificates are great, but making them has traditionally
been a massive pain. That is why I wrote `selfy`.

The `selfy` command is an easy way to create private keys, certificates,
and self-signed certficate authories. It is vastly more usable than the
mountain of complexity that is `openssl`. It is more limited in scope.
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
`selfy --nopass` flag. A long salt and the Argon2id key-derivation-function are used to
provide time and memory-hard protection against ASIC brute-force cracking 
attempts (see https://en.wikipedia.org/wiki/Argon2  https://datatracker.ietf.org/doc/html/rfc9106 ).

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
    
  -gensym string
        generate a new 32-byte symmetric encryption 
        key with crypto/rand, and save it under 
        this filename in the -p directory.
    
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

  -verify string
        verify this path is a certificate signed by the private key 
        corresponding to the -p {my-keep-private-dir}/ca.crt public key
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
