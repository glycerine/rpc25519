package rpc25519

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	//"sync/atomic"
	"time"
)

const (
	UserMaxPayload = 1_200_000 // users should chunk to this size, to be safe.

	maxMessage = 1_310_720 - 80 // ~ 1 MB max message size, prevents TLS clients from talking to TCP servers, as the random TLS data looks like very big message size. Also lets us test on smaller virtual machines without out-of-memory issues.
)

var ErrTooLong = fmt.Errorf("message message too long: over 64MB; encrypted client vs an un-encrypted server?")

var _ = io.EOF

var DebugVerboseCompress bool //= true

// uConn hopefully works for both quic.Stream and net.Conn, universally.
type uConn interface {
	io.Writer
	SetWriteDeadline(t time.Time) error

	io.Reader
	SetReadDeadline(t time.Time) error
}

// =========================
//
// message structure
//
// 0. magic: 8 bytes always the first thing. Simple, but
//           statistically has a high probability of detecting
//           partial message cutoff and resumption.
//           The magic[7] says what compression is used, if any.
//
// 1. lenBody: next  8 bytes: *body_length*, big endian uint64.
//                The *body_length* says how many bytes are in the body.
//
// 2. body: next length bytes: *body*. The *body* is *body_length* bytes long.
//          These are greenpack encoded (msgpack) bytes.
// =========================

// a work (workspace) lets us re-use memory
// without constantly allocating.
// There should be one for reading, and
// a separate one for writing, so each
// goroutine needs its own so as to not
// colide with any other goroutine.
type workspace struct {
	maxMsgSize int
	buf        []byte

	readLenMessageBytes  []byte
	writeLenMessageBytes []byte
	magicCheck           []byte

	name string

	compress bool
	// if so, which algo to use?
	// choices: "s2", "lz4", "zstd" available atm.
	defaultCompressionAlgo string
	defaultMagic7          magic7b

	decomp  *decomp
	pressor *pressor

	isServer bool
	cfg      *Config
	spair    *rwPair
	cpair    *cliPairState

	// one writer at a time.
	wmut sync.Mutex

	// Enforce one readMessage() on conn at a time,
	// at least within our workspace.
	// We do this because only
	// the single Read() operation
	// is considered atomic, but
	// a readFull() may do mulitple
	// Read() operations before
	// finishing, and we don't
	// want two goroutines interleaving
	// these. readMessage() certainly
	// does multiple Read()s as well.
	//
	// Update: technically we do not need this because
	// our use readMessage() is isolated to the
	// readLoop() function. Both the client and
	// server use only a single goroutine to read
	// a given uConn, so we are safe.
	//rmut sync.Mutex
}

// currently only used for headers; but bodies may
// well benefit as well. In which case, bump up
// to maxMessage+1024 or so, rather than this 64KB.
//
// Update: no longer true!
// newWorkspace is used by the chacha.go encoder.sendMessage
// for example, as it uses buf for encoding and encrypting
// the full messages. Hence maxMsgSize *must* be
// at least maxMessage + magic(8 bytes) + msglen(8 bytes) +
// noncesize(24 bytes at most) + authTag(16 bytes) overhead
// or larger now. XChaCha has a 24 byte nonce.
// Poly1305 has authTag overhead of 16 bytes.
// Hence at least 56 more bytes beyond maxMessage
// are needed. We currently allocate 80 more bytes.
// This pre-allocation avoids all reallocation
// and memory churn during regular operation.
func newWorkspace(name string, maxMsgSize int, isServer bool, cfg *Config, spair *rwPair, cpair *cliPairState) *workspace {

	w := &workspace{
		name:       name,
		maxMsgSize: maxMsgSize,
		// need at least len(msg) + 56; 56 because ==
		// magic(8) + msglen(8) + nonceX(24) + overhead(16)
		buf:                  make([]byte, maxMsgSize+80),
		readLenMessageBytes:  make([]byte, 8),
		writeLenMessageBytes: make([]byte, 8),
		magicCheck:           make([]byte, 8), // last byte is compression type.

		compress:               !cfg.CompressionOff,
		pressor:                newPressor(maxMsgSize + 80),
		decomp:                 newDecomp(maxMsgSize + 80),
		defaultCompressionAlgo: cfg.CompressAlgo,
		isServer:               isServer,
		cfg:                    cfg,
		spair:                  spair,
		cpair:                  cpair,
	}
	// write according to our defaults.
	w.defaultMagic7 = setMagicCheckWord(cfg.CompressAlgo, w.magicCheck)
	//vv("newWorkspace sets cfg.lastReadMagic7 = w.defaultMagic7 = %v from '%v'", w.defaultMagic7, cfg.CompressAlgo)
	if isServer {
		spair.lastReadMagic7.Store(int64(w.defaultMagic7))
	} else {
		cpair.lastReadMagic7.Store(int64(w.defaultMagic7))
	}
	return w
}

// receiveMessage reads a framed message from conn
// nil or 0 timeout means no timeout.
func (w *workspace) readMessage(conn uConn, timeout *time.Duration) (msg *Message, err error) {

	// our use of w.readMessage is isolated to the readLoop
	// on the server and in the client. They are the only
	// users within their processes. Hence this locking is
	// not needed. If that changes/if ever there are multiple goroutines
	// calling us, we would want to turn on read locking.
	//
	// Moreover we are wrapped in a blabber for encryption,
	// even if the encryption is not turned on. That means
	// that there is already a separate workspace for
	// the readers and writers, and it is safe to read
	// and send in parallel via a blabber (though not
	// of course from a workspace directly; which is
	// why the blabber keeps separate workspaces for
	// encrypting and decrypting). The upshot is simply
	// that we do not need locks here. We don't want
	// them for performance either.
	//w.wmut.Lock()
	//defer w.rwut.Unlock()

	// Read the first 8 bytes for the magic/compression type check.
	_, err = readFull(conn, w.magicCheck, timeout)
	if err != nil {
		return nil, err
	}

	if DebugVerboseCompress {
		alwaysPrintf("readMessage sees magic7 = %v", magic7b(w.magicCheck[7]))
	}

	// allow the magic[7] to indicate compression type.
	if !bytes.Equal(w.magicCheck[:7], magic[:7]) {
		return nil, ErrMagicWrong
	}
	magic7 := magic7b(w.magicCheck[7])

	// we don't want to loose all compression
	// just because a single message requested
	// none.
	if magic7 != magic7b_no_system_compression {
		if w.isServer {
			//vv("common readMessage magic7 = %v -> storing to w.spair.lastReadMagic7", magic7)
			w.spair.lastReadMagic7.Store(int64(magic7))
		} else {
			//vv("client: common readMessage magic7 = %v was seen", magic7)
			w.cpair.lastReadMagic7.Store(int64(magic7))
		}
	}

	// Read the next 8 bytes for the Message length.
	_, err = readFull(conn, w.readLenMessageBytes, timeout)
	if err != nil {
		return nil, err
	}
	messageLen := binary.BigEndian.Uint64(w.readLenMessageBytes)

	// Read the message based on the messageLen
	if messageLen > maxMessage {
		// probably an encrypted client against an unencrypted server
		return nil, ErrTooLong
	}

	// why not use the w.buf here? are we afraid of sharing memory/
	// having messages over-write early messages?
	message := make([]byte, messageLen)
	_, err = readFull(conn, message, timeout)
	if err != nil {
		return nil, err
	}

	// reverse any compression
	//
	// If you see an error like
	// `msgp: attempted to decode type "int" with method for "map"`
	// It probably means we are not getting the decompression done.
	//
	// Note that we must ignore the w.compress setting here.
	// While *we* may not be using compression to send, we
	// may well receive messages that are compressed.
	message, err = w.decomp.handleDecompress(magic7, message)
	if err != nil {
		return nil, err
	}

	msg, err = MessageFromGreenpack(message)
	if err != nil {
		alwaysPrintf("error decoding greenpack messages: '%v'", err)
		return nil, err
	}
	return msg, nil
}

// sendMessage sends a framed message to conn
// nil or 0 timeout means no timeout.
func (w *workspace) sendMessage(conn uConn, msg *Message, timeout *time.Duration) error {
	w.wmut.Lock()
	defer w.wmut.Unlock()

	// serialize message to bytes
	bytesMsg, err := msg.AsGreenpack(w.buf)
	if err != nil {
		return err
	}

	if msg.HDR.NoSystemCompression {
		// user requested no compression on this Message.
		w.magicCheck[7] = byte(magic7b_no_system_compression)

	} else if w.compress {
		var magic7 magic7b
		if w.isServer {
			// server tries to match what we last got from the client.
			magic7 = magic7b(w.spair.lastReadMagic7.Load())
			if magic7 < 0 || magic7 >= magic7b_out_of_bounds {
				magic7 = w.defaultMagic7
			} else {
				//vv("server matches client, magic7=%v", magic7)
			}
		} else {
			// client does as user requested.
			magic7 = w.defaultMagic7
			//vv("client doing as set, magic7=%v", magic7)
		}
		//vv("common.go sendMessage calling handleCompress: w.defaultMagic7 = %v", w.defaultMagic7)
		w.magicCheck[7] = byte(magic7)
		bytesMsg, err = w.pressor.handleCompress(magic7, bytesMsg)
		if err != nil {
			return err
		}
	} else {
		w.magicCheck[7] = byte(magic7b_none)
	}

	if DebugVerboseCompress {
		alwaysPrintf("sendMessage using magic7 = %v", magic7b(w.magicCheck[7]))
	}

	nbytesMsg := len(bytesMsg)

	binary.BigEndian.PutUint64(w.writeLenMessageBytes, uint64(nbytesMsg))

	// Write magic
	if err := writeFull(conn, w.magicCheck[:], timeout); err != nil {
		return err
	}

	// Write Message length
	if err := writeFull(conn, w.writeLenMessageBytes, timeout); err != nil {
		return err
	}

	// Write Message
	return writeFull(conn, bytesMsg, timeout)
}

var zeroTime = time.Time{}

// readFull reads exactly len(buf) bytes from conn.
// The returned numRead can be less than
// len(buf) and and then err will be non-nil (on timeout for example).
//
// numRead must be returned, because it may be important
// to account for the partial read
// when numRead < len(buf), so as to discard a partially
// read message and get to the start of the next message.
func readFull(conn uConn, buf []byte, timeout *time.Duration) (numRead int, err error) {

	if timeout != nil && *timeout > 0 {
		conn.SetReadDeadline(time.Now().Add(*timeout))
	} else {
		// do not let previous deadlines contaminate this one.
		conn.SetReadDeadline(zeroTime)
	}

	need := len(buf)

	for numRead < len(buf) {
		n, err := conn.Read(buf[numRead:])
		numRead += n
		if numRead == need {
			// probably just EOF
			return numRead, nil
		}
		if err != nil {
			// can be a timeout, with partial read.
			return numRead, err
		}
	}

	return numRead, nil
}

// writeFull writes all bytes in buf to conn
func writeFull(conn uConn, buf []byte, timeout *time.Duration) error {

	if timeout != nil && *timeout > 0 {
		line := time.Now().Add(*timeout)
		//vv("writeFull setting deadline to '%v'", line)
		conn.SetWriteDeadline(line)
	} else {
		//vv("writeFull has no deadline")
		// do not let previous deadlines contaminate this one.
		conn.SetWriteDeadline(zeroTime)
	}

	need := len(buf)
	total := 0
	for total < len(buf) {
		n, err := conn.Write(buf[total:])
		total += n
		if total == need {
			//panicOn(err) // probably just EOF
			//vv("writeFull returning nil after seeing total == need = %v", total)
			return nil

		}
		if err != nil {
			//vv("writeFull returning err '%v'", err)
			return err
		}
	}
	return nil
}
