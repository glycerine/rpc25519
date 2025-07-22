package block

type ServerOpaqueBlob struct {
	VolumeID string

	// Beg must be >= MinOffset.
	// Clients can break ties at MinOffset by
	// inserting random space between MinOffset
	// and SuggestedBeg
	MinOffset  int64
	TieBreaker int64

	Beg  int64
	Endx int64 // Beg + len(ServerData)

	ServerData []byte

	// chucksum ServerData
	ServerDataBlake3 string
}

// client side: can decrypt ServerData to:
type ClientBatch struct {
	PrePad      []byte
	DataPayload []byte
	PostPad     []byte

	// how DataPayload is compressed; empty string if not in use.
	Compression   string
	PayloadBlake3 string

	// generated before; will end up 1:1 with blake3 of this encrypted data.
	WriteCallID string
	WriteTime0  time.Time
	Expires     time.Time // allow backups to be purged too.

	VolumeID string

	// MinOffset gives the version we are after.
	MinOffset int64

	// chucksum ServerData must be zero when
	// encrypting Client
	ServerDataBlake3 string
}
