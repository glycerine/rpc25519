package block

type ServerBlock struct {
	VolumeID string

	Beg  int64 // where in volume does ServerData start.
	Endx int64 // Beg + len(ServerData)

	ServerData []byte

	// chucksum ServerData
	ServerDataBlake3 string
}

// client knows ClientBlock, and then
// leaves ServerDataBlake3 as default empty string
// when encrypting ClientBlock into ServerBlock.ServerData.
type ClientBlock struct {
	PrePad  []byte
	Payload []byte
	PostPad []byte

	// how DataPayload is compressed; empty string if none.
	Compression   string
	PayloadBlake3 string

	// generated before; will end up 1:1 with blake3 of this encrypted data.
	WriteCallID string
	WriteTime0  time.Time
	Expires     time.Time // allow backups to be purged too.

	VolumeID string

	// chucksum ServerData must be empty string when
	// encrypting ClientBlock into ServerData.
	ServerDataBlake3 string
}
