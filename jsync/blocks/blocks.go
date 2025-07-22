package block

type ServerBlock struct {
	// where is encrypted data stored.
	VolumeID string `zid:"0"`
	Beg      int64  `zid:"1"` // where in volume does ServerData start.
	Endx     int64  `zid:"2"` // Beg + len(ServerData)

	// encrypted data (nil unless sending over network).
	ServerData []byte `zid:"3"`

	// checksum ServerData
	ServerDataBlake3 string `zid:"4"`
}

// client knows ClientBlock, and then
// leaves ServerDataBlake3 as default empty string
// when encrypting ClientBlock into ServerBlock.ServerData.
type ClientBlock struct {
	PayloadBlake3 string `zid:"0"`

	// how DataPayload is compressed; empty string if none.
	Compression string `zid:"1"`

	// generated before; will end up 1:1 with blake3 of this encrypted data.
	WriteCallID string    `zid:"2"`
	WriteTime0  time.Time `zid:"3"`
	Expires     time.Time `zid:"4"` // allow backups to be purged too.

	// chucksum ServerData must be empty string when
	// encrypting ClientBlock into ServerData.
	// Then should be filled in so client knows
	// how to request it back.
	ServerDataBlake3 string `zid:"5"`

	PrePad  []byte `zid:"6"` // cry-rand padding
	Payload []byte `zid:"7"`
	PostPad []byte `zid:"8"` // cry-rand padding
}
