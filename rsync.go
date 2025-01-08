package rpc25519

//go:generate greenpack

type RsyncHashes struct {
	Path              string       `zid:"0"`
	Blake3FullFileSum string       `zid:"1"`
	ChunkerName       string       `zid:"2"`
	Chunk             []RsyncChunk `zid:"3"`
}

type RsyncChunk struct {
	Beg        int    `zid:"0"`
	Endx       int    `zid:"1"`
	Blake3Hash string `zid:"2"`
}
