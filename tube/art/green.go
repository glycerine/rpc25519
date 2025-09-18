package art

import (
	"github.com/glycerine/greenpack/msgp"
)

type Green interface {
	//msgp.Encodable // maybe comment out later.
	msgp.Encodable
	msgp.Decodable
	msgp.Marshaler
	msgp.Unmarshaler
}

/*type GreenWrap struct {
	mem  any
	Disk Green `zid:"0"`
}

func (g *GreenWrap) PreSaveHook() {
	switch x := g.mem.(type) {
	case Green:
		//g.Disk = g.mem.(Green)
		g.Disk = x
	}
}

func (g *GreenWrap) PostLoadHook() {
	g.mem = g.Disk
}
*/
