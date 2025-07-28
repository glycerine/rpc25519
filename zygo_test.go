package rpc25519

import (
	"fmt"
	"testing"

	"github.com/glycerine/zygomys/v9/zygo"
)

var _ = fmt.Printf

func Test060_load_config(t *testing.T) {

	// verify that we can serialize a Config
	// to a zygo S-expression and back;
	// so we can store the config on disk in
	// a file and load it in; or write it to disk
	// if need be.

	env := newConfigReadingZygoEnv()
	defer env.Close()
	str := "(rpc25519_Config )"

	cfg, err := NewRpcConfigFromSexpString(str, nil)
	panicOn(err)
	//vv("cfg = '%v'", cfg)

	// verify can turn the Go struct &TubeConfig{}
	// back into a string, and parse the string form back;
	// for a complete round trip and a half.
	str2 := cfg.SexpString(nil)
	//vv("str2 = '%v'", str2)
	mycfg2, err := env.EvalString(str2)
	panicOn(err)

	_, err = zygo.ToGoFunction(env, "togo", []zygo.Sexp{mycfg2})
	panicOn(err)
	cfg2 := mycfg2.(*zygo.SexpHash).GoShadowStruct.(*Config)
	_ = cfg2
	//vv("cfg2 = '%v'", cfg2)

	cfg3 := NewConfig()
	//vv("cfg3 = '%v'", cfg3)

	cfg3str := cfg3.SexpString(nil)
	mycfg3, err := env.EvalString(cfg3str)
	panicOn(err)

	_, err = zygo.ToGoFunction(env, "togo", []zygo.Sexp{mycfg3})
	panicOn(err)
	cfg4 := mycfg3.(*zygo.SexpHash).GoShadowStruct.(*Config)
	//vv("cfg4 = '%v'", cfg4.SexpString(nil))
	if cfg4.SexpString(nil) != cfg3str {
		panic("cfg4.SexpString(nil) differed from cfg3str")
	}

	// verify all the fields get restored

	all := `(rpc25519_Config
       QuietTestMode: true,
          ServerAddr: "server:5819",
      ClientHostPort: "client:9422",
ClientDialToHostPort: "server_name2:5819",
      TCPonly_no_TLS: true,
             UseQUIC: true,
     NoSharePortQUIC: true,
            CertPath: "some/cert/path",
      SkipVerifyKeys: true,
   ClientKeyPairName: "client_key_pair_name",
   ServerKeyPairName: "server_key_pair_name",
    PreSharedKeyPath: "per_shared_key_path",
      ConnectTimeout: (dur "9s"),
 ServerSendKeepAlive: (dur "7s"),
 ClientSendKeepAlive: (dur "3s"),
        CompressAlgo: "compressAlgo",
      CompressionOff: true,
 HTTPConnectRequired: true,
ServerAutoCreateClientsToDialOtherServers: true,
           UseSimNet: true,
)`

	cfg, err = NewRpcConfigFromSexpString(all, nil)
	panicOn(err)
	all2 := cfg.SexpString(nil)
	//vv("all2= '%v'", all2)
	if all2 != all {
		panic(fmt.Sprintf("wanted all='%v'\n got all2 = '%v'", all, all2))
	}
}
