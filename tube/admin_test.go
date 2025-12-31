package tube

import (
	"fmt"
	//"strings"
	//"reflect"
	"testing"

	"github.com/glycerine/zygomys/v9/zygo"
)

var _ = fmt.Printf

func Test060_load_config(t *testing.T) {

	// verify that we can serialize a TubeConfig
	// to a zygo S-expression and back;
	// so we can store the config on disk in
	// a file and load it in; or write it to disk
	// if need be.

	env := newTubeConfigReadingZygoEnv()
	defer env.Close()
	str := "(tubeConfig )"

	cfg, err := NewTubeConfigFromSexpString(str, nil)
	panicOn(err)
	//vv("cfg = '%v'", cfg)

	// verify can turn the Go struct &TubeConfig{}
	// back into a string, and parse the string form back;
	// for a complete round trip and a half.
	str2 := cfg.String()
	mycfg2, err := env.EvalString(str2)
	panicOn(err)

	_, err = zygo.ToGoFunction(env, "togo", []zygo.Sexp{mycfg2})
	panicOn(err)
	cfg2 := mycfg2.(*zygo.SexpHash).GoShadowStruct.(*TubeConfig)
	_ = cfg2
	//vv("cfg2 = '%v'", cfg2)

	clusterSize := 3
	clusterID := t.Name()
	cfg3 := NewTubeConfigTest(clusterSize, clusterID, faketime)
	//vv("cfg3 = '%v'", cfg3)

	cfg3str := cfg3.String()
	//vv("cfg3str = '%v'", cfg3str)
	mycfg3, err := env.EvalString(cfg3str)
	panicOn(err)

	_, err = zygo.ToGoFunction(env, "togo", []zygo.Sexp{mycfg3})
	panicOn(err)
	cfg4 := mycfg3.(*zygo.SexpHash).GoShadowStruct.(*TubeConfig)
	//vv("cfg4 = '%v'", cfg4)
	if cfg4.String() != cfg3str {
		panic("cfg4.String() differed from cfg3str")
	}

	// verify all the fields get restored

	// (there is no longer a separate SimnetConfig).
	all := `(tubeConfig
         ConfigName: "",
          ClusterID: "Test060_load_config",
          ConfigDir: "/my/dir",
            DataDir: "/data/dir",
      NoFaultTolDur: (dur "4m0s"),
             NoDisk: true,
    NoLogCompaction: true,
     TCPonly_no_TLS: true,
              ZapMC: true,
       HeartbeatDur: (dur "995ms"),
     MinElectionDur: (dur "11s"),
        ClusterSize: 7,
          UseSimNet: true,
   SimnetGOMAXPROCS: 1,
    ClockDriftBound: (dur "120ms"),
  InitialLeaderName: "doug",
             MyName: "doug",
    PeerServiceName: "",
NoBackgroundConnect: true,
          Node2Addr: (hash "barney":":7001", "doug":":7004", "frank":":7002", "wilma":":7003"),
             RpcCfg: (rpc25519_Config
    SimnetGOMAXPROCS: 0,
 LimitedServiceNames: ["singleton"],
   LimitedServiceMax: [1],
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
 HTTPConnectRequired: false,
ServerAutoCreateClientsToDialOtherServers: true,
           UseSimNet: true,
))`
	cfg, err = NewTubeConfigFromSexpString(all, nil)
	panicOn(err)
	all2 := cfg.String()
	//vv("all2= '%v'", all2)
	if !equalIgnoringSpaces(all2, all) {
		//if all2 != all {
		panic(fmt.Sprintf("wanted all='%v'\n got all2 = '%v'", all, all2))
	}

	numNodes := 3
	defaultCfg := NewTubeConfigTest(numNodes, t.Name(), faketime)
	_ = defaultCfg
	//vv("defaultCfg = \n%v", defaultCfg.SexpString(nil))
}

/*
defaultCfg =
(tubeConfig
          ClusterID: "Test060_load_config",
          ConfigDir: "",
            DataDir: "",
      NoFaultTolDur: (dur "0s"),
             NoDisk: true,
     TCPonly_no_TLS: true,
       HeartbeatDur: (dur "200ms"),
     MinElectionDur: (dur "1s"),
        ClusterSize: 3,
          UseSimNet: false,
   SimnetGOMAXPROCS: 0,
    ClockDriftBound: (dur "20ms"),
  InitialLeaderName: "",
             MyName: "",
          Node2Addr: (hash),
NoBackgroundConnect: false,
             RpcCfg: (rpc25519_Config
       QuietTestMode: true,
          ServerAddr: "127.0.0.1:0",
      ClientHostPort: "",
ClientDialToHostPort: "",
      TCPonly_no_TLS: true,
             UseQUIC: false,
     NoSharePortQUIC: false,
            CertPath: "",
      SkipVerifyKeys: false,
   ClientKeyPairName: "",
   ServerKeyPairName: "",
    PreSharedKeyPath: "",
      ConnectTimeout: (dur "0s"),
 ServerSendKeepAlive: (dur "0s"),
 ClientSendKeepAlive: (dur "0s"),
        CompressAlgo: "",
      CompressionOff: false,
 HTTPConnectRequired: false,
ServerAutoCreateClientsToDialOtherServers: true,
))
*/
