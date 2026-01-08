package tube

import (
	"fmt"
	"os"

	"github.com/glycerine/ipaddr"
)

// called by cmd/member, generalization of tup/tup.go setup
func LoadFromDiskTubeConfig(configName string, quiet, useSimNet, isTest bool) (cfg *TubeConfig, err error) {

	// first connect, then run repl
	dir := GetConfigDir()
	pathCfg := dir + "/" + configName + ".default.config"
	if fileExists(pathCfg) {
		vv("using config file: '%v'", pathCfg)
	} else {
		var fd *os.File
		fd, err = os.Create(pathCfg)
		panicOn(err)
		cfg = &TubeConfig{
			// distinguish multiple configName clients
			MyName:          configName + "_" + CryRand15B(),
			PeerServiceName: TUBE_CLIENT,
		}
		fmt.Fprintf(fd, "%v\n", cfg.SexpString(nil))
		fd.Close()
		err = fmt.Errorf("error: no config file. Created one from template in '%v'. Please complete it.\n", pathCfg)
		return
	}
	by, err := os.ReadFile(pathCfg)
	panicOn(err)
	//pp("got by = '%v'", string(by))

	cfg, err = NewTubeConfigFromSexpString(string(by), nil)
	if err != nil {
		return
	}

	// distinguish multiple tup clients
	cfg.MyName = configName + "_" + CryRand15B()
	cfg.PeerServiceName = TUBE_CLIENT

	myHost := ipaddr.GetExternalIP()
	myPort := ipaddr.GetAvailPort()
	cfg.RpcCfg.ServerAddr = fmt.Sprintf("%v:%v", myHost, myPort)

	// set up our config
	cfg.Init(quiet, isTest)

	cfg.UseSimNet = useSimNet

	cfg.RpcCfg.TCPonly_no_TLS = cfg.TCPonly_no_TLS
	cfg.RpcCfg.ServerAutoCreateClientsToDialOtherServers = true
	cfg.RpcCfg.QuietTestMode = quiet

	cfg.ClientProdConfigSaneOrPanic()

	cfg.ConvertToExternalAddr()

	//vv("cfg = '%v'", cfg.ShortSexpString(nil))

	return
}
