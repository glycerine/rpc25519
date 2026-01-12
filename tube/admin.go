package tube

// Admin API
import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	//"math/rand/v2"
	cryrand "crypto/rand"
	"time"

	rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/zygomys/v9/zygo"
)

func newTubeConfigReadingZygoEnv() (env *zygo.Zlisp) {
	env = zygo.NewZlisp()
	// minimal setup for config file handling.
	env.ImportBaseTypes()
	env.ImportEval()
	env.ImportTime()
	env.ImportMinimalBuilder() // :, comma, raw64

	env.AddFunction("tubeConfig", tubeConfigZygoFunction)
	rpc.RegisterConfigZygoConstructor(env)
	return
}

func tubeConfigZygoFunction(env *zygo.Zlisp, name string, args []zygo.Sexp) (zygo.Sexp, error) {

	// many parameters, treat as key:value pairs in the hash/record.
	return zygo.ConstructorFunction("msgmap")(env, "msgmap", append([]zygo.Sexp{&zygo.SexpStr{S: name}}, zygo.MakeList(args)))

}

// register our struct(s) in advance, so
// that zlisp knows about them.
func init() {
	gsr := &zygo.GoStructRegistry

	pf := &zygo.RegisteredType{GenDefMap: true, Factory: func(env *zygo.Zlisp, h *zygo.SexpHash) (interface{}, error) {
		return &TubeConfig{}, nil
	}}
	gsr.RegisterUserdef(pf, true, "tubeConfig")
}

func (cfg *TubeConfig) ConvertToExternalAddr() {
	for name, addr := range cfg.Node2Addr {
		external := GetExternalAddr(cfg.RpcCfg.UseQUIC, addr)
		cfg.Node2Addr[name] = external
	}
}

func NewTubeConfigFromSexpString(sexp string, env *zygo.Zlisp) (cfg *TubeConfig, err error) {

	if env == nil {
		env = newTubeConfigReadingZygoEnv()
		defer env.Close()
	}

	mycfg, err := env.EvalString(sexp)
	panicOn(err)
	//mystr := mycfg.SexpString(nil)

	_, err = zygo.ToGoFunction(env, "togo", []zygo.Sexp{mycfg})
	panicOn(err)
	cfg = mycfg.(*zygo.SexpHash).GoShadowStruct.(*TubeConfig)
	if cfg.RpcCfg == nil {
		cfg.RpcCfg = rpc.NewConfig()
	}
	err = cfg.CheckForProblems()
	return
}

func (s *TubeConfig) String() string {
	return s.SexpString(nil)
}

func (s *TubeConfig) SexpString(ps *zygo.PrintState) (r string) {
	nodes := "(hash "
	i := 0
	omap := NewOmap[string, string]()
	for node, addr := range s.Node2Addr {
		omap.Set(node, addr)
	}
	for node, addr := range omap.All() {
		if i > 0 {
			nodes += ", "
		}
		nodes += fmt.Sprintf("%q:%q", node, addr)
		i++
	}
	nodes += ")"
	var rpcConfigExtra string
	if s.RpcCfg != nil {
		rpcConfigExtra = fmt.Sprintf("             RpcCfg: %v", s.RpcCfg.SexpString(ps))
	}

	return fmt.Sprintf(`(tubeConfig
         ConfigName: %q,
          ClusterID: %q,
          ConfigDir: %q,
            DataDir: %q,
      NoFaultTolDur: (dur %q),
             NoDisk: %v,
    NoLogCompaction: %v,
     TCPonly_no_TLS: %v,
              ZapMC: %v,
       HeartbeatDur: (dur %q),
     MinElectionDur: (dur %q),
        ClusterSize: %v,
          UseSimNet: %v,
   SimnetGOMAXPROCS: %v,
    ClockDriftBound: (dur %q),
  InitialLeaderName: %q,
             MyName: %q,
    PeerServiceName: %q,
NoBackgroundConnect: %v,
   BatchAccumateDur: (dur %q),
          Node2Addr: %v,
%v)`, s.ConfigName,
		s.ClusterID,
		s.ConfigDir,
		s.DataDir,
		s.NoFaultTolDur,
		s.NoDisk,
		s.NoLogCompaction,
		s.TCPonly_no_TLS,
		s.ZapMC,
		s.HeartbeatDur,
		s.MinElectionDur,
		s.ClusterSize,
		s.UseSimNet,
		s.SimnetGOMAXPROCS,
		s.ClockDriftBound,
		s.InitialLeaderName,
		s.MyName,
		s.PeerServiceName,
		s.NoBackgroundConnect,
		s.BatchAccumateDur,
		nodes,
		rpcConfigExtra,
	)
}

// ShortSexpString omits any zero-value fields
func (s *TubeConfig) ShortSexpString(ps *zygo.PrintState) (r string) {
	var mainParts []string

	// String fields
	if s.ConfigName != "" {
		mainParts = append(mainParts, fmt.Sprintf("         ConfigName: %q", s.ConfigName))
	}
	if s.ClusterID != "" {
		mainParts = append(mainParts, fmt.Sprintf("          ClusterID: %q", s.ClusterID))
	}
	if s.ConfigDir != "" {
		mainParts = append(mainParts, fmt.Sprintf("          ConfigDir: %q", s.ConfigDir))
	}
	if s.DataDir != "" {
		mainParts = append(mainParts, fmt.Sprintf("            DataDir: %q", s.DataDir))
	}
	if s.NoFaultTolDur != 0 {
		mainParts = append(mainParts, fmt.Sprintf("      NoFaultTolDur: (dur %q)", s.NoFaultTolDur))
	}
	if s.NoDisk {
		mainParts = append(mainParts, fmt.Sprintf("             NoDisk: %v", s.NoDisk))
	}
	if s.NoLogCompaction {
		mainParts = append(mainParts, fmt.Sprintf("    NoLogCompaction: %v", s.NoLogCompaction))
	}

	if s.TCPonly_no_TLS {
		mainParts = append(mainParts, fmt.Sprintf("     TCPonly_no_TLS: %v", s.TCPonly_no_TLS))
	}
	if s.ZapMC {
		mainParts = append(mainParts, fmt.Sprintf("              ZapMC: %v", s.ZapMC))
	}
	if s.HeartbeatDur != 0 {
		mainParts = append(mainParts, fmt.Sprintf("       HeartbeatDur: (dur %q)", s.HeartbeatDur))
	}
	if s.MinElectionDur != 0 {
		mainParts = append(mainParts, fmt.Sprintf("     MinElectionDur: (dur %q)", s.MinElectionDur))
	}
	if s.ClusterSize != 0 {
		mainParts = append(mainParts, fmt.Sprintf("        ClusterSize: %v", s.ClusterSize))
	}
	if s.UseSimNet {
		mainParts = append(mainParts, fmt.Sprintf("          UseSimNet: %v", s.UseSimNet))
		// only bother if UseSimNet true
		if s.SimnetGOMAXPROCS > 0 {
			mainParts = append(mainParts, fmt.Sprintf("   SimnetGOMAXPROCS: %v", s.SimnetGOMAXPROCS))
		}
	}
	if s.ClockDriftBound != 0 {
		mainParts = append(mainParts, fmt.Sprintf("    ClockDriftBound: (dur %q)", s.ClockDriftBound))
	}

	if s.InitialLeaderName != "" {
		mainParts = append(mainParts, fmt.Sprintf("  InitialLeaderName: %q", s.InitialLeaderName))
	}
	if s.MyName != "" {
		mainParts = append(mainParts, fmt.Sprintf("             MyName: %q", s.MyName))
	}
	if s.PeerServiceName != "" {
		mainParts = append(mainParts, fmt.Sprintf("    PeerServiceName: %q", s.PeerServiceName))
	}

	if s.NoBackgroundConnect {
		mainParts = append(mainParts, fmt.Sprintf("NoBackgroundConnect: %v", s.NoBackgroundConnect))
	}
	if s.BatchAccumateDur != 0 {
		mainParts = append(mainParts, fmt.Sprintf("   BatchAccumateDur: (dur %q)", s.BatchAccumateDur))
	}

	nodes := "(hash "
	i := 0
	omap := NewOmap[string, string]()
	for node, addr := range s.Node2Addr {
		omap.Set(node, addr)
	}
	for node, addr := range omap.All() {
		//if i > 0 {
		//nodes += ", "
		nodes += "\n"
		//}
		nodes += fmt.Sprintf("%q:%q", node, addr)
		i++
	}
	nodes += ")"
	mainParts = append(mainParts, fmt.Sprintf("          Node2Addr: %v", nodes))

	// Pointer field (RpcCfg)
	var rpcPart string
	if s.RpcCfg != nil {
		rpcPart = fmt.Sprintf("             RpcCfg: %v", s.RpcCfg.ShortSexpString(ps))
	}

	// Assemble the final string
	if len(mainParts) == 0 && rpcPart == "" {
		return "(tubeConfig)"
	}

	body := strings.Join(mainParts, ",\n")

	if body != "" && rpcPart != "" {
		// Add comma between main parts and the rpcPart
		body += ",\n" + rpcPart
	} else if rpcPart != "" {
		body = rpcPart
	}

	return fmt.Sprintf("(tubeConfig\n%s)", body)
}

// Type returns the type of the value.
func (s *TubeConfig) Type() *zygo.RegisteredType {
	return zygo.GoStructRegistry.Lookup("tubeConfig")
}

// CheckForProblems is called by
// NewTubeConfigFromSexpString() right after
// the s *TubeConfig is loaded from s-expression
// in order to detect any manual mis-configuration
// like whitespace in string fields that
// will create problems for disk paths.
//
// At the moment, the rules are:
// 1) No whitespace in string fields.
// 2) time.Duration >= 0 for all dur fields.
// 3) InitialLeaderName is also in Node2Addr, if set.
func (s *TubeConfig) CheckForProblems() error {

	if s.ClusterSize < 0 {
		return fmt.Errorf("cfg.ClusterSize cannot be negative: %v", s.ClusterSize)
	}

	durationType := reflect.TypeOf(time.Duration(0))

	v := reflect.ValueOf(s).Elem()
	for i := 0; i < v.NumField(); i++ {
		fieldValue := v.Field(i)
		fieldType := v.Type().Field(i)
		switch fieldValue.Kind() {
		case reflect.String:
			// Get the string value from the reflection value.
			stringValue := fieldValue.String()
			if hasWhiteSpace(stringValue) {
				return fmt.Errorf("error in TubeConfig: field cannot contain whitespace (field '%v') is: '%v'", fieldType.Name, stringValue)
			}
		case reflect.Int64:
			if fieldValue.Type() == durationType {
				dur := fieldValue.Int()
				if dur < 0 {
					return fmt.Errorf("error in TubeConfig: time.Duration field cannot be negative (field '%v') is: '%v'", fieldType.Name, dur)
				}
			}
		}
	}

	if s.InitialLeaderName != "" {
		found := false
		for name, _ := range s.Node2Addr {
			if name == s.InitialLeaderName {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("error in TubeConfig: InitialLeaderName('%v') is not also found as a node name key in Node2Addr: '%#v'", s.InitialLeaderName, s.Node2Addr)
		}
	}
	return nil
}

//type Key string // for now... []byte later.
//type Val string // for now... []byte

var sep = string(os.PathSeparator)

var (
	ErrInvalidNodeID     = fmt.Errorf("invalid node ID")
	ErrNodeAlreadyExists = fmt.Errorf("node already exists in membership")
	ErrNodeNotFound      = fmt.Errorf("node not found in membership")
	ErrKeyNotFound       = fmt.Errorf("key not found")
	ErrCannotRemoveSelf  = fmt.Errorf("cannot remove self from membership")
	ErrCannotJoinSelf    = fmt.Errorf("cannot join through self")
	ErrChannelFull       = fmt.Errorf("message channel is full")
	ErrConflict          = fmt.Errorf("error conflict: too low version")
	ErrTimeOut           = fmt.Errorf("error timeout")
)

// GetConfigDir returns the config dir
// where we look for our .host.cid file
// to uniquely identify the host, so
// that we don't accidentally overwrite
// a file with itself.
func GetConfigDir() (path string) {
	dir := os.Getenv("XDG_CONFIG_HOME")
	home := os.Getenv("HOME")
	suffix := sep + ".config" + sep + "tube"
	switch {
	case dir != "":
		path = dir + suffix
	case home != "":
		path = home + suffix
	default:
		return "." // use cwd
	}
	err := os.MkdirAll(path, 0700)
	panicOn(err)
	return
}

// GetServerDataDir tells the Server where to store its
// log data files. We prefer the ZDB_PAXOS_DATA_DIR
// environment variable, if set, for this directory.
//
// Quoting from the XDG Base Directory Specification:
//
// "There is a single base directory relative to
// which user-specific data files should be written.
// This directory is defined by the environment
// variable $XDG_DATA_HOME"
//
// "$XDG_DATA_HOME defines the base directory relative to which
// user-specific data files should be stored. If $XDG_DATA_HOME
// is either not set or empty, a default equal
// to $HOME/.local/share should be used."
// -- https://specifications.freedesktop.org/basedir-spec/latest/
// (as of 2025 January 22).
//
// So we return, in order of preference:
//
//	$ZDB_PAXOS_DATA_DIR
//	$XDG_DATA_HOME/tube
//	$HOME/.local/state/tube
//
// If none of these is available, we will report an error
// to let the user know how to set the server's data directory.
func GetServerDataDir() (path string, err error) {
	app := os.Getenv("ZDB_PAXOS_DATA_DIR")
	dir := os.Getenv("XDG_DATA_HOME")
	home := os.Getenv("HOME")
	suffix := sep + "tube"
	switch {
	case app != "":
		path = app
	case dir != "":
		path = dir + suffix
	case home != "":
		path = home + sep + ".local" + sep + "state" + suffix
	default:
		return "", fmt.Errorf("zdb.GetDataDir() error: " +
			"could not determine server data directory. " +
			"We must have one of ZDB_PAXOS_DATA_DIR, XDG_DATA_HOME," +
			" or HOME environment variable set (in that order of preference).")
	}
	err = os.MkdirAll(path, 0700)
	return
}

// check for production mode problems, not
// just test config.
func (cfg *TubeConfig) ClientProdConfigSaneOrPanic() {
	cfg.ProdConfigSaneOrPanic(false)
}
func (cfg *TubeConfig) ReplicaProdConfigSaneOrPanic() {
	cfg.ProdConfigSaneOrPanic(true)
}
func (cfg *TubeConfig) ProdConfigSaneOrPanic(isReplica bool) {

	// basic sanity checks that the config is not
	// obviously borked.
	if cfg.PeerServiceName == "" {
		panic("must specify PeerServiceName; usually tube.TUBE_REPLICA or tube.TUBE_CLIENT")
	}
	if cfg.ConfigName == "" {
		panic("must specify ConfigName (and cannot have spaces)")
	}
	if cfg.ClusterID == "" {
		panic("must specify ClusterID (and cannot have spaces)")
	}
	if cfg.UseSimNet {
		panic("cannot have cfg.UseSimNet true in production.")
	}
	if cfg.TCPonly_no_TLS != cfg.RpcCfg.TCPonly_no_TLS {
		panic(fmt.Sprintf("cfg has inconsistent no_TLS settings: (%v)cfg.TCPonly_no_TLS != cfg.RpcCfg.TCPonly_no_TLS(%v)", cfg.TCPonly_no_TLS, cfg.RpcCfg.TCPonly_no_TLS))
	}

	if !cfg.TCPonly_no_TLS {
		if cfg.RpcCfg.ClientKeyPairName == "" {
			panic("cfg error: since using TLS, cannot have empty cfg.RpcCfg.ClientKeyPairName")
		}
		if cfg.RpcCfg.ServerKeyPairName == "" {
			panic("cfg error: since using TLS, cannot have empty cfg.RpcCfg.ServerKeyPairName")
		}
	}
	if cfg.ConfigDir != "" {
		path := filepath.Join(cfg.ConfigDir, "tuber_on_start_writable_path_check_ConfigDir_"+rndstring())
		fd, err := os.Create(path)
		if err != nil {
			panic(fmt.Sprintf("must be able to write into cfg.ConfigDir ('%v') but we could not: '%v'", cfg.ConfigDir, err))
		}
		fd.Close()
		os.Remove(path)
	}
	if cfg.DataDir != "" {
		path := filepath.Join(cfg.DataDir, "tuber_on_start_writable_path_check_DataDir_"+rndstring())
		fd, err := os.Create(path)
		if err != nil {
			panic(fmt.Sprintf("must be able to write into cfg.DataDir ('%v') but we could not: '%v'", cfg.ConfigDir, err))
		}
		fd.Close()
		os.Remove(path)
	}
	if isReplica {
		if cfg.NoDisk {
			panic("cannot have cfg.NoDisk true in production.")
		}
		if cfg.NoFaultTolDur > time.Hour*2 {
			panic("cannot have cfg.NoFaultTolDur longer than 2 hours")
		}
		if cfg.ClockDriftBound > time.Minute {
			panic("cannot have cfg.ClockDriftBound longer than 60 seconds")
		}
		if cfg.HeartbeatDur < 5*time.Millisecond {
			panic("cfg.HeartbeatDur must be >= 5 milliseconds")
		}
		if cfg.MinElectionDur < 50*time.Millisecond {
			panic("cfg.MinElectionDur must be >= 50 milliseconds")
		}
		if cfg.MinElectionDur < cfg.HeartbeatDur*5 {
			panic("cfg.MinElectionDur must be >= cfg.HeartbeatDur * 5")
		}
		if cfg.MinElectionDur <= cfg.ClockDriftBound*2 {
			panic("cfg.MinElectionDur must be > cfg.ClockDriftBound * 2")
		}
		if cfg.BatchAccumateDur < 0 || cfg.BatchAccumateDur > time.Second*10 {
			panic("cfg.BatchAccumateDur must be in the interval [0s, 10s]")
		}
	}
	if cfg.RpcCfg != nil {
		//if cfg.RpcCfg.QuietTestMode {
		//	panic("cannot have cfg.RpcCfg.QuietTestMode on in tuber")
		//}
		if cfg.RpcCfg.UseSimNet {
			panic("cannot have cfg.RpcCfg.UseSimNet on in tuber")
		}
		if !cfg.RpcCfg.ServerAutoCreateClientsToDialOtherServers {
			panic("must have cfg.RpcCfg.ServerAutoCreateClientsToDialOtherServers or we will not auto-cli dial out!")
		}
	} else {
		panic("must have cfg.RpcCfg! is nil! and RpcCfg must have cfg.RpcCfg.ServerAutoCreateClientsToDialOtherServers=true")
	}

	if isReplica {
		myAddr, ok := cfg.Node2Addr[cfg.MyName]
		if !ok {
			panic(fmt.Sprintf("could not find MyName:'%v' in Node2Addr map: '%#v'", cfg.MyName, cfg.Node2Addr))
		}
		_ = myAddr
	}

	// force a panic if len(cfg.RpcCfg.LimitedServiceNames) !=
	// len(cfg.RpcCfg.LimitedServiceMax)
	maxReplicaPerProcess := cfg.RpcCfg.GetLimitMax(string(TUBE_REPLICA))
	if maxReplicaPerProcess != 0 && maxReplicaPerProcess != 1 {
		panic(fmt.Sprintf("huh: only sane values are 0 or 1 for maxReplicaPerProcess: %v", maxReplicaPerProcess))
	}
	if maxReplicaPerProcess != 1 {
		alwaysPrintf("updating LimitedServiceNames:%v from %v -> 1", TUBE_REPLICA, maxReplicaPerProcess)
		cfg.RpcCfg.UpdateLimitMax(string(TUBE_REPLICA), 1)
	}
}

func rndstring() string {
	b := make([]byte, 10)
	cryrand.Read(b)
	return fmt.Sprintf("%x", b)
}

/*
// JoinCluster allows this node to join an existing cluster through a known node
func (s *RaftNode) JoinCluster(curMem *RaftNode) error {

	retry := s.cfg.AutoRetry
	for i := 0; i <= retry; i++ {
		// Create a join request message
		joinRequest := s.NewTicket("JoinCluster", func(in *RaftState) (out *RaftState) {
			// add ourselves

			// could just do this, but lets test the learning too,
			// when the learners may not be in the accept or propose
			// set. This could be useful for reconfiguration.
			//in.addPeers(PeersPrepAccept, s)
			in.addPeers(PeersAll, s)
			return in
		})

		// Send join request to contact node
		select {
		case curMem.reqCh <- joinRequest:
			vv("Join request sent from %v to %v", s.NodeID, curMem.NodeID)
		case <-s.stopChan.Chan:
			return ErrShutDown
		}

		select {
		case <-joinRequest.Done.Chan:
			//vv("%v joinRequest done.", s.NodeID)
		case <-s.stopChan.Chan:
			return ErrShutDown
		}

		if joinRequest.Err == nil {
			//vv("%v JoinCluster sees nil error from joinRequest.", s.NodeID)
			return nil
		}

		if i <= retry {
			// try to avoid live lock by waiting a random
			// amount of time between retries, using exponential backoff.
			dur := s.expBackoff.next()
			time.Sleep(dur)
			vv("%v JoinCluster re-trying (i=%v of %v) on err = '%v'", s.NodeID, i, retry, joinRequest.Err)
		}
	}

	return nil
}

func (s *RaftNode) AddPeers(which PeerSet, as ...*RaftNode) error {

	s.expBackoff.reset()

	retry := s.cfg.AutoRetry
	for i := 0; i <= retry; i++ {
		addPeersReq := s.NewTicket("AddPeers", func(in *RaftState) (out *RaftState) {
			//vv("AddPeers CAS executing")
			in.addPeers(which, as...)
			return in
		})

		// Send join request to contact node
		select {
		case s.reqCh <- addPeersReq:
			//vv("Join request sent from %v", s.NodeID)
		case <-s.stopChan.Chan:
			return ErrShutDown
		}

		select {
		case <-addPeersReq.Done.Chan:
			//vv("%v addPeersReq done.", s.NodeID)
		case <-s.stopChan.Chan:
			return ErrShutDown
		}

		if addPeersReq.Err == nil {
			//vv("%v JoinCluster sees nil error from addPeersReq.", s.NodeID)
			return nil
		}

		if i <= retry {
			// try to avoid live lock by waiting a random
			// amount of time between retries, using exponential backoff.
			dur := s.expBackoff.next()
			time.Sleep(dur)
			vv("%v AddPeers() re-trying (i=%v of %v) on err = '%v'", s.NodeID, i, retry, addPeersReq.Err)
		}
	}

	return nil
}

func (s *RaftNode) RemovePeers(which PeerSet, as ...*RaftNode) error {

	s.expBackoff.reset()

	retry := s.cfg.AutoRetry
	for i := 0; i <= retry; i++ {

		leaveReq := s.NewTicket("RemovePeers", func(in *RaftState) (out *RaftState) {
			in.removePeers(which, as...)
			return in
		})

		select {
		case s.reqCh <- leaveReq:
			vv("Leave notification sent")
		case <-s.stopChan.Chan:
			return ErrShutDown
		}

		select {
		case <-leaveReq.Done.Chan:
			vv("leaveReq done, err = '%v'", leaveReq.Err)
		case <-s.stopChan.Chan:
			vv("shutting down")
		}

		if leaveReq.Err == nil {
			//vv("%v RemovePeers sees nil error from leaveReq.", s.NodeID)
			return nil
		}

		if i <= retry {

			// try to avoid live lock by waiting a random
			// amount of time between retries, using exponential backoff.
			dur := s.expBackoff.next()
			time.Sleep(dur)
			vv("%v RemovePeers re-trying (i=%v of %v) on err = '%v'", s.NodeID, i, retry, leaveReq.Err)
		}
	}

	return nil
}

// Write a new value under key, or update key's existing
// value to val if key already exists.
//
// Setting waitForDur = 0 means the Write will
// wait indefinitely for the write to complete, and
// is a reasonable default. This provides strong consistency
// (linearizability) from all live replicas.
//
// Key versioning is used to make Write's action
// linearizable over a global total
// order of writes and reads to all live replicas. Any node can
// issue a Write, and any node can issue a Read, and
// both are linearizable with respect to each other.
// This provides the strongest and most intuitive
// consistency. It also gives us composability --
// a synonym for ease of use.
func (s *RaftNode) Write(key Key, val Val, waitForDur time.Duration) (err error) {
	//vv("%v Write called, goro %v", s.NodeID, GoroNumber())
	s.expBackoff.reset()

	retry := s.cfg.AutoRetry
	for i := 0; i <= retry; i++ {

		var writeReq *Ticket
		writeReq = s.NewTicket("Write", func(in *RaftState) (out *RaftState) {
			//vv("%v writeReq FuncCAS called, tkt='%v'", s.NodeID, writeReq)
			in.HashMap[key] = val
			return in
		})

		select {
		case s.reqCh <- writeReq:
			//vv("Write request sent")
			if writeReq.Err != ErrConflict {
				panicOn(err)
				panicOn(writeReq.Err)
			}
		case <-s.stopChan.Chan:
			return ErrShutDown
		case <-s.halt.ReqStop.Chan:
			return ErrTimeOut
		}

		select {
		case <-writeReq.Done.Chan:
			//vv("%v writeReq done, writeReq= '%v'", s.NodeID, writeReq)
		case <-s.stopChan.Chan:
			//vv("shutting down")
			return ErrShutDown
		case <-s.halt.ReqStop.Chan:
			return ErrShutDown
		}

		if writeReq.Err == nil {
			//vv("%v RemovePeers sees nil error from writeReq.", s.NodeID)
			return nil
		}

		if writeReq.Err == ErrConflict {
			vv("%v Write sees ErrConflict, trying again?", s.NodeID)

			if i <= retry {

				// try to avoid live lock by waiting a random
				// amount of time between retries, using exponential backoff.
				dur := s.expBackoff.next()
				time.Sleep(dur)
				vv("%v Write re-trying (i=%v of %v) on err = '%v'", s.NodeID, i, retry, writeReq.Err)
			}
		} else {
			break
		}
	}

	return
}

// Read a key's value. Setting waitForDur = 0
// means the Read will wait indefinitely for a valid key.
//
// For production use, you may want a non-zero waitForDur
// timeout, if you are able to (and/or don't want to) wait
// for some tail events like the replica recovery
// protocol to finish.
//
// A waitForDur of -1 means try locally, but return
// ErrNotFound quickly if there is nothing here.
//
// The default waitForDur of 0 avoids
// races with writers, and still provides the fastest
// possible local read if the key is valid (not
// being written at the moment).
//
// Read only ever returns a linearizable, replicated value
// when the returned error is nil. Non-nil errors can
// include ErrTimeOut, ErrShutDown, and ErrKeyNotFound, in
// which case val will be undefined but typically nil.
func (s *RaftNode) Read(key Key, waitForDur time.Duration) (val Val, err error) {
	//vv("%v Read called, goro %v", s.NodeID, GoroNumber())

	s.expBackoff.reset()

	retry := s.cfg.AutoRetry
	for i := 0; i <= retry; i++ {

		readReq := s.NewTicket("Read", func(in *RaftState) (out *RaftState) {
			var ok bool
			val, ok = in.HashMap[key]
			if !ok {
				// normal and expected under Test000_ read_test.go
				//panic(fmt.Sprintf("key not found: '%v'; HashMap: '%#v'", key, in.HashMap))
				err = ErrKeyNotFound
			}
			//if val == "" { // expected in Test000_
			//	panic(fmt.Sprintf("key: '%v' had empty val: '%v'; HashMap: '%#v'", key, val, in.HashMap))
			//}
			return in
		})
		// no retries were observed.
		// neither of the above panics fired, and yet linz_test 010 got
		// linz_test.go:20 2025-04-20 01:51:50.816 -0500 CDT n= 24
		// linz_test.go:46: write a:'123' to node0, read back from node(i=20) ''; nodes gave so far: '[]string{"123", "123", "123", "123", "123", "123", "123", "123", "123", "123", "123", "123", "123", "123", "123", "123", "123", "123", "123", "123", ""}'; kv='map[arb.Key]arb.Val{"a":"123"}'; N=30
		// --- FAIL: Test010_larger_cluster (227.49s)

		select {
		case s.reqCh <- readReq:
			//vv("Read request sent")
		case <-s.stopChan.Chan:
			err = ErrShutDown
			return
		case <-s.halt.ReqStop.Chan:
			err = ErrTimeOut
			return
		}

		select {
		case <-readReq.Done.Chan:
			//vv("%v readReq done, readReq= '%v'", s.NodeID, readReq)
			if err == ErrKeyNotFound {
				return
			}
			if readReq.Err != ErrConflict {
				panicOn(readReq.Err)
			}
			if readReq.Err == nil && !readReq.called {
				panic(fmt.Sprintf("reaReq.FuncCAS was not called! readReq.Err =%v", readReq.Err)) // hit! how??? probably an ErrConflict that just needs retry add Err==nil condition.
			}
		case <-s.stopChan.Chan:
			vv("shutting down")
			err = ErrShutDown
			return
		case <-s.halt.ReqStop.Chan:
			err = ErrShutDown
			return
		}

		if readReq.Err == nil {
			//vv("%v Read sees nil error from readReq.", s.NodeID)
			return
		}

		if readReq.Err == ErrConflict {
			vv("%v Read sees ErrConflict, trying again? %v", s.NodeID, i < retry)

			if i <= retry {

				// try to avoid live lock by waiting a random
				// amount of time between retries, using exponential backoff.
				dur := s.expBackoff.next()
				time.Sleep(dur)
				vv("%v Read re-trying (i=%v of %v) on err = '%v'", s.NodeID, i, retry, readReq.Err)
			} else {
				break
			}
		} // if conflict
	} // for
	return
}
*/
