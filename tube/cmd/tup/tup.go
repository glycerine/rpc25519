package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	//"path/filepath"
	//"sort"
	//"time"

	//rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/ipaddr"
	"github.com/glycerine/zdb/tube"
	"github.com/glycerine/zdb/tube/art"
)

var sep = string(os.PathSeparator)

type ConfigTup struct {
	ContactName string // -c name of node to contact
	Help        bool   // -h for help, false, show this help
	Verbose     bool   // -v verbose: show config/connection attempts.
}

func (c *ConfigTup) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.ContactName, "c", "", "name of node to contact (defaults to leader)")
	fs.BoolVar(&c.Help, "h", false, "show this help")
	fs.BoolVar(&c.Verbose, "v", false, "verbose diagnostics logging to stdout")
}

func (c *ConfigTup) FinishConfig(fs *flag.FlagSet) (err error) {
	return
}
func (c *ConfigTup) SetDefaults() {}

func main() {
	cmdCfg := &ConfigTup{}

	fs := flag.NewFlagSet("tup", flag.ExitOnError)
	cmdCfg.SetFlags(fs)
	fs.Parse(os.Args[1:])
	cmdCfg.SetDefaults()
	err := cmdCfg.FinishConfig(fs)
	panicOn(err)

	if cmdCfg.Verbose {
		verboseVerbose = true
	}
	if cmdCfg.Help {
		fmt.Fprintf(os.Stderr, "tup help:\n")
		fs.PrintDefaults()
		return
	}

	// first connect, then run repl
	dir := tube.GetConfigDir()
	pathCfg := dir + "/" + "tup.default.config"
	if fileExists(pathCfg) {
		pp("using config file: '%v'", pathCfg)
	} else {
		fd, err := os.Create(pathCfg)
		panicOn(err)
		cfg := &tube.TubeConfig{
			// distinguish multiple tup clients
			MyName:          "tup_" + tube.CryRand15B(),
			PeerServiceName: tube.TUBE_CLIENT,
		}
		fmt.Fprintf(fd, "%v\n", cfg.SexpString(nil))
		fd.Close()
		fmt.Fprintf(os.Stderr, "tup error: no config file. Created one from template in '%v'. Please complete it.\n", pathCfg)
		os.Exit(1)
	}
	by, err := os.ReadFile(pathCfg)
	panicOn(err)
	pp("got by = '%v'", string(by))

	cfg, err := tube.NewTubeConfigFromSexpString(string(by), nil)
	panicOn(err)
	// distinguish multiple tup clients
	cfg.MyName = "tup_" + tube.CryRand15B()
	cfg.PeerServiceName = tube.TUBE_CLIENT

	myHost := ipaddr.GetExternalIP()
	myPort := ipaddr.GetAvailPort()
	cfg.RpcCfg.ServerAddr = fmt.Sprintf("%v:%v", myHost, myPort)

	// set up our config
	const quiet = false
	const isTest = false
	cfg.Init(quiet, isTest)

	cfg.UseSimNet = false

	cfg.RpcCfg.TCPonly_no_TLS = cfg.TCPonly_no_TLS
	cfg.RpcCfg.ServerAutoCreateClientsToDialOtherServers = true
	cfg.RpcCfg.QuietTestMode = true

	cfg.ClientProdConfigSaneOrPanic()

	pp("cfg = '%v'", cfg.ShortSexpString(nil))

	//nodeID := rpc.NewCallID("")
	name := cfg.MyName
	node := tube.NewTubeNode(name, cfg)

	err = node.InitAndStart()
	panicOn(err)
	defer node.Close()

	var leaderURL string
	greet := cmdCfg.ContactName
	if greet == "" {
		greet = cfg.InitialLeaderName
	}
	addr, ok := cfg.Node2Addr[greet]
	if !ok {
		fmt.Fprintf(os.Stderr, "error: giving up, as no address! gotta have cfg.InitialLeaderName or -c name of node to contact (we use the names listed in the config file '%v' under Node2Addr).\n", pathCfg)
		os.Exit(1)
	} else {
		leaderURL = tube.FixAddrPrefix(addr)
		if cmdCfg.ContactName == "" {
			pp("by default we contact cfg.InitialLeaderName='%v'; addr='%v' -> leaderURL = '%v'", cfg.InitialLeaderName, addr, leaderURL)
		} else {
			pp("requested cmdCfg.ContactName='%v' maps to addr='%v' -> URL = '%v'", cmdCfg.ContactName, addr, leaderURL)
		}
	}

	noGood := make(map[string]bool) // list those we tried and rejected
	noGood[cfg.MyName] = true       // skip self
	ctx := context.Background()
	var onlyPossibleAddr string
	_ = onlyPossibleAddr
	onlyPossibleAddr, err = node.UseLeaderURL(ctx, leaderURL)
	if err != nil {
		noGood[leaderURL] = true
		// try others
		for name, addr := range cfg.Node2Addr {
			if name == greet || name == cfg.MyName {
				noGood[name] = true
				continue
			}
			pp("instead trying addr='%v'", addr)
			leaderURL = tube.FixAddrPrefix(addr)
			_, err = node.UseLeaderURL(ctx, leaderURL)
			if err == nil {
				break
			}
			noGood[name] = true
		}
		panicOn(err)
	}
	// note we can still get stuck talking to a
	// node that has been removed from current membership.
	// Thus we try again below, consulting the noGood map.
	//vv("back from node.UseLeaderURL(leaderURL='%v')", leaderURL)

	newestMemberConfig, insp, actualLeaderURL, leaderName, onlyPossibleAddr, err := node.GetPeerListFrom(ctx, leaderURL)
	panicOn(err)
	_ = insp
	_ = newestMemberConfig
	//vv("GetPeerListFrom(leaderURL='%v') -> newestMemberConfig = '%v'", leaderURL, newestMemberConfig)
	pp("leaderName = '%v'; GetPeerListFrom(leaderURL='%v') -> actualLeaderURL = '%v'", leaderName, leaderURL, actualLeaderURL)
	if actualLeaderURL != "" && actualLeaderURL != leaderURL {
		pp("use actual='%v' rather than orig='%v'", actualLeaderURL, leaderURL)
		leaderURL = actualLeaderURL
	}

	if actualLeaderURL == "" {
		// keep searching! that was a dead end.
		// try others, again.

		var name, addr string
		for name, addr = range cfg.Node2Addr {
			if noGood[name] {
				continue
			}
			pp("instead trying addr='%v'", addr)
			leaderURL = tube.FixAddrPrefix(addr)
			_, err = node.UseLeaderURL(ctx, leaderURL)
			if err == nil {
				break
			}
			noGood[name] = true
		}
		if err != nil {
			alwaysPrintf("could not contact anyone. last attempt to '%v' (addr: '%v') => err='%v'\n", name, addr, err)
			os.Exit(1)
		}
	}

	// repeat this part from above, now the we have actual leader connection.
	newestMemberConfig, insp, actualLeaderURL, leaderName, onlyPossibleAddr, err = node.GetPeerListFrom(ctx, leaderURL)
	panicOn(err)
	_ = insp
	_ = newestMemberConfig
	//pp("GetPeerListFrom(leaderURL='%v') -> newestMemberConfig = '%v'", leaderURL, newestMemberConfig)
	pp("leaderName = '%v'; GetPeerListFrom(leaderURL='%v') -> actualLeaderURL = '%v'", leaderName, leaderURL, actualLeaderURL)
	if actualLeaderURL != "" && actualLeaderURL != leaderURL {
		pp("use actual='%v' rather than orig='%v'", actualLeaderURL, leaderURL)
		leaderURL = actualLeaderURL
	}

	sess, err := node.CreateNewSession(ctx, leaderURL)
	panicOn(err)
	defer sess.Close()
	//pp("back from node.CreateNewSession(leaderURL='%v')", leaderURL)

	needNewSess := func(sess *tube.Session, err error) (s2 *tube.Session) {
		if err == nil {
			return sess
		}
		errs := err.Error()
		if strings.Contains(errs, "call CreateNewSession first") {
			sess.Close()
			s2, err := node.CreateNewSession(ctx, leaderURL)
			panicOn(err)
			return s2
		}
		return sess
	}
	// repl loop

	reader := bufio.NewReader(os.Stdin)

	table := "base"
	fmt.Printf(`tup: the tube updater; use tup -v for diagnostics.
commands: .key               : read key from current table
          key                : read key from current table (if not keyword)
          !key newval        : write newval to key in current table
          @table key newval  : write newval to key in table
          ,table key         : read key from table
          +table {key} {endx}: read  ascending key, key+1, ..., endx from table
          -table {key} {endx}: read descending key, key-1, ..., endx from table
                             :  {key} {endx} optional. + alone for current table
          del key            : delete key from current table
          show               : show all tables
          show table         : show all keys in table
          ls                 : show all keys in current table
          mv old new         : rename table old to new
          use table          : table becomes the current table
          rmtable table      : drop the named table
          newtable table     : make a new table
          cas key old new    : if key holds old, replace old with new (compare and swap).

keywords: cas, newtable, rmtable, use, mv, ls, show, del

`)
repl:
	for {
		targetTable := table
		fmt.Printf("[%v connected](table '%v') > ", leaderName, table)
		line, err := getLine(reader)
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "error at repl getting a line: '%v'\n", err)
			}
			os.Exit(1)
		}
		args := strings.Fields(line)
		//pp("line '%v' -> args '%#v'", line, args)

		if len(args) == 0 {
			continue repl
		}

		na := len(args)
		if na > 4 {
			fmt.Fprintf(os.Stderr, "tup syntax error: too many words.\n{use,rename,ls,show {table},cas key old new\n!x 100 will assign 100 to x.")
			continue repl
		}

		isSet := false
		isGet := false
		isDel := false
		isRenameTable := false
		isShowKeys := false
		isMakeTable := false
		isDeleteTable := false
		isRangeScan := false
		isRangeScanDescend := false

		var newTableName, keyEndx string
		key := args[0]
		var value string
		switch len(args) {
		case 1:
			switch key {
			case "show": // list all tables
				isShowKeys = true
				key = ""
			case "ls": // list all keys of current table
				isShowKeys = true
				key = targetTable
			default:
				if key[0] == '.' {
					if key == "." {
						continue repl
					}
					key = key[1:]
					isGet = true
				} else if key[0] == ',' {
					if key == "," {
						fmt.Printf("no table to read from\n")
						continue repl
					}
					targetTable = key[1:]
					fmt.Printf("error: from table '%v': no key requested\n", targetTable)
					continue repl
				} else if key[0] == '+' {
					// full table key range scan, ascending
					if key == "+" {
						// use default targetTable
					} else {
						targetTable = key[1:]
					}
					isRangeScan = true
					isRangeScanDescend = false
					key = ""
				} else if key[0] == '-' {
					// full table key range scan, descending
					if key == "-" {
						// use default targetTable
					} else {
						targetTable = key[1:]
					}
					isRangeScan = true
					isRangeScanDescend = true
					key = ""
				} else {
					isGet = true
				}
			}
		case 2:
			switch key {
			case "use":
				table = args[1]
				fmt.Printf("using table '%v'\n", table)
			case "del":
				isDel = true
				key = args[1]
			case "show":
				isShowKeys = true
				key = args[1]
			case "ls":
				isShowKeys = true
				key = args[1]
			case "newtable":
				isMakeTable = true
				key = args[1]
			case "rmtable":
				isDeleteTable = true
				key = args[1]
			default:
				if key[0] == '!' {
					if key == "!" {
						fmt.Printf("no key to set\n")
						continue repl
					}
					isSet = true
					key = key[1:]
					value = args[1]
				} else if key[0] == ',' {
					if key == "," {
						fmt.Printf("no table to read from\n")
						continue repl
					}
					targetTable = key[1:]
					key = args[1]
					//pp("isGet: targetTable = '%v', key='%v'", targetTable, key)
					isGet = true
				} else if key[0] == '@' {
					if key == "@" {
						fmt.Printf("no table to write to\n")
						continue repl
					}
					targetTable = key[1:]
					key = args[1]
					fmt.Printf("error: no value supplied to write to key '%v' in table '%v'\n", key, targetTable)
					continue

				} else if key[0] == '+' {
					// key range scan, ascending, no endx
					if key == "+" {
						fmt.Printf("no table to read key range from\n")
						continue repl
					}
					isRangeScan = true
					isRangeScanDescend = false
					targetTable = key[1:]
					key = args[1]
					//keyEndx = args[2]
					//pp("rangeScan targetTable='%v'; key='%v'; keyEndx='%v'", targetTable, key, keyEndx)
				} else if key[0] == '-' {
					// key range scan, descending, no endx
					if key == "-" {
						fmt.Printf("no table to read key range from\n")
						continue repl
					}
					isRangeScan = true
					isRangeScanDescend = true
					targetTable = key[1:]
					key = args[1]
					//keyEndx = args[2]
					//pp("rangeScan targetTable='%v'; key='%v'; keyEndx='%v'", targetTable, key, keyEndx)
				}
			} // end switch key
		case 3:
			switch key {
			case "mv": // ,"rename":
				isRenameTable = true
				key = args[1]
				newTableName = args[2]
			default:
				if key[0] == '@' {
					if key == "@" {
						fmt.Printf("no table to write to\n")
						continue repl
					}
					isSet = true
					targetTable = key[1:]
					key = args[1]
					value = args[2]
				} else if key[0] == '+' {
					// key range scan, ascending
					if key == "+" {
						fmt.Printf("no table to read key range from\n")
						continue repl
					}
					isRangeScan = true
					isRangeScanDescend = false
					targetTable = key[1:]
					key = args[1]
					keyEndx = args[2]
					//pp("rangeScan targetTable='%v'; key='%v'; keyEndx='%v'", targetTable, key, keyEndx)
				} else if key[0] == '-' {
					// key range scan, descending
					if key == "-" {
						fmt.Printf("no table to read key range from\n")
						continue repl
					}
					isRangeScan = true
					isRangeScanDescend = true
					targetTable = key[1:]
					key = args[1]
					keyEndx = args[2]
					//pp("rangeScan targetTable='%v'; key='%v'; keyEndx='%v'", targetTable, key, keyEndx)
				} else {
					fmt.Printf("syntax error: the 3 word commands start with 'mv' and '@'.\n")
					continue repl
				}
			}
		case 4:
			switch key {
			case "cas":
				// cas key old new
				key = args[1]
				oldval := args[2]
				newval := args[3]
				tkt, err := sess.CAS(ctx, tube.Key(table), tube.Key(key), tube.Val(oldval), tube.Val(newval), 0)
				if err != nil {
					fmt.Printf("error: %v\n", err)
					sess = needNewSess(sess, err)
				} else {
					if tkt.CASwapped {
						fmt.Printf("cas accepted: %v <- %v\n", key, string(newval))
					} else {
						fmt.Printf("cas rejected, cur val: %v\n", string(tkt.CASRejectedBecauseCurVal))
					}
				}
			default:
				fmt.Printf("syntax error: the only 4 word command is 'cas'\n")
				continue repl
			}
		}

		switch {
		case isRangeScan:
			tktRange, err := sess.ReadKeyRange(ctx, tube.Key(targetTable), tube.Key(key), tube.Key(keyEndx), isRangeScanDescend, 0)
			if err != nil {
				fmt.Printf("error in range scan of table '%v' for keys from '%v':'%v'; error= %v\n", targetTable, key, keyEndx, err)
				sess = needNewSess(sess, err)
			} else {
				if tktRange.Err != nil {
					fmt.Printf("error in range scan of table from '%v' to '%v': %v\n", targetTable, key, keyEndx, tktRange.Err)
					sess = needNewSess(sess, err)

				} else {
					if tktRange.KeyValRangeScan == nil {
						fmt.Printf("(0 keys back)\n") // empty result set back (from range scan of table from '%v' to '%v').\n", targetTable, key, keyEndx)
					} else {
						seen := 0
						if isRangeScanDescend {
							for k, lf := range art.Descend(tktRange.KeyValRangeScan, nil, nil) {
								fmt.Printf("(from table '%v') read key '%v': %v\n", targetTable, string(k), string(lf.Value))
								seen++
							}
						} else {
							for k, lf := range art.Ascend(tktRange.KeyValRangeScan, nil, nil) {
								fmt.Printf("(from table '%v') read key '%v': %v\n", targetTable, string(k), string(lf.Value))
								seen++
							}
						}
						fmt.Printf("(%v keys back)\n", seen)
					}
				}
			}

		case isRenameTable:
			_, err := sess.RenameTable(ctx, tube.Key(key), tube.Key(newTableName), 0)
			if err != nil {
				fmt.Printf("error renaming table from '%v' to '%v': %v\n", key, newTableName, err)
				sess = needNewSess(sess, err)

			} else {
				fmt.Printf("renamed table: '%v' -> '%v'\n", key, newTableName)
			}
		case isMakeTable:
			_, err := sess.MakeTable(ctx, tube.Key(key), 0)
			if err != nil {
				fmt.Printf("error on make table '%v': %v\n", key, err)
				sess = needNewSess(sess, err)
			} else {
				fmt.Printf("made table: '%v'\n", key)
			}

		case isDeleteTable:
			_, err := sess.DeleteTable(ctx, tube.Key(key), 0)
			if err != nil {
				fmt.Printf("error on delete table '%v': %v\n", key, err)
				sess = needNewSess(sess, err)

			} else {
				fmt.Printf("deleted table: '%v'\n", key)
			}

		case isDel:
			_, err := sess.DeleteKey(ctx, tube.Key(table), tube.Key(key), 0)
			if err != nil {
				fmt.Printf("error on delete key '%v' from table '%v': %v\n", key, table, err)
				sess = needNewSess(sess, err)

			} else {
				fmt.Printf("deleted from table '%v' key: '%v'\n", table, key)
			}

		case isShowKeys:
			tkt, err := sess.ShowKeys(ctx, tube.Key(key), 0)
			if err != nil {
				fmt.Printf("error: %v\n", err)
				sess = needNewSess(sess, err)

			} else {
				if key == "" {
					fmt.Printf("available tables:\n%v\n", string(tkt.Val))
				} else {
					fmt.Printf("available keys in table '%v':\n%v\n", key, string(tkt.Val))
				}
			}
		case isSet:
			_, err := sess.Write(ctx, tube.Key(targetTable), tube.Key(key), tube.Val(value), 0)
			if err != nil {
				fmt.Printf("error: %v\n", err)
				sess = needNewSess(sess, err)

			} else {
				fmt.Printf("wrote: %v <- %v (in table '%v')\n", key, value, targetTable)
			}

		case isGet:
			tktR, err := sess.Read(ctx, tube.Key(targetTable), tube.Key(key), 0)
			if err != nil {
				fmt.Printf("error on read key '%v': %v\n", key, err)
				sess = needNewSess(sess, err)

			} else {
				readVal := tktR.Val
				fmt.Printf("(from table '%v') read key '%v': %v\n", targetTable, key, string(readVal))
			}
		}
	} // end repl for loop
}

func getLine(reader *bufio.Reader) (string, error) {
	line := make([]byte, 0)
	for {
		linepart, hasMore, err := reader.ReadLine()
		if err != nil {
			return "", err
		}
		line = append(line, linepart...)
		if !hasMore {
			break
		}
	}
	return string(line), nil
}
