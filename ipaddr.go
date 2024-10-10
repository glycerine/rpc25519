package rpc25519

import (
	"fmt"
	"net"
	//"os"
	"regexp"
	"runtime"
	"strings"
	"time"
)

var validIPv4addr = regexp.MustCompile(`^[0-9]+[.][0-9]+[.][0-9]+[.][0-9]+$`)

var privateIPv4addr = regexp.MustCompile(`(^127\.0\.0\.1)|(^10\.)|(^172\.1[6-9]\.)|(^172\.2[0-9]\.)|(^172\.3[0-1]\.)|(^192\.168\.)`)

// IsRoutableIPv4 returns true if the string in ip represents an IPv4 address that is not
// private. See http://en.wikipedia.org/wiki/Private_network#Private_IPv4_address_spaces
// for the numeric ranges that are private. 127.0.0.1, 192.168.0.1, and 172.16.0.1 are
// examples of non-routables IP addresses.
func IsRoutableIPv4(ip string) bool {
	match := privateIPv4addr.FindStringSubmatch(ip)
	if match != nil {
		return false
	}
	return true
}

// GetExternalIP tries to determine the external IP address
// used on this host.
func GetExternalIP() string {
	if runtime.GOOS == "windows" {
		return "127.0.0.1"
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		panic(err)
	}

	valid := []string{}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			addr := ipnet.IP.String()
			match := validIPv4addr.FindStringSubmatch(addr)
			if match != nil {
				if addr != "127.0.0.1" {
					valid = append(valid, addr)
				}
			}
		}
	}
	switch len(valid) {
	case 0:
		return "127.0.0.1"
	case 1:
		return valid[0]
	default:
		// try to get a routable ip if possible.
		for _, ip := range valid {
			if IsRoutableIPv4(ip) {
				return ip
			}
		}
		// give up, just return the first.
		return valid[0]
	}
}

// GetExternalIPAsInt calls GetExternalIP() and then converts
// the resulting IPv4 string into an integer.
func GetExternalIPAsInt() int {
	s := GetExternalIP()
	ip := net.ParseIP(s).To4()
	if ip == nil {
		return 0
	}
	sum := 0
	for i := 0; i < 4; i++ {
		mult := 1 << (8 * uint64(3-i))
		//fmt.Printf("mult = %d\n", mult)
		sum += int(mult) * int(ip[i])
		//fmt.Printf("sum = %d\n", sum)
	}
	//fmt.Printf("GetExternalIPAsInt() returns %d\n", sum)
	return sum
}

// GetAvailPort asks the OS for an unused port.
// There's a race here, where the port could be grabbed by someone else
// before the caller gets to Listen on it, but in practice such races
// are rare. Uses net.Listen("tcp", ":0") to determine a free port, then
// releases it back to the OS with Listener.Close().
func GetAvailPort() int {
	l, _ := net.Listen("tcp", ":0")
	r := l.Addr()
	l.Close()
	return r.(*net.TCPAddr).Port
}

// GenAddress generates a local address by calling GetAvailPort() and
// GetExternalIP(), then prefixing them with 'tcp://'.
func GenAddress() string {
	port := GetAvailPort()
	ip := GetExternalIP()
	s := fmt.Sprintf("tcp://%s:%d", ip, port)
	//fmt.Printf("GenAddress returning '%s'\n", s)
	return s
}

// reduce `tcp://blah:port` to `blah:port`
var validSplitOffProto = regexp.MustCompile(`^[^:]*://(.*)$`)

// StripNanomsgAddressPrefix removes the 'tcp://' prefix from
// nanomsgAddr.
func StripNanomsgAddressPrefix(nanomsgAddr string) (suffix string, err error) {

	match := validSplitOffProto.FindStringSubmatch(nanomsgAddr)
	if match == nil || len(match) != 2 {
		return "", fmt.Errorf("could not strip prefix tcp:// from nanomsg address '%s'", nanomsgAddr)
	}
	return match[1], nil
}

func WaitUntilCanConnect(addr string) {

	stripped, err := StripNanomsgAddressPrefix(addr)
	if err != nil {
		panic(err)
	}

	t0 := time.Now()
	for {
		cn, err := net.Dial("tcp", stripped)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		cn.Close()
		break
	}
	vv("WaitUntilCanConnect finished after %v", time.Since(t0))
}

func removeNetworkPrefix(address string) string {
	// Split the address into two parts, at the first occurrence of "://"
	parts := strings.SplitN(address, "://", 2)

	// If the split resulted in two parts, return the second part (i.e., address without prefix)
	if len(parts) == 2 {
		return parts[1]
	}

	// Otherwise, return the original address (no prefix found)
	return address
}

// if it needs [] ipv6 brackets, add them
func WrapWithBrackets(local string) string {

	if local == "" {
		return local
	}
	if local[0] == '[' {
		return local
	}

	ip := net.ParseIP(local)
	if ip != nil {
		if ip.To4() == nil {
			// is IP v6
			return "[" + local + "]"
		}
	}
	return local
}

// LocalAddrMatching finds a matching interface IP to a server destination address
//
// addr should b "host:port" of server, we'll find the local IP to use.
func LocalAddrMatching(addr string) (local string, err error) {

	defer func() {
		// wrap IPv6 in [] if need be.
		if local != "" && err == nil {
			if local[0] == '[' {
				return
			}
			ip := net.ParseIP(local)
			if ip != nil {
				if ip.To4() == nil {
					// is IP v6
					local = "[" + local + "]"
				}
			}
		}
	}()

	// Resolve the server address
	addr = removeNetworkPrefix(addr)

	// if localhost, return same
	isLocal, host := IsLocalhost(addr)
	if isLocal {
		return host, nil
	}

	serverAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return "", fmt.Errorf("Failed to resolve server address: %v", err)
	}

	remote6 := serverAddr.IP.To4() == nil

	// Get a list of network interfaces on the machine
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("Failed to get network interfaces: %v", err)
	}

	// Iterate over interfaces and inspect their addresses
	var selectedIP net.IP
	for _, iface := range interfaces {
		// Ignore down or loopback interfaces
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			//fmt.Printf("Failed to get addresses for interface %s: %v\n", iface.Name, err)
			continue
		}

		// Iterate over each address of the interface
		for _, addr := range addrs {
			var ip net.IP
			var ipNet *net.IPNet

			// Check if the address is an IPNet (which gives both IP and mask)
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
				ipNet = v
			case *net.IPAddr:
				ip = v.IP
			}

			// Skip if no IPNet
			if ip == nil || ipNet == nil {
				//if ip == nil || ipNet == nil {
				continue
			}

			local6 := ip.To4() == nil
			if local6 != remote6 {
				continue
			}

			// If the server IP is private, check for same subnet
			if isPrivateIP(serverAddr.IP) && isPrivateIP(ip) {
				if ipNet.Contains(serverAddr.IP) {
					return ip.String(), nil
				}
			} else if !isPrivateIP(serverAddr.IP) && !isPrivateIP(ip) {
				// If the server has a public IP, pick a public client IP
				return ip.String(), nil
				//fmt.Printf("Selected local interface: %s, IP: %s (Public IP)\n", iface.Name, ip.String())
			}
		}

		// Stop searching if a valid IP is found
		if selectedIP != nil {
			break
		}
	}

	if selectedIP == nil {
		return "", fmt.Errorf("No suitable local interface found that can connect to the server '%v'", serverAddr)
	}

	return selectedIP.String(), nil
}

// Helper function to check if an IP is private
func isPrivateIP(ip net.IP) bool {
	privateIPBlocks := []net.IPNet{
		// 10.0.0.0/8
		{IP: net.IPv4(10, 0, 0, 0), Mask: net.CIDRMask(8, 32)},
		// 172.16.0.0/12
		{IP: net.IPv4(172, 16, 0, 0), Mask: net.CIDRMask(12, 32)},
		// 192.168.0.0/16
		{IP: net.IPv4(192, 168, 0, 0), Mask: net.CIDRMask(16, 32)},
	}

	for _, block := range privateIPBlocks {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

func IsLocalhost(ipStr string) (isLocal bool, hostOnlyNoPort string) {
	host, _, err := net.SplitHostPort(ipStr)
	if err == nil {
		ipStr = host
	}
	hostOnlyNoPort = ipStr
	ip := net.ParseIP(ipStr)
	if ip == nil {
		isLocal = false // Invalid IP
	}
	isLocal = ip.IsLoopback() || ip.Equal(net.IPv4(127, 0, 0, 1)) || ip.Equal(net.IPv6loopback)
	return
}
