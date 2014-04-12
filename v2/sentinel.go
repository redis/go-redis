package redis

import (
	"fmt"
	"github.com/golang/glog"
	"net"
	"strings"
	"sync"
)

// NewSentinelClient creates a client which connects via Sentinel
func NewSentinelClient(name string, sentinels []string, opt *Options) *Client {
	// Sentinel options
	opts := *opt
	opts.Password = ""
	opts.PoolSize = 1
	opts.DB = 0

	lock := new(sync.Mutex) // sentinels lock
	dial := func() (net.Conn, error) {
		return sentinelDial(name, sentinels, lock, &opts)
	}
	return newClient(opt, dial)
}

// Establish a new connection via sentinel
func sentinelDial(name string, sentinels []string, lock *sync.Mutex, opt *Options) (net.Conn, error) {
	var unreachable int

	for n, addr := range sentinels {
		master, known, err := sentinelDiscover(name, addr, opt)

		if err == nil {
			sentinelUpdate(n, sentinels, known, lock)
			return net.DialTimeout("tcp", master, opt.getDialTimeout())
		} else if err == Nil {
			continue
		} else if err.Error()[:9] == "IDONTKNOW" {
			glog.Warningf("sentinel '%s' doesn't know the address of '%s'", addr, name)
		} else {
			unreachable++
			glog.Warningf("sentinel connect failed: %s", err.Error())
		}
	}

	if unreachable == len(sentinels) {
		return nil, fmt.Errorf("redis: all sentinels are unreachable")
	}

	return nil, fmt.Errorf("redis: unknown master '%s'", name)
}

// Returns the master address, the list of known sentinels and an error (if any)
func sentinelDiscover(name, addr string, opt *Options) (string, []string, error) {
	opt.Addr = addr
	client := NewTCPClient(opt)
	defer client.Close()

	mcmd := NewStringSliceCmd("SENTINEL", "get-master-addr-by-name", name)
	scmd := NewStringSliceCmd("SENTINEL", "sentinels", name)

	client.Process(mcmd)
	if mcmd.err == nil {
		client.Process(scmd)
	}

	return strings.Join(mcmd.val, ":"), scmd.val, mcmd.err
}

// Updates the list of sentinels
func sentinelUpdate(working int, sentinels []string, known []string, lock *sync.Mutex) {
	lock.Lock()
	defer lock.Unlock()

	// Push working sentinel to the top
	if working > 0 {
		sentinels[0], sentinels[working] = sentinels[working], sentinels[0]
	}

	// Append known sentinels
	if len(known) > 0 {
		lookup := make(map[string]bool, len(sentinels))
		for _, s := range sentinels {
			lookup[s] = true
		}

		for _, s := range known {
			if _, ok := lookup[s]; !ok {
				sentinels = append(sentinels, s)
			}
		}
	}

}
