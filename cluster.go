package redis

import (
	"errors"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const HashSlots = 16384

//------------------------------------------------------------------------------

type clusterPool struct {
	addrs []string
	slots [][]string
	conns map[string]pool
	opts  *ClusterOptions

	_forceReload uint32
	sync.RWMutex
}

func newClusterPool(opts *ClusterOptions) (*clusterPool, error) {
	addrs, err := opts.ensureAddrs()
	if err != nil {
		return nil, err
	}

	p := &clusterPool{addrs: addrs, opts: opts, _forceReload: 1}
	p.reset()
	return p, nil
}

// Closes the pool
func (p *clusterPool) Close() (err error) {
	p.Lock()
	defer p.Unlock()

	p.reset()
	return nil
}

// Closes all connections and reloads slot cache
func (p *clusterPool) Reload() (err error) {
	var infos []ClusterSlotInfo

	p.Lock()
	defer p.Unlock()

	for _, addr := range p.addrs {
		p.reset()

		if infos, err = p.fetch(addr); err == nil {
			p.update(infos)
			break
		}
	}
	return
}

// Finds the current master address for a hash slot
func (p *clusterPool) master(hashSlot int) string {
	if addrs := p.slots[hashSlot]; len(addrs) > 0 {
		return addrs[0]
	}
	return ""
}

// Find the next unseen address
func (p *clusterPool) next(seen map[string]struct{}) string {
	for _, addr := range p.addrs {
		if _, ok := seen[addr]; !ok {
			return addr
		}
	}
	return ""
}

// Fetch slot information
func (p *clusterPool) fetch(addr string) ([]ClusterSlotInfo, error) {
	client := NewTCPClient(p.opts.opts(addr))
	defer client.Close()

	return client.ClusterSlots().Result()
}

// Update slot information
func (p *clusterPool) update(infos []ClusterSlotInfo) {

	// Create a map of known nodes
	known := make(map[string]struct{}, len(p.addrs))
	for _, addr := range p.addrs {
		known[addr] = struct{}{}
	}

	// Populate slots, store unknown nodes
	for _, info := range infos {
		for i := info.Min; i <= info.Max; i++ {
			p.slots[i] = info.Addrs
		}

		for _, addr := range info.Addrs {
			if _, ok := known[addr]; !ok {
				p.addrs = append(p.addrs, addr)
				known[addr] = struct{}{}
			}
		}
	}

	// Shuffle addresses
	for i := range p.addrs {
		j := rand.Intn(i + 1)
		p.addrs[i], p.addrs[j] = p.addrs[j], p.addrs[i]
	}
}

// Closes all connections and flushes slots cache
func (p *clusterPool) reset() {
	for _, conn := range p.conns {
		conn.Close()
	}
	p.conns = make(map[string]pool, 10)
	p.slots = make([][]string, HashSlots)
}

// Is a cache reload due
func (p *clusterPool) reloadDue() bool {
	return atomic.CompareAndSwapUint32(&p._forceReload, 1, 0)
}

// Forces a cache reload on next request
func (p *clusterPool) forceReload() {
	atomic.StoreUint32(&p._forceReload, 1)
}

//------------------------------------------------------------------------------

var errNoAddrs = errors.New("redis: no addresses")

type ClusterOptions struct {
	// A seed-list of host:port addresses of known cluster nodes
	Addrs []string

	// An optional password
	Password string

	// The maximum number of total TCP connections to all
	// cluster nodes. Default: 60
	PoolSize int

	// Timeout settings
	DialTimeout, ReadTimeout, WriteTimeout, IdleTimeout time.Duration
}

func (opt *ClusterOptions) getPoolSize() int {
	if opt.PoolSize == 0 {
		return 60
	}
	return opt.PoolSize
}

func (opt *ClusterOptions) getDialTimeout() time.Duration {
	if opt.DialTimeout == 0 {
		return 5 * time.Second
	}
	return opt.DialTimeout
}

func (opt *ClusterOptions) ensureAddrs() ([]string, error) {
	if len(opt.Addrs) < 1 {
		return nil, errNoAddrs
	}
	return opt.Addrs, nil
}

func (opt *ClusterOptions) opts(addr string) *Options {
	return &Options{
		Addr:     addr,
		DB:       0,
		Password: opt.Password,

		DialTimeout:  opt.getDialTimeout(),
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,

		PoolSize:    opt.getPoolSize(),
		IdleTimeout: opt.IdleTimeout,
	}
}

//------------------------------------------------------------------------------

type ClusterSlotInfo struct {
	Min, Max int
	Addrs    []string
}

//------------------------------------------------------------------------------

// HashSlot returns a consistent slot number between 0 and 16383
// for any given string key
func HashSlot(key string) int {
	if s := strings.IndexByte(key, '{'); s > -1 {
		if e := strings.IndexByte(key[s+1:], '}'); e > 0 {
			key = key[s+1 : s+e+1]
		}
	}
	if key == "" {
		return rand.Intn(HashSlots)
	}
	return int(crc16sum(key)) % HashSlots
}

// CRC16 implementation according to CCITT standards.
// Copyright 2001-2010 Georges Menie (www.menie.org)
// Copyright 2013 The Go Authors. All rights reserved.
var crc16tab = [256]uint16{
	0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
	0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
	0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
	0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
	0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
	0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
	0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
	0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
	0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
	0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
	0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
	0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
	0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
	0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
	0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
	0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
	0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
	0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
	0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
	0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
	0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
	0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
	0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
	0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
	0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
	0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
	0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
	0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
	0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
	0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
	0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
	0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
}

func crc16sum(key string) (crc uint16) {
	for _, v := range key {
		crc = (crc << 8) ^ crc16tab[(byte(crc>>8)^byte(v))&0x00ff]
	}
	return
}
