package redis_test

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestFailoverClientSubscribeBoundedByDialTimeout checks that the master dial
// uses the default DialTimeout when FailoverOptions.DialTimeout is unset. The
// pub/sub pool passes the caller ctx to the dialer as is, so an unbounded dial
// made Subscribe hang. The fake master accepts TCP but never completes the TLS
// handshake, which stands in for an unreachable host without network tricks.
func TestFailoverClientSubscribeBoundedByDialTimeout(t *testing.T) {
	cert := selfSignedCert(t)

	master, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	stalled := make(chan net.Conn, 16)
	defer func() {
		master.Close()
		for {
			select {
			case conn := <-stalled:
				conn.Close()
			default:
				return
			}
		}
	}()
	go func() {
		for {
			conn, err := master.Accept()
			if err != nil {
				return
			}
			stalled <- conn
		}
	}()
	masterHost, masterPort, _ := net.SplitHostPort(master.Addr().String())

	sentinel, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer sentinel.Close()
	go func() {
		for {
			conn, err := sentinel.Accept()
			if err != nil {
				return
			}
			go serveFakeSentinel(conn, masterHost, masterPort)
		}
	}()

	client := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: []string{sentinel.Addr().String()},
		TLSConfig:     &tls.Config{InsecureSkipVerify: true},
	})
	defer client.Close()

	dialTimeout := client.Options().DialTimeout
	errc := make(chan error, 1)
	start := time.Now()
	go func() {
		pubsub := client.Subscribe(context.Background())
		defer pubsub.Close()
		errc <- pubsub.Subscribe(context.Background(), "ch")
	}()

	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("expected a dial error, got nil")
		}
		if elapsed := time.Since(start); elapsed > dialTimeout+time.Second {
			t.Fatalf("Subscribe took %s, want about %s", elapsed, dialTimeout)
		}
	case <-time.After(2 * dialTimeout):
		t.Fatalf("Subscribe did not return within %s", 2*dialTimeout)
	}
}

func serveFakeSentinel(conn net.Conn, masterHost, masterPort string) {
	defer conn.Close()
	rd := bufio.NewReader(conn)
	for {
		args, err := readCommand(rd)
		if err != nil {
			return
		}
		var reply string
		switch {
		case len(args) == 3 && strings.EqualFold(args[0], "sentinel") &&
			strings.EqualFold(args[1], "get-master-addr-by-name"):
			reply = "*2\r\n$" + strconv.Itoa(len(masterHost)) + "\r\n" + masterHost + "\r\n$" +
				strconv.Itoa(len(masterPort)) + "\r\n" + masterPort + "\r\n"
		case strings.EqualFold(args[0], "psubscribe"):
			// Leave the sentinel pub/sub connection idle.
			continue
		default:
			reply = "-ERR unknown command\r\n"
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
	}
}

func selfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
