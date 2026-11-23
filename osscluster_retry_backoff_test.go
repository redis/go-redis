package redis

import "testing"

// A node client must keep a disabled (-1) retry backoff instead of falling
// back to the 10ms..1s default of Options.init.
func TestClusterNodeRetryBackoffDisabled(t *testing.T) {
	c := NewClusterClient(&ClusterOptions{
		Addrs:           []string{"127.0.0.1:1"},
		MaxRetries:      3,
		MinRetryBackoff: -1,
		MaxRetryBackoff: -1,
	})
	defer c.Close()

	node, err := c.nodes.GetOrCreate("127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	opt := node.Client.Options()
	if opt.MinRetryBackoff != 0 || opt.MaxRetryBackoff != 0 {
		t.Errorf("node backoff = %v..%v, want 0..0", opt.MinRetryBackoff, opt.MaxRetryBackoff)
	}
	if d := node.Client.retryBackoff(1); d != 0 {
		t.Errorf("node retryBackoff(1) = %v, want 0", d)
	}
}

// The defaults still reach the node when the backoffs are left unset.
func TestClusterNodeRetryBackoffDefault(t *testing.T) {
	c := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	defer c.Close()

	node, err := c.nodes.GetOrCreate("127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	opt := node.Client.Options()
	if opt.MinRetryBackoff != c.opt.MinRetryBackoff || opt.MaxRetryBackoff != c.opt.MaxRetryBackoff {
		t.Errorf("node backoff = %v..%v, want %v..%v",
			opt.MinRetryBackoff, opt.MaxRetryBackoff, c.opt.MinRetryBackoff, c.opt.MaxRetryBackoff)
	}
}
