package redis

import (
	"context"
	"reflect"
	"testing"
)

func TestHelloAuthArgs(t *testing.T) {
	tests := []struct {
		name               string
		username, password string
		authEmptyPassword  bool
		want               []interface{}
	}{
		{"no credentials", "", "", false, []interface{}{"hello", 3}},
		{"no credentials opt-in", "", "", true, []interface{}{"hello", 3}},
		{"password only", "", "pw", false, []interface{}{"hello", 3, "auth", "default", "pw"}},
		{"username and password", "u", "pw", false, []interface{}{"hello", 3, "auth", "u", "pw"}},
		{"username empty password", "u", "", false, []interface{}{"hello", 3}},
		{"username empty password opt-in", "u", "", true, []interface{}{"hello", 3, "auth", "u", ""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []interface{}
			c := statefulCmdable(func(_ context.Context, cmd Cmder) error {
				got = cmd.Args()
				return nil
			})
			opt := &Options{AuthEmptyPassword: tt.authEmptyPassword}
			c.hello(context.Background(), 3, tt.username, tt.password, "", opt.sendAuth(tt.username, tt.password))
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("args = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestHelloPublicAuthOnlyWithPassword pins the public Hello behaviour: a
// username without a password sends no AUTH.
func TestHelloPublicAuthOnlyWithPassword(t *testing.T) {
	var got []interface{}
	c := statefulCmdable(func(_ context.Context, cmd Cmder) error {
		got = cmd.Args()
		return nil
	})
	c.Hello(context.Background(), 3, "u", "", "name")
	want := []interface{}{"hello", 3, "setname", "name"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %v, want %v", got, want)
	}
}

func TestAuthEmptyPasswordURL(t *testing.T) {
	const q = "?auth_empty_password=true"

	o, err := ParseURL("redis://u@localhost:6379" + q)
	if err != nil {
		t.Fatal(err)
	}
	if !o.AuthEmptyPassword {
		t.Fatal("ParseURL: AuthEmptyPassword not set")
	}

	co, err := ParseClusterURL("redis://u@localhost:6379" + q)
	if err != nil {
		t.Fatal(err)
	}
	if !co.AuthEmptyPassword {
		t.Fatal("ParseClusterURL: AuthEmptyPassword not set")
	}

	fo, err := ParseFailoverURL("redis://localhost:26379?master_name=m&auth_empty_password=true")
	if err != nil {
		t.Fatal(err)
	}
	if !fo.AuthEmptyPassword {
		t.Fatal("ParseFailoverURL: AuthEmptyPassword not set")
	}

	o, err = ParseURL("redis://u@localhost:6379")
	if err != nil {
		t.Fatal(err)
	}
	if o.AuthEmptyPassword {
		t.Fatal("ParseURL: AuthEmptyPassword set by default")
	}

	if _, err := ParseURL("redis://localhost:6379?auth_empty_password=maybe"); err == nil {
		t.Fatal("ParseURL: invalid bool accepted")
	}
}

func TestAuthEmptyPasswordPlumbing(t *testing.T) {
	uo := &UniversalOptions{Addrs: []string{"a:1"}, MasterName: "m", AuthEmptyPassword: true}
	if !uo.Simple().AuthEmptyPassword {
		t.Error("UniversalOptions.Simple")
	}
	if !uo.Cluster().AuthEmptyPassword {
		t.Error("UniversalOptions.Cluster")
	}
	if !uo.Failover().AuthEmptyPassword {
		t.Error("UniversalOptions.Failover")
	}

	co := &ClusterOptions{AuthEmptyPassword: true}
	co.init()
	if !co.clientOptions().AuthEmptyPassword {
		t.Error("ClusterOptions.clientOptions")
	}

	fo := &FailoverOptions{MasterName: "m", AuthEmptyPassword: true}
	if !fo.clientOptions().AuthEmptyPassword {
		t.Error("FailoverOptions.clientOptions")
	}
	if !fo.clusterOptions().AuthEmptyPassword {
		t.Error("FailoverOptions.clusterOptions")
	}
	// Sentinel and data-node credentials are independent, and so are their
	// opt-ins.
	if fo.sentinelOptions("a:1").AuthEmptyPassword {
		t.Error("FailoverOptions.AuthEmptyPassword reached the sentinel connections")
	}
	so := &FailoverOptions{MasterName: "m", SentinelAuthEmptyPassword: true}
	if !so.sentinelOptions("a:1").AuthEmptyPassword {
		t.Error("FailoverOptions.SentinelAuthEmptyPassword not on sentinelOptions")
	}
	if so.clientOptions().AuthEmptyPassword || so.clusterOptions().AuthEmptyPassword {
		t.Error("FailoverOptions.SentinelAuthEmptyPassword reached the data-node connections")
	}
	us := &UniversalOptions{Addrs: []string{"a:1"}, MasterName: "m", SentinelAuthEmptyPassword: true}
	if f := us.Failover(); !f.SentinelAuthEmptyPassword || f.AuthEmptyPassword {
		t.Error("UniversalOptions.Failover: SentinelAuthEmptyPassword not kept apart")
	}

	pf, err := ParseFailoverURL("redis://sentinel:26379?master_name=m&sentinel_auth_empty_password=true")
	if err != nil {
		t.Fatalf("ParseFailoverURL: %v", err)
	}
	if !pf.SentinelAuthEmptyPassword || pf.AuthEmptyPassword {
		t.Errorf("ParseFailoverURL: SentinelAuthEmptyPassword=%v AuthEmptyPassword=%v", pf.SentinelAuthEmptyPassword, pf.AuthEmptyPassword)
	}

	ro := &RingOptions{AuthEmptyPassword: true}
	if !ro.clientOptions().AuthEmptyPassword {
		t.Error("RingOptions.clientOptions")
	}
}

// TestAuthEmptyPasswordCSCNamespace checks that clients sharing a Cache do not
// share entries across ACL identities: a Username sends AUTH (its own
// namespace), or sends none and runs as the default user or a TLS-certificate
// user (a separate namespace per Username, never the default one).
func TestAuthEmptyPasswordCSCNamespace(t *testing.T) {
	tests := []struct {
		name string
		opt  Options
		want string
	}{
		{"no credentials", Options{}, cscNamespacePrefix(0, "")},
		{"username empty password", Options{Username: "u"}, cscNamespacePrefix(0, "noauth u")},
		{"other username empty password", Options{Username: "v"}, cscNamespacePrefix(0, "noauth v")},
		{"username empty password opt-in", Options{Username: "u", AuthEmptyPassword: true}, cscNamespacePrefix(0, "u")},
		{"username and password", Options{Username: "u", Password: "pw"}, cscNamespacePrefix(0, "u")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt := tt.opt
			opt.Addr = "127.0.0.1:1"
			opt.Protocol = 3
			opt.ClientSideCacheConfig = &ClientSideCacheConfig{}
			c := NewClient(&opt)
			defer c.Close()
			if c.csc == nil {
				t.Fatal("client-side cache not attached")
			}
			if c.cscKeyPrefix != tt.want {
				t.Fatalf("cscKeyPrefix = %q, want %q", c.cscKeyPrefix, tt.want)
			}
		})
	}
}

// TestAuthEmptyPasswordConnect checks the connection identity for a nopass
// ACL user given as Username with an empty Password.
func TestAuthEmptyPasswordConnect(t *testing.T) {
	ctx := context.Background()
	addr := internalTestRedisAddr()
	if err := probeRedis(addr); err != nil {
		t.Skipf("no redis: %v", err)
	}
	admin := NewClient(&Options{Addr: addr})
	// t.Cleanup, not defer: cleanups run LIFO after the test returns, so the
	// ACL user deletion registered below runs while admin is still open.
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if whoami, err := admin.ACLWhoAmI(ctx).Result(); err != nil || whoami != "default" {
		t.Skipf("default user unavailable: %q %v", whoami, err)
	}
	const user = "aep_user"
	if err := admin.ACLSetUser(ctx, user, "reset", "on", "nopass", "~*", "+@all").Err(); err != nil {
		t.Skipf("ACL SETUSER unsupported: %v", err)
	}
	t.Cleanup(func() {
		// Other ACL specs expect only the default user.
		if err := admin.ACLDelUser(context.Background(), user).Err(); err != nil {
			t.Errorf("ACL DELUSER %s: %v", user, err)
		}
	})

	credsProvider := func() (string, string) { return user, "" }

	for _, protocol := range []int{3, 2} {
		for _, tc := range []struct {
			name      string
			opt       Options
			wantWhoAm string
		}{
			{"username default", Options{Username: user}, "default"},
			{"username opt-in", Options{Username: user, AuthEmptyPassword: true}, user},
			{"provider default", Options{CredentialsProvider: credsProvider}, "default"},
			{"provider opt-in", Options{CredentialsProvider: credsProvider, AuthEmptyPassword: true}, user},
		} {
			opt := tc.opt
			opt.Addr = addr
			opt.Protocol = protocol
			c := NewClient(&opt)
			whoami, err := c.ACLWhoAmI(ctx).Result()
			_ = c.Close()
			if err != nil {
				t.Fatalf("protocol %d %s: %v", protocol, tc.name, err)
			}
			if whoami != tc.wantWhoAm {
				t.Fatalf("protocol %d %s: whoami = %q, want %q", protocol, tc.name, whoami, tc.wantWhoAm)
			}
		}
	}
}
