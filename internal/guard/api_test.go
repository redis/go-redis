package guard

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for rel, src := range files {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func apiLines(t *testing.T, root string) []string {
	t.Helper()
	layers, err := Extract(root)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range layers["11_api"] {
		out = append(out, e.Line())
	}
	return out
}

func TestAPILayer(t *testing.T) {
	root := writeTree(t, map[string]string{
		"a.go": `package redis

import "context"

type Hook func(ctx context.Context, name string) error

type Options struct {
	Addr   string
	Dialer func(ctx context.Context, network, addr string) (net.Conn, error)
	hidden int
	Embedded
}

type Cmdable interface {
	Get(ctx context.Context, key string) *StringCmd
	StringCmdable
}

type Pair[K comparable, V any] struct{ Key K }

type Alias = Options

type cmdable func(ctx context.Context, cmd Cmder) error

func (c cmdable) Get(ctx context.Context, key string) *StringCmd { return nil }
func (c *Client) Do(ctx context.Context, args ...interface{}) *Cmd  { return nil }
func (c *Client) private()                                          {}
func NewClient(opt *Options) *Client                                { return nil }
func Map[T any](in []T, f func(v T) T) (out []T, err error)          { return nil, nil }
func unexported()                                                   {}

var Nil = proto.Nil
var Version string
var hidden = 1
`,
		"a_test.go":        "package redis\n\nfunc TestX(t *testing.T) {}\nfunc Exported() {}\n",
		"internal/x/x.go":  "package x\n\nfunc Exported() {}\n",
		"cmd/tool/main.go": "package main\n\nfunc Exported() {}\n",
	})
	want := []string{
		". | embed Cmdable.StringCmdable | StringCmdable",
		". | field Options.Addr | string",
		". | field Options.Dialer | func(context.Context, string, string) (net.Conn, error)",
		". | field Options.Embedded | embedded Embedded",
		". | field Pair.Key | K",
		". | func Map | func[any]([]T, func(T) T) ([]T, error)",
		". | func NewClient | func(*Options) *Client",
		". | method *Client.Do | func(context.Context, ...interface{}) *Cmd",
		". | method Cmdable.Get | func(context.Context, string) *StringCmd",
		". | type Alias | = Options",
		". | type Cmdable | interface",
		". | type Hook | func(context.Context, string) error",
		". | type Options | struct",
		". | type Pair | [comparable, any] struct",
		". | var Nil | = proto.Nil",
		". | var Version | string",
	}
	if got := apiLines(t, root); !reflect.DeepEqual(got, want) {
		t.Fatalf("api entries:\n got %q\nwant %q", got, want)
	}
}

func TestAPIParamRenameIsNoChange(t *testing.T) {
	a := writeTree(t, map[string]string{"a.go": "package p\n\nfunc F(a, b int, s string) (n int, err error) { return }\n"})
	b := writeTree(t, map[string]string{"a.go": "package p\n\nfunc F(x, y int, z string) (int, error) { return 0, nil }\n"})
	if ga, gb := apiLines(t, a), apiLines(t, b); !reflect.DeepEqual(ga, gb) {
		t.Fatalf("rename changed the API entry: %q vs %q", ga, gb)
	}
}

func TestSourceLayersSkipGuardAndSkipDirs(t *testing.T) {
	root := writeTree(t, map[string]string{
		"internal/guard/g.go":   "package guard\n\nconst X = 1\n",
		"internal/guard/c/m.go": "package main\n\nconst Y = 1\n",
		"example/e/e.go":        "package e\n\nconst Z = 1\n",
		"a.go":                  "package redis\n\nconst A = 1\n",
	})
	layers, err := Extract(root)
	if err != nil {
		t.Fatal(err)
	}
	got := layers["01_constants"]
	if len(got) != 1 || got[0].Entity != "A" {
		t.Fatalf("constants = %v, want only A", got)
	}
}

func TestExtractFailsOnParseError(t *testing.T) {
	root := writeTree(t, map[string]string{"a.go": "package redis\n\nfunc {\n"})
	if _, err := Extract(root); err == nil {
		t.Fatal("parse error was ignored")
	}
}

func TestAPIPromotedMethods(t *testing.T) {
	root := writeTree(t, map[string]string{"a.go": `package redis

type cmdable func() error
type hooksMixin struct{ inner }
type inner struct{}
type unused struct{}

type Client struct {
	cmdable
	*hooksMixin
}

func (c cmdable) Get(key string) string { return "" }
func (h *hooksMixin) AddHook(x int)     {}
func (i inner) Deep()                   {}
func (u unused) Hidden()                {}
`})
	want := []string{
		". | method *hooksMixin.AddHook | func(int)",
		". | method cmdable.Get | func(string) string",
		". | method inner.Deep | func()",
		". | type Client | struct",
	}
	if got := apiLines(t, root); !reflect.DeepEqual(got, want) {
		t.Fatalf("api entries:\n got %q\nwant %q", got, want)
	}
}
