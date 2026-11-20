// Command guard extracts guard entries from a source tree and compares two
// extractions. It parses files only; it never builds or runs the scanned
// code. Usually run through scripts/guard-delta.sh.
//
//	guard dump <root> <out-dir>
//	guard delta [-base-label s] [-head-label s] <base-dump> <head-dump>
//
// delta exits 0 when nothing needs approval, 1 when something does, and 2
// on any error.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9/internal/guard"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "dump":
		if len(os.Args) != 4 {
			usage()
		}
		layers, err := guard.Extract(os.Args[2])
		if err != nil {
			fail(err)
		}
		if err := guard.WriteDump(os.Args[3], layers); err != nil {
			fail(err)
		}
	case "delta":
		fs := flag.NewFlagSet("delta", flag.ExitOnError)
		baseLabel := fs.String("base-label", "base", "name of the base tree in the report")
		headLabel := fs.String("head-label", "head", "name of the head tree in the report")
		_ = fs.Parse(os.Args[2:])
		if fs.NArg() != 2 {
			usage()
		}
		base, err := guard.ReadDump(fs.Arg(0))
		if err != nil {
			fail(err)
		}
		head, err := guard.ReadDump(fs.Arg(1))
		if err != nil {
			fail(err)
		}
		changes := guard.Delta(base, head)
		if err := guard.WriteReport(os.Stdout, *baseLabel, *headLabel, changes); err != nil {
			fail(err)
		}
		if len(guard.Gated(changes)) > 0 {
			os.Exit(1)
		}
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: guard dump <root> <out-dir>")
	fmt.Fprintln(os.Stderr, "       guard delta [-base-label s] [-head-label s] <base-dump> <head-dump>")
	os.Exit(2)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "guard:", err)
	os.Exit(2)
}
