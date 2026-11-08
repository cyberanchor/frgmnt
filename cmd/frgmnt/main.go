// Command frgmnt is the unified Fragment.com username checker: one binary,
// two modes.
//
//	frgmnt check [flags]   console checker (names.txt -> results.txt)
//	frgmnt web   [flags]   web panel (REST + SSE + embedded UI)
//
// For backwards compatibility, plain `frgmnt -input ...` still means
// "check", and web-only flags such as -addr/-data/-log-level route to the
// web panel automatically.
package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cyberanchor/frgmnt/internal/cli"
	"github.com/cyberanchor/frgmnt/internal/webapp"
)

// version is the unified binary version. Component versions are independent.
const version = "1.0.0"

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	if len(args) == 0 {
		usage(os.Stderr)
		return 2
	}

	switch args[0] {
	case "web", "serve", "server":
		webapp.ProgName = "frgmnt web"
		return webapp.Main(args[1:])
	case "check", "cli":
		cli.ProgName = "frgmnt check"
		return cli.Main(args[1:])
	case "version", "-version", "--version":
		fmt.Printf("frgmnt %s (cli %s, web %s)\n", version, cli.Version, webapp.Version)
		return 0
	case "help", "-h", "--help":
		usage(os.Stdout)
		return 0
	}

	// Legacy shorthand: web-only flags select the web panel; everything
	// else is passed to the console checker.
	if looksLikeWeb(args) {
		webapp.ProgName = "frgmnt web"
		return webapp.Main(args)
	}
	cli.ProgName = "frgmnt check"
	return cli.Main(args)
}

// looksLikeWeb reports whether args contain a flag that only the web panel
// understands. This keeps both `frgmnt web -addr ...` and the old
// `frgmnt -addr ...` working.
func looksLikeWeb(args []string) bool {
	for _, a := range args {
		name := a
		if i := strings.IndexByte(name, '='); i >= 0 {
			name = name[:i]
		}
		switch name {
		case "-addr", "--addr", "-data", "--data", "-log-level", "--log-level":
			return true
		}
	}
	return false
}

func usage(w io.Writer) {
	fmt.Fprintf(w, "frgmnt %s — Fragment.com username checker (console + web)\n\n", version)
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  frgmnt check [flags]   Console checker: names.txt -> results.txt")
	fmt.Fprintln(w, "  frgmnt web   [flags]   Web panel: REST + SSE + embedded UI")
	fmt.Fprintln(w, "  frgmnt -input ...      Legacy shorthand for \"check\"")
	fmt.Fprintln(w, "  frgmnt -addr ...       Shorthand for \"web\" (auto-detected)")
	fmt.Fprintln(w, "  frgmnt version         Print binary and component versions")
	fmt.Fprintln(w, "  frgmnt help            This text")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Examples:")
	fmt.Fprintln(w, "  frgmnt check -input names.txt -output results.txt -workers 200")
	fmt.Fprintln(w, "  frgmnt web -addr 127.0.0.1:8080 -data ./data")
	fmt.Fprintln(w, "  frgmnt web -no-cache")
	fmt.Fprintln(w, "  frgmnt -input names.txt -output results.txt")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Both modes use the same parser and page-cache implementation. Run")
	fmt.Fprintln(w, "  frgmnt check -h")
	fmt.Fprintln(w, "  frgmnt web -h")
	fmt.Fprintln(w, "for the full flag list of each mode.")
}
