// Command fragment_checker_web runs only the web panel. The unified binary
// (cmd/frgmnt) exposes the same mode as `frgmnt web`.
package main

import (
	"os"

	"github.com/cyberanchor/frgmnt/internal/webapp"
)

func main() { os.Exit(webapp.Main(os.Args[1:])) }
