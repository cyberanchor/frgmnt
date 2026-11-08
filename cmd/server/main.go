// Command fragment_checker_web runs only the web panel. The unified binary
// (cmd/frgmnt) exposes the same mode as `frgmnt web`; "fragment_checker_web
// token ..." issues an access token like "frgmnt token".
package main

import (
	"os"

	"github.com/cyberanchor/frgmnt/internal/webapp"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "token" {
		os.Exit(webapp.TokenMain(os.Args[2:]))
	}
	os.Exit(webapp.Main(os.Args[1:]))
}
