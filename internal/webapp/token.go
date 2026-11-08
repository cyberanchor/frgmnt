package webapp

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cyberanchor/frgmnt/internal/auth"
)

// TokenMain is "frgmnt token": it issues an access token for the web panel
// of a data directory and prints it on stdout (alone, for scripts). The
// token goes into the login page of the panel, or into the header
// "Authorization: Bearer <token>" of API calls.
func TokenMain(args []string) int {
	fs := flag.NewFlagSet(ProgName+" token", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var (
		dataDir = fs.String("data", "./data", "data directory of the panel (the key is <data>/"+auth.KeyFile+")")
		ttl     = fs.Duration("ttl", 30*24*time.Hour, "how long the token is valid; 0 = it never expires")
		name    = fs.String("name", "", "a label for the token, shown in the panel's log on login")
		rotate  = fs.Bool("rotate", false, "make a new key first: every token issued before stops working")
	)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s token [-data DIR] [-ttl 720h] [-name LABEL] [-rotate]\n\n", ProgName)
		fmt.Fprintln(os.Stderr, "Issues an access token for the web panel and prints it on stdout.")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *ttl < 0 {
		fmt.Fprintln(os.Stderr, "token: -ttl must not be negative")
		return 2
	}
	absData, err := filepath.Abs(*dataDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "token:", err)
		return 1
	}
	keeper, created, err := auth.Open(absData)
	if err != nil {
		fmt.Fprintln(os.Stderr, "token:", err)
		return 1
	}
	if *rotate && !created {
		if err := keeper.Rotate(); err != nil {
			fmt.Fprintln(os.Stderr, "token:", err)
			return 1
		}
		fmt.Fprintln(os.Stderr, "new key in "+keeper.Path()+": the tokens issued before no longer work")
	}
	tok, claims, err := keeper.Issue(*name, *ttl)
	if err != nil {
		fmt.Fprintln(os.Stderr, "token:", err)
		return 1
	}
	expires := "never expires"
	if exp := claims.ExpiresAt(); !exp.IsZero() {
		expires = "valid until " + exp.Format("2006-01-02 15:04 MST")
	}
	fmt.Fprintf(os.Stderr, "access token for the panel in %s, %s.\n"+
		"Paste it on the login page, or send \"Authorization: Bearer <token>\" with API calls.\n", absData, expires)
	fmt.Println(tok)
	return 0
}
