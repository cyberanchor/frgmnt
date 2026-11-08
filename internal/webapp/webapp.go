// Binary fragment_checker_web is a single-binary Fragment.com username
// checker with an embedded web panel. It bundles a SQLite store, a small
// REST API, Server-Sent Events for live progress, and a static frontend.
//
// Usage:
//
//	fragment_checker_web                                  # default: :8080, ./data
//	fragment_checker_web -addr 127.0.0.1:9090 -data ./var
//	fragment_checker_web -log-level debug
//
// All web traffic is plain HTTP and binds to 127.0.0.1 by default. Override
// with -addr to expose it on a LAN interface — at your own risk.
package webapp

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cyberanchor/frgmnt/internal/auth"
	"github.com/cyberanchor/frgmnt/internal/checker"
	"github.com/cyberanchor/frgmnt/internal/logbus"
	"github.com/cyberanchor/frgmnt/internal/server"
	"github.com/cyberanchor/frgmnt/internal/staticfs"
	"github.com/cyberanchor/frgmnt/internal/store"
	"github.com/cyberanchor/frgmnt/internal/worker"
)

// version is the human-readable release tag.
const Version = "0.3.0"

// ProgName is used in logs and -version; the unified binary sets it to
// "frgmnt web".
var ProgName = "fragment_checker_web"

func Main(args []string) int {
	fs := flag.NewFlagSet(ProgName, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var (
		addr         = fs.String("addr", "127.0.0.1:8080", "HTTP listen address")
		dataDir      = fs.String("data", "./data", "directory for SQLite DB and settings.json")
		logLevel     = fs.String("log-level", "info", "log level: error, warn, info, debug, trace")
		cacheDir     = fs.String("cache-dir", "", "page cache directory (default <data>/pagecache)")
		cacheTTL     = fs.Duration("cache-ttl", 2*time.Minute, "TTL for volatile pages (auction/available)")
		cacheSoldTTL = fs.Duration("cache-sold-ttl", 7*24*time.Hour, "TTL for sold/bought pages; negative = never expire")
		noCache      = fs.Bool("no-cache", false, "disable the on-disk page cache")
		baseURL      = fs.String("base-url", "", "Fragment base URL to check against (tests, mirrors); default https://fragment.com")
		allowHosts   = fs.String("allow-host", "", "comma-separated host names the panel answers to besides localhost, e.g. the domain of a reverse proxy")
		tlsCert      = fs.String("tls-cert", "", "PEM certificate (full chain) to serve HTTPS; reloaded when the file changes")
		tlsKey       = fs.String("tls-key", "", "PEM private key for -tls-cert")
		showVer      = fs.Bool("version", false, "print version and exit")
	)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *showVer {
		fmt.Println(ProgName, Version)
		return 0
	}

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if (*tlsCert == "") != (*tlsKey == "") {
		fmt.Fprintln(os.Stderr, "-tls-cert and -tls-key go together")
		return 2
	}
	if *baseURL != "" {
		// Same as the console's -base-url: the web mode could not be
		// pointed at a test server or mirror without a code change.
		checker.FragmentURLTemplate = strings.TrimRight(*baseURL, "/") + "/username/%s"
	}

	absData, err := filepath.Abs(*dataDir)
	if err != nil {
		log.Fatalf("resolve data dir: %v", err)
	}

	level := logbus.ParseLevel(*logLevel)
	bus := logbus.New(2000, level)
	defer bus.Close()

	// Mirror every published entry to stdout so the operator can tail it.
	go mirrorBusToStdout(bus)

	st, err := store.Open(absData)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	// CLI -log-level wins at boot. The persisted setting only changes
	// the level at runtime (via PUT /api/settings) so the operator can
	// always recover a chatty/quiet boot regardless of stored state.

	mgr := worker.New(st, bus)
	mgr.ConfigureCache(*cacheDir, *cacheTTL, *cacheSoldTTL, *noCache)
	if err := mgr.Load(context.Background()); err != nil {
		log.Fatalf("load workers: %v", err)
	}

	keeper, created, err := auth.Open(absData)
	if err != nil {
		log.Fatalf("access tokens: %v", err)
	}

	staticSub, err := staticfs.Sub()
	if err != nil {
		log.Fatalf("embed sub-fs: %v", err)
	}
	srv := server.New(server.Options{
		Addr:       *addr,
		AllowHosts: splitHosts(*allowHosts),
		Manager:    mgr,
		Store:      st,
		Bus:        bus,
		StaticFS:   staticSub,
		Auth:       keeper,
		TLSCert:    *tlsCert,
		TLSKey:     *tlsKey,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigCh
		// A second Ctrl+C quits at once instead of being ignored.
		signal.Reset(syscall.SIGINT, syscall.SIGTERM)
		bus.Publish(logbus.LevelInfo, "", "received "+s.String()+", shutting down…")
		// CloseAll waits until every run has saved its last results, so
		// the deferred st.Close() cannot cut them off.
		mgr.CloseAll()
		cancel()
	}()

	bus.Publish(logbus.LevelInfo, "", ProgName+" v"+Version+" | data="+absData+" | log="+bus.Level().String())
	if *baseURL != "" {
		bus.Publish(logbus.LevelWarn, "", "checking against "+*baseURL+" instead of fragment.com (-base-url)")
	}
	keyNote := "access tokens are signed with " + keeper.Path()
	if created {
		keyNote = "a new access key was created in " + keeper.Path()
	}
	bus.Publish(logbus.LevelInfo, "", keyNote+"; issue a token with: frgmnt token -data "+absData)
	if err := srv.Run(ctx); err != nil {
		log.Fatalf("server error: %v", err)
	}
	bus.Publish(logbus.LevelInfo, "", "server stopped cleanly")
	return 0
}

// splitHosts turns "-allow-host a.example, B.example:443" into
// [a.example b.example]: names only, the port does not matter.
func splitHosts(raw string) []string {
	var out []string
	for _, h := range strings.Split(raw, ",") {
		h = strings.ToLower(strings.TrimSpace(h))
		if host, _, err := net.SplitHostPort(h); err == nil {
			h = host
		}
		if h = strings.Trim(h, "[]"); h != "" {
			out = append(out, h)
		}
	}
	return out
}

// mirrorBusToStdout subscribes to every bus event and prints it to stderr
// for operator visibility. The subscription is unbounded (no cancel) on
// purpose — the bus is closed at shutdown which closes our channel.
func mirrorBusToStdout(bus *logbus.Bus) {
	ch, _ := bus.Subscribe("", 1024)
	for e := range ch {
		topic := ""
		if e.Topic != "" {
			topic = " [" + e.Topic[:min(8, len(e.Topic))] + "]"
		}
		fmt.Fprintf(os.Stderr, "%s %s%s %s\n",
			e.Time.Format("15:04:05.000"), e.Level, topic, e.Message)
	}
}
