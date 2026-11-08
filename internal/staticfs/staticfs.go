// Package staticfs embeds the web/ asset bundle and exposes it as an
// io/fs.FS. Keeping the embed directive next to the assets avoids the
// `pattern web: no matching files found` error that occurs when the
// directive is declared in cmd/server/ but the files live elsewhere.
package staticfs

import (
	"embed"
	"io/fs"
)

//go:embed all:web
var FS embed.FS

// Sub returns the sub-FS rooted at "web" so callers can serve files
// without the "web/" prefix.
func Sub() (fs.FS, error) {
	return fs.Sub(FS, "web")
}
