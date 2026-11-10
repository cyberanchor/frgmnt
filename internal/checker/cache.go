package checker

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Page-cache policy defaults. Only statuses that are expensive to re-check
// (and reasonably stable) are cached:
//
//   - sold: purchased on Fragment; minutes-long rechecks are pointless.
//   - taken: owned on Telegram; stable until the owner lists it.
//   - reserved: stable by definition.
//   - available / auction: cached briefly so retries and resumed runs do
//     not hammer Fragment, but short enough to notice an auction ending.
//
// free/unknown/error pages have no useful body and are never cached.
const (
	defaultVolatileTTL = 2 * time.Minute
	defaultSoldTTL     = 7 * 24 * time.Hour
	defaultTakenTTL    = 6 * time.Hour
	defaultReservedTTL = 24 * time.Hour
)

// cacheEntry is the sidecar metadata for one cached HTML page.
type cacheEntry struct {
	Username   string    `json:"username"`
	URL        string    `json:"url"`
	Status     Status    `json:"status"`
	Confidence int       `json:"confidence"`
	Evidence   string    `json:"evidence"`
	FetchedAt  time.Time `json:"fetched_at"`
	Bytes      int       `json:"bytes"`
}

// pageCache stores the (gzipped) HTML of Fragment username pages on disk.
// Keeping the raw body — rather than only the verdict — means a parser
// improvement can be applied to already-cached names without new requests.
type pageCache struct {
	dir         string
	volatileTTL time.Duration
	soldTTL     time.Duration
	takenTTL    time.Duration
	reservedTTL time.Duration
}

// newPageCache returns nil when the directory is empty (cache disabled).
// volatileTTL / soldTTL semantics:
//
//	0  -> use the defaults
//	<0 -> volatile: never cache; sold: never expire
func newPageCache(dir string, volatileTTL, soldTTL time.Duration) *pageCache {
	if strings.TrimSpace(dir) == "" {
		return nil
	}
	if volatileTTL == 0 {
		volatileTTL = defaultVolatileTTL
	}
	if soldTTL == 0 {
		soldTTL = defaultSoldTTL
	}
	return &pageCache{
		dir:         dir,
		volatileTTL: volatileTTL,
		soldTTL:     soldTTL,
		takenTTL:    defaultTakenTTL,
		reservedTTL: defaultReservedTTL,
	}
}

func (c *pageCache) paths(name string) (htmlPath, metaPath string) {
	prefix := "_"
	if len(name) >= 2 {
		prefix = name[:2]
	}
	base := filepath.Join(c.dir, prefix)
	return filepath.Join(base, name+".html.gz"), filepath.Join(base, name+".json")
}

// get returns the cached metadata and raw HTML body, if both are readable.
// A name that is not in the cache is a miss (ok false, err nil); err tells
// an entry that is there but cannot be used (a broken file, a permission),
// which is a miss too but used to be silent.
func (c *pageCache) get(name string) (ent *cacheEntry, body []byte, ok bool, err error) {
	htmlPath, metaPath := c.paths(name)

	metaRaw, err := os.ReadFile(metaPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, false, nil
		}
		return nil, nil, false, err
	}
	ent = new(cacheEntry)
	if err := json.Unmarshal(metaRaw, ent); err != nil {
		return nil, nil, false, fmt.Errorf("%s: %w", filepath.Base(metaPath), err)
	}

	f, err := os.Open(htmlPath)
	if err != nil {
		return nil, nil, false, err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, nil, false, fmt.Errorf("%s: %w", filepath.Base(htmlPath), err)
	}
	defer gz.Close()

	body, err = io.ReadAll(io.LimitReader(gz, 256<<10))
	if err != nil {
		return nil, nil, false, fmt.Errorf("%s: %w", filepath.Base(htmlPath), err)
	}
	if len(body) == 0 {
		return nil, nil, false, fmt.Errorf("%s: empty page", filepath.Base(htmlPath))
	}
	return ent, body, true, nil
}

// put writes the body and metadata atomically. Errors are deliberately
// ignored by the caller: a read-only cache directory must not fail checks.
func (c *pageCache) put(name, pageURL string, info ParseInfo, body []byte) error {
	if !cacheableStatus(info.Status) || len(body) == 0 {
		return nil
	}
	if (info.Status == StatusAuction || info.Status == StatusAvailable) && c.volatileTTL < 0 {
		return nil
	}
	htmlPath, metaPath := c.paths(name)
	dir := filepath.Dir(htmlPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	if err := writeGzipAtomic(htmlPath, body); err != nil {
		return err
	}

	ent := cacheEntry{
		Username:   name,
		URL:        pageURL,
		Status:     info.Status,
		Confidence: info.Confidence,
		Evidence:   info.Evidence,
		FetchedAt:  time.Now().UTC(),
		Bytes:      len(body),
	}
	meta, err := json.Marshal(ent)
	if err != nil {
		return err
	}
	return writeFileAtomic(metaPath, meta)
}

func cacheableStatus(st Status) bool {
	switch st {
	case StatusSold, StatusTaken, StatusReserved, StatusAvailable, StatusAuction:
		return true
	default:
		return false
	}
}

// fresh reports whether a cached verdict of status st may be reused.
func (c *pageCache) fresh(st Status, fetchedAt time.Time) bool {
	age := time.Since(fetchedAt)
	if age < 0 {
		age = 0
	}
	switch st {
	case StatusSold:
		if c.soldTTL < 0 {
			return true
		}
		return age < c.soldTTL
	case StatusTaken:
		return age < c.takenTTL
	case StatusReserved:
		return age < c.reservedTTL
	case StatusAuction, StatusAvailable:
		if c.volatileTTL < 0 {
			return false
		}
		return age < c.volatileTTL
	default:
		return false
	}
}

func writeGzipAtomic(path string, body []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	gz := gzip.NewWriter(tmp)
	if _, err := gz.Write(body); err != nil {
		tmp.Close()
		return err
	}
	if err := gz.Close(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func writeFileAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
