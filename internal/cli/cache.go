package cli

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Page-cache policy defaults. Sold (bought) pages are the main use case:
// once a username is purchased its page rarely changes, so re-checking it on
// every run wastes requests. Volatile verdicts get a short TTL.
const (
	defaultCacheTTL     = 2 * time.Minute
	defaultCacheSoldTTL = 7 * 24 * time.Hour
	defaultCacheTaken   = 6 * time.Hour
	defaultCacheReservd = 24 * time.Hour
)

type cacheEntry struct {
	Username   string    `json:"username"`
	URL        string    `json:"url"`
	Status     Status    `json:"status"`
	Confidence int       `json:"confidence"`
	Evidence   string    `json:"evidence"`
	FetchedAt  time.Time `json:"fetched_at"`
	Bytes      int       `json:"bytes"`
}

// pageCache stores gzipped Fragment HTML on disk. Raw bodies (not just the
// verdict) are kept so parser changes apply to already-cached names.
type pageCache struct {
	dir         string
	volatileTTL time.Duration
	soldTTL     time.Duration
	takenTTL    time.Duration
	reservedTTL time.Duration
}

// newPageCache returns nil when dir is empty (cache disabled).
//
//	volatileTTL: 0 = default, <0 = do not cache available/auction.
//	soldTTL:     0 = default, <0 = never expire sold pages.
func newPageCache(dir string, volatileTTL, soldTTL time.Duration) *pageCache {
	if strings.TrimSpace(dir) == "" {
		return nil
	}
	if volatileTTL == 0 {
		volatileTTL = defaultCacheTTL
	}
	if soldTTL == 0 {
		soldTTL = defaultCacheSoldTTL
	}
	return &pageCache{
		dir:         dir,
		volatileTTL: volatileTTL,
		soldTTL:     soldTTL,
		takenTTL:    defaultCacheTaken,
		reservedTTL: defaultCacheReservd,
	}
}

func (c *pageCache) paths(name string) (string, string) {
	prefix := "_"
	if len(name) >= 2 {
		prefix = name[:2]
	}
	base := filepath.Join(c.dir, prefix)
	return filepath.Join(base, name+".html.gz"), filepath.Join(base, name+".json")
}

func (c *pageCache) get(name string) (*cacheEntry, []byte, bool) {
	htmlPath, metaPath := c.paths(name)

	metaRaw, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, nil, false
	}
	var ent cacheEntry
	if err := json.Unmarshal(metaRaw, &ent); err != nil {
		return nil, nil, false
	}

	f, err := os.Open(htmlPath)
	if err != nil {
		return nil, nil, false
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, nil, false
	}
	defer gz.Close()

	body, err := io.ReadAll(io.LimitReader(gz, 256<<10))
	if err != nil || len(body) == 0 {
		return nil, nil, false
	}
	return &ent, body, true
}

func (c *pageCache) put(name, pageURL string, st Status, conf int, evidence string, body []byte) {
	if !cacheableStatus(st) || len(body) == 0 {
		return
	}
	if (st == StatusAuction || st == StatusAvailable) && c.volatileTTL < 0 {
		return
	}
	htmlPath, metaPath := c.paths(name)
	dir := filepath.Dir(htmlPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	if err := writeGzipAtomic(htmlPath, body); err != nil {
		return
	}
	meta, err := json.Marshal(cacheEntry{
		Username:   name,
		URL:        pageURL,
		Status:     st,
		Confidence: conf,
		Evidence:   evidence,
		FetchedAt:  time.Now().UTC(),
		Bytes:      len(body),
	})
	if err != nil {
		return
	}
	_ = writeFileAtomic(metaPath, meta)
}

func cacheableStatus(st Status) bool {
	switch st {
	case StatusSold, StatusTaken, StatusReserved, StatusAvailable, StatusAuction:
		return true
	default:
		return false
	}
}

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
