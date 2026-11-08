BIN     := fragment_checker_web
UNIFIED := frgmnt
PKG     := ./cmd/server
UPKG    := ./cmd/frgmnt

GO   := go
LDFLAGS := -s -w

.PHONY: unified build release run vet fmt tidy test test-suite test-race clean clean-cache

unified:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(UNIFIED) $(UPKG)

build:
	$(GO) build -o $(BIN) $(PKG)

release: unified
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN) $(PKG)

run: unified
	./$(UNIFIED) web

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

test-suite:
	$(GO) test ./tests/... -v

test-race:
	$(GO) test -race -count=1 ./...

fmt:
	$(GO) fmt ./...

tidy:
	$(GO) mod tidy

# clean removes only what the build makes. data/ holds the web panel's
# database (workers, lists, results) and is never deleted here.
clean:
	rm -f $(BIN) $(UNIFIED)

# clean-cache drops the page caches of both modes; they are rebuilt from
# Fragment on the next checks.
clean-cache:
	rm -rf .fragment_cache data/pagecache
