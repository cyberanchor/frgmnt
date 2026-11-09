package checker

import (
	"math/rand"
	"strings"
	"testing"
)

const nameAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789_"

// Every name of 1 to 3 characters packs to its own key, so names of
// different lengths cannot collide either ("a" and "aa", "_" and "a_").
func TestPackNameIsInjectiveForShortNames(t *testing.T) {
	seen := make(map[uint64]string, 60000)
	var walk func(prefix string)
	walk = func(prefix string) {
		if prefix != "" {
			k, ok := packName(prefix)
			if !ok {
				t.Fatalf("packName(%q) refused a name from the alphabet", prefix)
			}
			if other, dup := seen[k]; dup {
				t.Fatalf("packName(%q) == packName(%q) == %d", prefix, other, k)
			}
			seen[k] = prefix
		}
		if len(prefix) == 3 {
			return
		}
		for i := 0; i < len(nameAlphabet); i++ {
			walk(prefix + nameAlphabet[i:i+1])
		}
	}
	walk("")
	if want := 37 + 37*37 + 37*37*37; len(seen) != want {
		t.Fatalf("%d keys, want %d", len(seen), want)
	}
}

// Random names of every packed length: equal keys only for equal names.
func TestPackNameRandomNamesDoNotCollide(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	seen := make(map[uint64]string, 200000)
	for i := 0; i < 200000; i++ {
		n := 1 + rng.Intn(maxPackedName)
		b := make([]byte, n)
		for j := range b {
			b[j] = nameAlphabet[rng.Intn(len(nameAlphabet))]
		}
		name := string(b)
		k, ok := packName(name)
		if !ok {
			t.Fatalf("packName(%q) refused", name)
		}
		if other, dup := seen[k]; dup && other != name {
			t.Fatalf("packName(%q) == packName(%q)", name, other)
		}
		seen[k] = name
	}
}

func TestNameSet(t *testing.T) {
	var s NameSet // the zero value works
	long := strings.Repeat("a", maxPackedName+1)
	maxPacked := strings.Repeat("_", maxPackedName)
	for _, name := range []string{"alice", "a", maxPacked, long, "имя", "has-dash", "UPPER"} {
		if s.Has(name) {
			t.Fatalf("%q in an empty set", name)
		}
		if !s.Add(name) {
			t.Fatalf("Add(%q) reported a duplicate", name)
		}
		if s.Add(name) {
			t.Fatalf("second Add(%q) reported a new name", name)
		}
		if !s.Has(name) {
			t.Fatalf("%q missing after Add", name)
		}
	}
	if s.Len() != 7 {
		t.Fatalf("Len = %d, want 7", s.Len())
	}
	if _, ok := packName(long); ok {
		t.Fatalf("a %d-character name was packed", len(long))
	}
	if s.Has("alicf") || s.Has(long[:maxPackedName]) || s.Has("upper") {
		t.Fatal("a name that was never added is reported present")
	}
	var nilSet *NameSet
	if nilSet.Has("alice") || nilSet.Len() != 0 {
		t.Fatal("a nil set is not empty")
	}
}
