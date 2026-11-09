package checker

import (
	"bytes"
	"io"
)

// NameSet is the set used to drop duplicate names from a list and to
// remember the names a resumed run skips. A map[string]struct{} cost about
// 148 MB of memory per million names (the string header, a separate
// allocation per name, and a map the garbage collector has to scan), which
// limited the console to roughly 1.5 million unique names on a machine
// with 1 GB of RAM.
//
// A name of up to 12 characters from the Telegram alphabet (a-z, 0-9, _)
// is packed into a uint64 exactly, so it costs one 8-byte key in a map
// without pointers, which the garbage collector does not scan. Any other
// token (13 to 32 characters, or bytes outside that alphabet) is kept as a
// string. Membership is exact: it is never a hash that could make two
// names collide. The zero value is ready to use. Not safe for concurrent
// use.
type NameSet struct {
	packed map[uint64]struct{}
	other  map[string]struct{}
}

// maxPackedName is the longest name packName can store: 38^12 < 2^64.
const maxPackedName = 12

// Add adds name and reports whether it was not in the set yet.
func (s *NameSet) Add(name string) bool {
	if k, ok := packName(name); ok {
		if _, dup := s.packed[k]; dup {
			return false
		}
		if s.packed == nil {
			s.packed = make(map[uint64]struct{}, 1024)
		}
		s.packed[k] = struct{}{}
		return true
	}
	if _, dup := s.other[name]; dup {
		return false
	}
	if s.other == nil {
		s.other = make(map[string]struct{})
	}
	s.other[name] = struct{}{}
	return true
}

// Has reports whether name is in the set. A nil set is empty.
func (s *NameSet) Has(name string) bool {
	if s == nil {
		return false
	}
	if k, ok := packName(name); ok {
		_, found := s.packed[k]
		return found
	}
	_, found := s.other[name]
	return found
}

// Len returns the number of names in the set. A nil set is empty.
func (s *NameSet) Len() int {
	if s == nil {
		return 0
	}
	return len(s.packed) + len(s.other)
}

// packName maps a name of 1 to maxPackedName characters from [a-z0-9_] to
// a distinct uint64. Every character is a digit from 1 to 37 in base 38
// (0 is never used), so names of different lengths cannot meet: all names
// of length L fall between (38^L-1)/37 and 38^L-1, below the smallest
// value of length L+1.
func packName(name string) (uint64, bool) {
	if len(name) == 0 || len(name) > maxPackedName {
		return 0, false
	}
	var k uint64
	for i := 0; i < len(name); i++ {
		var d uint64
		switch c := name[i]; {
		case c >= 'a' && c <= 'z':
			d = uint64(c-'a') + 1 // 1..26
		case c >= '0' && c <= '9':
			d = uint64(c-'0') + 27 // 27..36
		case c == '_':
			d = 37
		default:
			return 0, false
		}
		k = k*38 + d
	}
	return k, true
}

// NameList keeps names in one byte buffer, one name per line: about one
// byte per character, instead of a string header and an allocation per
// name in a []string. The zero value is ready to use.
type NameList struct {
	buf []byte
	n   int
}

// Add appends name, which must not contain a new line.
func (l *NameList) Add(name string) {
	l.buf = append(l.buf, name...)
	l.buf = append(l.buf, '\n')
	l.n++
}

// Len returns the number of names. A nil list is empty.
func (l *NameList) Len() int {
	if l == nil {
		return 0
	}
	return l.n
}

// Each calls fn for every name, in order, and stops at the first error.
func (l *NameList) Each(fn func(name string) error) error {
	if l == nil {
		return nil
	}
	b := l.buf
	for len(b) > 0 {
		i := bytes.IndexByte(b, '\n')
		if err := fn(string(b[:i])); err != nil {
			return err
		}
		b = b[i+1:]
	}
	return nil
}

// CollectNames reads a list with ScanNames and keeps the valid names,
// without duplicates and in order, in a NameList; rejected counts the
// invalid ones. It gives the same names as ParseNamesReader for lists too
// large for a []string: the list costs about one byte per character plus
// a new line, so a million names of 9 characters take 10 MB, where a
// []string needs a 16-byte header and an allocation per name on top.
func CollectNames(r io.Reader) (list *NameList, rejected int, err error) {
	list = new(NameList)
	var seen NameSet
	err = ScanNames(r, func(name string) error {
		if ValidateUsername(name) != nil {
			rejected++
			return nil
		}
		if seen.Add(name) {
			list.Add(name)
		}
		return nil
	})
	return list, rejected, err
}
