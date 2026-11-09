package checker

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
)

// benchList is a 10 MB list like a real export: names, some with "@",
// separated by new lines, commas and spaces, and a few "| status" tails.
func benchList() []byte {
	var b bytes.Buffer
	for i := 0; b.Len() < 10<<20; i++ {
		switch i % 4 {
		case 0:
			fmt.Fprintf(&b, "@name%07d\n", i)
		case 1:
			fmt.Fprintf(&b, "name%07d, ", i)
		case 2:
			fmt.Fprintf(&b, "name%07d | free\n", i)
		default:
			fmt.Fprintf(&b, "Name%07d\r\n", i)
		}
	}
	return b.Bytes()
}

func BenchmarkScanNames(b *testing.B) {
	data := benchList()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := 0
		if err := ScanNames(bytes.NewReader(data), func(string) error { n++; return nil }); err != nil {
			b.Fatal(err)
		}
	}
}

// scanNamesByteByByte is ScanNames as it was before it read in chunks: the
// reference that the chunked version must match exactly.
func scanNamesByteByByte(r io.Reader, fn func(name string) error) error {
	br := bufio.NewReaderSize(r, 64*1024)
	tok := make([]byte, 0, 64)
	skipLine := false
	emit := func() error {
		if len(tok) == 0 {
			return nil
		}
		name := strings.ToLower(strings.TrimPrefix(string(tok), "@"))
		tok = tok[:0]
		if name == "" {
			return nil
		}
		return fn(name)
	}
	for {
		b, err := br.ReadByte()
		if err != nil {
			if eerr := emit(); eerr != nil {
				return eerr
			}
			if err == io.EOF {
				return nil
			}
			return err
		}
		if skipLine {
			skipLine = b != '\n'
			continue
		}
		switch b {
		case '\n', '\r', ' ', '\t', ',', ';':
			if err := emit(); err != nil {
				return err
			}
		case '|':
			if err := emit(); err != nil {
				return err
			}
			skipLine = true
		default:
			if len(tok) < maxNameToken {
				tok = append(tok, b)
			}
		}
	}
}

func scanAll(t *testing.T, scan func(io.Reader, func(string) error) error, r io.Reader) []string {
	t.Helper()
	var out []string
	if err := scan(r, func(n string) error { out = append(out, n); return nil }); err != nil {
		t.Fatal(err)
	}
	return out
}

// The chunked ScanNames gives exactly the tokens of the byte-by-byte one:
// for tokens and "| ..." tails split across reads (a reader that returns
// one byte at a time, or a few), an over-long token, and random lists.
func TestScanNamesChunkedMatchesByteByByte(t *testing.T) {
	inputs := []string{
		"", "alice", "alice\nbob\r\ncarol", "@Alice, @alice;BOB\tdan", "a | b c\nd", "|x\ny", "x|\n|\nz",
		"name | free\n@other | taken", strings.Repeat("a", maxNameToken+100) + " b", "ab\xffcd ef", "@@x @ y",
		"tail without newline | free", strings.Repeat("abc,", 20000),
		"ÄBC Имя ÉCOLE @ÜBER, @Straße\nМОСКВА", "Ab\xffCD @\xc3X", // non-ASCII: strings.ToLower
	}
	rng := rand.New(rand.NewSource(7))
	const alphabet = "ab@Z_9 \n\r\t,;|xÄé"
	for i := 0; i < 300; i++ {
		b := make([]byte, rng.Intn(400))
		for j := range b {
			b[j] = alphabet[rng.Intn(len(alphabet))]
		}
		inputs = append(inputs, string(b))
	}
	inputs = append(inputs, string(benchList()[:300000]))
	for _, in := range inputs {
		want := scanAll(t, scanNamesByteByByte, strings.NewReader(in))
		for _, r := range []io.Reader{strings.NewReader(in), iotest.OneByteReader(strings.NewReader(in)),
			iotest.HalfReader(strings.NewReader(in)), iotest.DataErrReader(strings.NewReader(in))} {
			if got := scanAll(t, ScanNames, r); !reflect.DeepEqual(got, want) {
				t.Fatalf("input %.60q: chunked %d tokens, byte-by-byte %d", in, len(got), len(want))
			}
		}
	}
}
