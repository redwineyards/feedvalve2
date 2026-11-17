package lzo_test

// The compressed fixtures these tests decompress were produced by an
// independent LZO1Z implementation, github.com/rhnvrm/lzo1z, and are frozen.
// That independence is the whole point: a decompressor checked only against a
// compressor from the same codebase agrees with itself even when both are
// wrong.
//
// Freezing the bytes costs no coverage. Every input here is deterministic:
// fixed strings, bytes.Repeat, zeroed buffers, and math/rand with hardcoded
// seeds. The fixtures never varied between runs. What it buys is a module with
// one fewer dependency, and no compressor of our own to be wrong.
//
// # Regenerating fixtures
//
// Nothing here needs it. Every input below is either an existing frozen entry
// or is DERIVED from one -- truncated, byte-mutated, given trailing bytes, or
// decompressed into an undersized buffer -- and none of those needs a
// compressor. Adding a test case with a genuinely new input is the only thing
// that would, and such a case fails loudly with its own length and hash rather
// than passing silently. The recording procedure lives in the earlier
// FeedValve repo (the header of its poc/decompress_test.go); this repo has no
// recorder.

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// lzoFixtureFile is the frozen fixture set, copied verbatim from the earlier
// FeedValve repo's poc/testdata/lzo_fixtures.json (poc/ is not in this repo).
// It is an oracle: never re-record it from a compressor in this codebase.
const lzoFixtureFile = "testdata/lzo_fixtures.json"

// lzoFixture is one recorded (input -> compressed) pair. InputLen is carried
// only so a human reading a diff can tell which case an entry belongs to.
type lzoFixture struct {
	InputLen   int    `json:"inputLen"`
	Compressed string `json:"compressed"` // base64
}

var (
	fixturesOnce sync.Once
	fixtures     map[string][]byte
	fixturesErr  error
)

// loadLZOFixtures reads and decodes the frozen fixture file once.
func loadLZOFixtures() (map[string][]byte, error) {
	fixturesOnce.Do(func() {
		raw, err := os.ReadFile(filepath.FromSlash(lzoFixtureFile))
		if err != nil {
			fixturesErr = err
			return
		}
		var recorded map[string]lzoFixture
		if err := json.Unmarshal(raw, &recorded); err != nil {
			fixturesErr = err
			return
		}
		fixtures = make(map[string][]byte, len(recorded))
		for key, f := range recorded {
			b, err := base64.StdEncoding.DecodeString(f.Compressed)
			if err != nil {
				fixturesErr = err
				return
			}
			fixtures[key] = b
		}
	})
	return fixtures, fixturesErr
}

// compressWithReference returns the LZO1Z-compressed form of data as produced
// by the reference implementation and recorded in testdata.
//
// The name is historical: it once called the reference compressor directly,
// and now only looks the answer up. Lookup is by hash of the input, which means a test can build its fixture
// however it likes as long as it does so deterministically.
func compressWithReference(t *testing.T, data []byte) []byte {
	t.Helper()

	recorded, err := loadLZOFixtures()
	if err != nil {
		t.Fatalf("loading %s: %v", lzoFixtureFile, err)
	}

	key := fixtureKey(data)
	compressed, ok := recorded[key]
	if !ok {
		t.Fatalf("no recorded LZO fixture for this input (%d bytes, sha256 %s).\n"+
			"Fixtures are frozen in %s and this repo has no recorder; derive the input\n"+
			"from an existing fixture instead (see the comment at the top of fixtures_test.go).",
			len(data), key, lzoFixtureFile)
	}
	return compressed
}

// fixtureKey is how the frozen set is indexed: the SHA-256 of the input, so a
// test may build its input however it likes as long as it does so
// deterministically.
func fixtureKey(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// buildSyntheticBroadcastMessage builds a byte slice shaped like a real
// exchange broadcast message: a 40-byte header with a few varying fields
// followed by a mostly-zero-padded body, matching the sparse structure of
// the actual on-wire data these packets carry.
func buildSyntheticBroadcastMessage(token int32, messageLen int) []byte {
	data := make([]byte, messageLen)
	data[10] = 0x1c // transactionCode = 7305
	data[11] = 0x89
	be32 := func(off int, v int32) {
		data[off] = byte(v >> 24)
		data[off+1] = byte(v >> 16)
		data[off+2] = byte(v >> 8)
		data[off+3] = byte(v)
	}
	be32(4, 1700000000) // logTime
	be32(14, 42)        // sequenceNumber
	be32(40, token)     // token, right after the header
	for i := 0; i < 20 && 44+i < messageLen; i++ {
		data[44+i] = byte('A' + i%5) // a short non-zero run so it isn't 100% trivial
	}
	return data
}

func randomBytes(t *testing.T, n int, seed int64) []byte {
	t.Helper()
	r := rand.New(rand.NewSource(seed))
	b := make([]byte, n)
	if _, err := r.Read(b); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return b
}

// The 470-byte synthetic MBP-shaped message, which is the input the rejection
// and corruption tests work from: it is the largest frozen case that matches a
// real compressed transcode (7208 MS_BCAST_ONLY_MBP, 470 bytes declared), so a
// truncation or a mutated byte lands in a stream the same shape as one off the
// wire rather than in a degenerate one.
const mbpShapedLen = 470

func mbpShapedFixture(t *testing.T) (data, compressed []byte) {
	t.Helper()
	data = buildSyntheticBroadcastMessage(12345, mbpShapedLen)
	return data, compressWithReference(t, data)
}
