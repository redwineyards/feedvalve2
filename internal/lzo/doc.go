// Package lzo is a pure Go port of LZO1Z decompression, copied from the
// earlier FeedValve repo's internal/lzo (itself carried over from its poc/).
//
// lzo MUST NOT import anything outside the standard library.
//
// DecompressSafe is bounds-checked and returns an error on malformed input; it
// is what the receive path uses, because compressed sub-packets are untrusted
// network input (bible §3.5). Decompress is the unchecked variant and may panic
// on malformed input -- it is for tests, where the fixtures are known good.
//
// This package decompresses. It does not compress, and it must stay that way.
//
// The compressed bytes the tests decompress come from an independent LZO1Z
// implementation (github.com/rhnvrm/lzo1z) and are frozen as recorded fixtures
// in tests/internal/lzo/testdata/lzo_fixtures.json, not produced at test time.
// The module therefore has no compression dependency at all, and should not
// gain one.
//
// Writing a compressor of our own would be worse than either. A decompressor
// validated only against a compressor from the same codebase agrees with
// itself even when both are wrong.
//
// The port exists so the binary needs no cgo and no liblzo2, which is what keeps
// CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 working (req §4).
package lzo
