package lzo

import "errors"

// The four sentinels below are not four spellings of "decompression failed".
// They mean four genuinely different things to an operator, so each should get
// its own failure counter (FR10). Collapsing them would throw away a
// distinction this port makes for free and leave an operator unable to tell a
// truncated stream (wire corruption, escalate) from an output overrun (buffer
// too small, raise it) -- different problems with different fixes.
//
// Mapping a sentinel to a counter is deliberately NOT done here. lzo imports
// nothing outside the standard library, which keeps it free of cgo and
// liblzo2. The mapping belongs at the call site, where the counter is
// incremented.
var (
	// ErrInputOverrun is returned when the compressed input is truncated:
	// an opcode or match needed more input bytes than were available.
	// Corresponds to LZO_E_INPUT_OVERRUN.
	//
	// Operationally: wire corruption, or iCompLen disagreeing with the bytes
	// actually sent. Capture and escalate.
	ErrInputOverrun = errors.New("lzo1z: input overrun")

	// ErrOutputOverrun is returned when decompression would write more
	// bytes than fit in the destination buffer.
	// Corresponds to LZO_E_OUTPUT_OVERRUN.
	//
	// Operationally: a message decompressed larger than the buffer allows -- a
	// capacity problem with a known fix. Completely different from the other
	// three. See MaxDecompressedLen.
	ErrOutputOverrun = errors.New("lzo1z: output overrun")

	// ErrLookbehindOverrun is returned when a match references a
	// position before the start of the output buffer (corrupt distance).
	// Corresponds to LZO_E_LOOKBEHIND_OVERRUN.
	//
	// Operationally: wire corruption.
	ErrLookbehindOverrun = errors.New("lzo1z: lookbehind overrun")

	// ErrInputNotConsumed is returned when the end-of-stream marker was
	// found but trailing bytes remain in the input.
	// Corresponds to LZO_E_INPUT_NOT_CONSUMED.
	//
	// Operationally: iCompLen is larger than the real compressed length -- a
	// framing question, not a compression one.
	ErrInputNotConsumed = errors.New("lzo1z: input not consumed")
)
