# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository state

Implementation is starting. Go code is being added test-first, as set out in "Go TDD" below, which also
lists the test commands. Two more commands:

- Cross-compile check: `CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 go build ./cmd/feedvalve`. This must always succeed.
- Race tests: `go test -race ./...`. Run them on Linux only, because AIX/ppc64 has no race detector.

The design lives in three Markdown documents:

| File | Role |
|---|---|
| `feedvalve_requirements.md` | System requirements (v4.0). Defines *what* FeedValve must do; architecture is left to the designer. |
| `ncobrd_bible.md` | **Normative protocol reference**: a distilled, error-corrected restatement of the NSEIL NNF protocol v3.5, Chapters 1, 2 and 10 (broadcast). It overrides the requirements doc on protocol facts. |
| `phase1_hld_design1.md` | Phase 1 high-level design for the candidate "Sharded Write-Behind Mirror", with 18 ADRs. |

Documents these files reference that are **not in this repo**: `poc/` (the existing proof-of-concept,
cited as `file:line`) and `phase1_hld2.md` (all three candidate designs plus the comparison matrix).
`phase1_hld_design1.md` has **diverged** from `phase1_hld2.md` (ADR-016, ADR-017), so don't assume
the two are consistent.

## What FeedValve is

A single-process Go service. It receives an exchange's UDP broadcast market-data feed (NSEIL
Commodity Derivatives, netId 8), splits datagrams into sub-packets, LZO1Z-decompresses the
compressed ones, decodes each one, fans it out into per-token update units, coalesces them and
persists them to Oracle.

Fixed constraints (requirements §4, §7):
- **Pure Go, no cgo**, cross-compiled with `CGO_ENABLED=0 GOOS=aix GOARCH=ppc64`. LZO1Z must be implemented in Go. AIX/ppc64 is a second-tier Go port with no race detector, so concurrency testing runs on Linux.
- **Oracle via `go-ora`** only. Binds are `:1`/`:name`, never `?`. Upserts use `MERGE INTO … USING dual` (Oracle has no `ON CONFLICT`).
- Target is an AIX LPAR with a **capped entitled capacity of 4**. Size `GOMAXPROCS` and the worker pools from config, never from the detected CPU count (SMT-8 reports far more logical CPUs). Memory (40 GB) is not the binding constraint. The DB write path is the expected bottleneck.
- `udp_recvspace` = 640 KB, about 0.5 s of buffering, so the **read loop must never stall for more than ~100 ms** or block on DB work (NFR4).
- **No code is reused from `poc/`.** It is a protocol reference and a differential-test oracle: the new decoder must be validated against `poc/`'s output on fabricated inputs.
- The Oracle password comes **only from the environment**, never a flag or the config file. Redact the DSN in all logs.
- Every tunable is runtime-reloadable by default (NFR8).
- **All times in config are integer milliseconds**, and the field name ends in `Ms` (e.g. `connectTimeoutMs`). Convert at the point of use with `time.Duration(ms) * time.Millisecond`.

## Frozen: `internal/lzo`

**Do not modify `internal/lzo/` or `tests/internal/lzo/` (including `testdata/`) in any way: no code, comment, formatting or fixture changes.** Use the package only through its exported API (`DecompressSafe` on the receive path, never `Decompress`). It is exempt from the function-layout and TDD rules below. If a change ever seems necessary, stop and ask the user.

## Protocol pitfalls (from `ncobrd_bible.md`, read before writing any decoder)

- Everything is **big-endian**. Read field by field through a cursor and never `memcpy` onto structs.
- Layout is **`#pragma pack(2)`** ("word" = 2 bytes). **The spec's Offset columns are often wrong.** The declared `Packet Length` is authoritative. Reconcile with the bible's Part V and errata in Appendix B. Read padding explicitly, e.g. `r.pad("name", n)`.
- **8-byte prefix**: every sub-packet payload has 8 bytes before the header (after decompression for compressed ones). Skip it in exactly one place. Byte 0 must equal the envelope netId, which serves as a frame-alignment check: log a mismatch, don't drop on it.
- `BCAST_HEADER` is used for most messages, but **`MESSAGE_HEADER` for 6503 and 6541**. Dispatch on `TransactionCode` (header offset 10 = payload offset 18). Check `ErrorCode` first.
- Clamp every record-count field to the array bound. Bounds-check LZO decompression, since input is untrusted. A malformed packet is logged and dropped, never fatal.
- 7200 and 7208 are mutually exclusive: support whichever the exchange sends. 6541 is the heartbeat. There is no retransmission, so `BCSeqNo` gaps mean data is permanently lost and must be surfaced.
- The bible's Part VI is the implementation checklist.

## Chosen-candidate architecture (`phase1_hld_design1.md`)

*Distribute-then-coalesce*:
- Decode runs **inline on the receive goroutine** (receiver → framing → lzo → header → decoders → policy/destinations → router), with no queue before the decoder.
- The router hashes the token to a fixed set of **shard goroutines**. Each shard exclusively owns its partition of **three coalescing mirrors, one per destination table**, keyed by destination row.
- A separately sized pool of **stateless writers** commits array-bound `MERGE` batches, one transaction across all three destinations in a fixed order.
- **Two policy classes**: `rate-limited` (coalesce, flush on global tick `W`) and `non-rate-limited` (apply, then trigger an immediate flush of all shards). A flush is an **epoch**: it writes the entire dirty set. Rows dirtied during the flush go to the next epoch.
- Ordering (FR9) comes from single-writer arrival order. Header sequence comparison is a defence, not the mechanism.
- Key invariants: `UpdateUnit` holds only scalars and fixed-size arrays, never slices, pointers or strings, because the receive buffers are reused. Every destination row a unit touches must belong to that unit's own shard.
- Three deliberate deviations from the requirements, stated in HLD §1.1:
  - "rate-limited" there means lossless coalescing, the opposite of the requirements glossary.
  - FR7 persists the latest occurrence applied, not every occurrence.
  - There is one global flush window, not a per-transcode window.

## Design principles: KISS and YAGNI

These apply to both the design documents and the code.

- **KISS (keep it simple).** Choose the simplest mechanism that meets the requirement. Prefer one data path, stdlib primitives (goroutines, channels, `sync`/`atomic`) and plain structs over frameworks, generic abstractions or layers of indirection. Any added complexity must be justified by a specific requirement ID or measured need, in the text or an ADR. Operational simplicity is rubric criterion 3 (req §11.1): fewer interacting tunables and fewer moving parts win when designs are otherwise equal.
- **YAGNI (you aren't gonna need it).** Build only what the current requirements and the current phase call for. Don't add:
  - hooks, interfaces or config knobs for hypothetical futures
  - out-of-scope features such as multi-feed, horizontal scaling, replay, rich metrics or an HTTP endpoint (req §8)
  - decoders for transcodes that aren't in the configured inventory
  
  Note where something would attach later, but don't build it.
- Where the two conflict with a requirement, the requirement wins. Don't simplify away robustness obligations: bounds checks, per-packet containment, the NFR4 stall budget and ordering (FR9).

## Go TDD (Test-Driven Development) Workflow
- **Strict Test-First Policy**: Write or modify a `_test.go` file BEFORE changing any production code.
- **Red-Green-Refactor Cycle**:
  1. **RED**: Write the test case. Run `go test ./...` to verify it fails due to a missing or incomplete implementation (compilation errors count as a failure state).
  2. **GREEN**: Write the minimal Go code necessary to make that specific test pass. Run `go test ./...` to verify success.
  3. **REFACTOR**: Improve readability, remove duplication, and ensure idiomatic Go (e.g., proper error handling, naming conventions) while keeping the test suite green.
- **Commands**:
  - Run all tests: `go test ./...`
  - Run specific test: `go test -run TestName ./...`
- **Go Specific Rules**:
  - Table-Driven Tests: Prefer table-driven testing (`struct` slices with `t.Run`) for functions with multiple inputs/outputs.
  - **Test location**: All tests live under a root `tests/` directory that mirrors the source tree (`internal/config/config.go` → `tests/internal/config/config_test.go`). Test packages are named `<pkg>_test` and import the code under test (`feedvalve/internal/config`). No `_test.go` files sit beside production code.
  - **Test the exported API only**: Because `tests/` is a separate package, tests can reach only exported identifiers. Every exported function, type and method needs a test. Unexported helpers are covered through the exported API that uses them. Don't export something just to test it.
  - **Keep `main` thin**: Package `main` can't be imported, so `cmd/feedvalve/main.go` only parses flags, sets up signals, calls `app.Run` and sets the exit code. All startup logic lives in `internal/app`, where tests can reach it.

## Go code style: function layout

Applies to production code (`cmd/`, `internal/`). Test files under `tests/` are exempt.

- **Function comment:** every function and method has a comment before it saying, in simple terms, what it does.
- **Fixed order inside a function:** variables → config reads → allocations → logic. Each section sits together in one place, before any real logic.
  1. **Variables:** one `var (...)` block at the top of the scope, each line with a short comment giving its purpose. Assign later with `=`.
  2. **Config reads:** read every config value and environment setting the function needs, together, as early as possible.
  3. **Allocations:** every `make()`, buffer and allocating literal, together, sized from the config just read. If an allocation needs a value only the logic produces (e.g. a JSON decoder over file bytes just read), put it as early as its inputs allow. Its variable is still declared in the `var` block.
  4. **Logic:** everything else.
- **Config is read in one place:** only `main` (flags) and the start of `app.Run` (config file and env vars) read configuration. They pass the values to whatever needs them as arguments. No other package imports `internal/config` or calls `os.Getenv`.
- **Exempt:** `err`/`ok` results, loop counters and range variables, and variables scoped to an `if`/`switch` init statement.

## Working on the documents

- The requirements doc uses precise terms (datagram, packet, update unit, coalescing vs. rate limiting, and so on; §2). Keep that usage.
- Cite protocol claims as `§` references into `ncobrd_bible.md`, and requirements as `req §N` / FR/NFR IDs, as the HLD does.
- The process has two phases (req §11): Phase 1 picks an HLD against the rubric in §11.1 (burst robustness > latency > operational simplicity > insensitivity to open items > effort), then Phase 2 produces LLDs. If Phase 2 invalidates a Phase 1 assumption, it goes back to the stakeholder and is not absorbed silently.
- Open items are listed in req §12 and HLD §15 / Appendix A.
