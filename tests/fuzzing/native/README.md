# Native fuzzing

Coverage-guided fuzzing of the file parsers — skene, and rugo's Parquet, JSONL
and CSV readers.

These are the only components in the tree that read bytes they did not write.
A SQL fuzzer explores what the engine *computes*; this explores what it will
*accept*, which is a different failure mode: a malformed footer is a bounds
question, not a wrong-answer question, and its consequence is a crash or an
out-of-bounds read rather than a bad result. skene states the stakes in its own
error model — the format "memcpys buffers and rebuilds absolute pointers from
stored offsets, so continuing past a detected inconsistency is memory corruption
rather than a wrong answer" (`skene/status.h`).

## Running it

```
make replay     # build the replay drivers, run the corpus     (works everywhere)
make fuzz       # build the libFuzzer targets
make run        # fuzz each target for FUZZ_SECONDS (default 60)
make clean
```

`make replay` is the one to run locally and on every CI push: it is
deterministic, takes seconds, and re-checks every corpus input — including any
input a fuzzing run has previously crashed on — under ASan and UBSan. `make run`
searches for *new* inputs and belongs on the nightly schedule.

### Two builds, one harness

The harnesses expose `LLVMFuzzerTestOneInput` and nothing else.
`replay_main.cpp` supplies the `main()` that libFuzzer would otherwise provide,
so the same source builds both ways.

This is not redundancy. libFuzzer's runtime (`libclang_rt.fuzzer_osx.a`) does
not ship with Apple's Command Line Tools — the headers are there, the runtime is
not — so `-fsanitize=fuzzer` cannot link on a stock macOS dev machine, which is
the platform this repo is developed on. Without the replay driver the native
fuzzers would be CI-only code that a developer could neither run nor debug.
`make fuzz` says so explicitly rather than emitting a linker error.

## The oracle is the sanitizer

Every parser here reports malformed input by returning a status or by throwing,
and **both are a pass**. Refusing a bad file is the reader working. Only three
things are failures:

* a crash
* a hang
* an ASan or UBSan report

This is why the code under test is *compiled* by this Makefile with the
sanitizer flags rather than linked from `skene/build/libskene.a` or the rugo
extension: linking a prebuilt library would leave the very code being fuzzed
uninstrumented, and the harness would report clean no matter what it hit.

It is also why the Parquet harness wraps its calls in `catch (...)`. That is not
flow control hiding a failure — a memory error fires before any exception could
be thrown, so the catch cannot mask one; it only stops a rejected file from
ending the run.

## Corpus

`corpus/<target>/` holds seed inputs, currently small real files from
`testdata/`. libFuzzer writes new interesting inputs back into the same
directory, and writes any crashing input there too (`-artifact_prefix`).

**Commit a crashing input.** That is what turns a one-off discovery into a
permanent regression case, because `make replay` runs everything in `corpus/`.

## Getting past skene's checksums

skene verifies a file-footer checksum, then the row group footer's checksum (as
recorded in the file footer's row group directory), then a per-section checksum
before each section is used — three levels since row groups were packed into
files. A mutated byte fails validation long before reaching the structural checks
and buffer building — which is where the memory-safety risk
actually lives. Measured: 300 random mutations of a real `.skene` file were all
rejected cleanly, and none reached the interesting code.

The skene target is therefore built with `-DSKENE_FUZZING_SKIP_CHECKSUM`, which
skips only the *comparison* — every bounds check still runs ahead of it, so this
never turns a rejected read into an out-of-bounds one. See
`skene/include/skene/checksum.h`, and `make -C skene check-no-fuzz-flag`, which
asserts no shipping build defines it.

**This is not a lower bar, it is the right threat model.** A checksum defends
against accidental corruption. It does not defend against a crafted file, because
whoever crafts the bytes computes the checksums over them too. Skipping the
comparison models the attacker rather than the disk error.

It paid for itself immediately: with the flag on, 12 of 500 mutations crashed —
three distinct ASan signatures (BUS, heap-buffer-overflow, heap-use-after-free)
at one site in `build_column`. Those inputs are in `corpus/skene/` as
`crash-build_column-*.skene`.

Parquet is less affected (its structural fields are not checksum-protected) and
the JSONL and CSV scanners are not affected at all.
