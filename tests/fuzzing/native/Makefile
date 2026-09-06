# Native fuzzing — the file parsers, which are the only code here that reads
# bytes it did not write.
#
#   make replay     build the replay drivers and run the corpus (works everywhere)
#   make fuzz       build the libFuzzer targets
#   make run        build and fuzz for FUZZ_SECONDS each (default 60)
#   make clean
#
# Plain make, matching skene/Makefile: cmake is not reliably present on the dev
# machine and `make` is the sanctioned interface in opteryx-core.
#
# WHY TWO BUILDS. libFuzzer's runtime (libclang_rt.fuzzer_osx.a) does not ship
# with Apple's Command Line Tools, so `-fsanitize=fuzzer` does not link on a
# stock macOS dev machine — the platform this repo is developed on. `replay`
# needs only ASan/UBSan, so a developer can run and debug the same harnesses
# locally; `fuzz` searches for new inputs where the runtime exists (CI, Linux).
# The harness sources are identical either way: replay_main.cpp supplies the
# main() that libFuzzer would otherwise provide.
#
# SANITIZERS ARE THE ORACLE. Every parser under test reports malformed input by
# returning a status or throwing, and both are a PASS — refusing a bad file is
# the reader working. Only a crash, a hang, or an ASan/UBSan report is a
# failure. That is why the sources under test are COMPILED here with the
# sanitizer flags rather than linked from a prebuilt library: linking
# skene/build/libskene.a would leave the very code being fuzzed uninstrumented.

ROOT     ?= $(CURDIR)/../../..
CXX      ?= clang++
CXXSTD   ?= -std=c++20
OPT      ?= -O1
SAN      ?= -fsanitize=address,undefined -fno-omit-frame-pointer
# -w because the code under test is not this harness's to warn about; the
# harness sources themselves are small enough to keep clean by inspection.
BASE     := $(CXXSTD) $(OPT) -g $(SAN) -w

BUILD    := build
CORPUS   := corpus
FUZZ_SECONDS ?= 60

# --- shared include roots -----------------------------------------------------
DRAKEN_INC := -I$(ROOT)/draken -I$(ROOT)/draken/core -I$(ROOT)/draken/simd \
              -I$(ROOT)/third_party/cyan4973 -I$(ROOT)/third_party/mabel/carchar

# --- skene --------------------------------------------------------------------
# zstd is compiled in because per-section compression is part of the format, so
# a decompressor fed a hostile section is in scope.
#
# huf_decompress.cpp references BMI2 fast-path symbols
# (HUF_decompress4X1/4X2_usingDTable_internal_fast_asm_loop) that are defined
# in huf_decompress_amd64.S, not in any .cpp — the *.cpp wildcard alone links
# on arm64 (the asm is dead code there) but leaves those symbols undefined on
# x86-64. skene/Makefile hits the same trap; see its ZSTD_OBJS comment.
ZSTD_DIR  := $(ROOT)/third_party/zstd
ZSTD_SRCS := $(wildcard $(ZSTD_DIR)/common/*.cpp) \
             $(wildcard $(ZSTD_DIR)/compress/*.cpp) \
             $(wildcard $(ZSTD_DIR)/decompress/*.cpp) \
             $(ZSTD_DIR)/decompress/huf_decompress_amd64.S

# skene's LZ4 codec (skene/src/encoding.cpp) needs the vendored implementation;
# without -I/lz4.c this only fails at link time, since Ubuntu CI runners have
# liblz4-dev's header on the system include path already.
LZ4_DIR   := $(ROOT)/third_party/lz4

SKENE_INC  := -I$(ROOT)/skene/include -I$(ROOT)/skene/src -I$(ZSTD_DIR) \
              -I$(LZ4_DIR) $(DRAKEN_INC)
SKENE_SRCS := $(wildcard $(ROOT)/skene/src/*.cpp) \
              $(ROOT)/draken/core/vector_alloc.cpp \
              $(LZ4_DIR)/lz4.c \
              $(ZSTD_SRCS)
# skene_native.cpp is the Cython shim and needs Python; the format code does not.
SKENE_SRCS := $(filter-out %/skene_native.cpp,$(SKENE_SRCS))
# Let the mutator past the integrity layer. Every mutation of a real .skene file
# breaks a checksum, so without this the read is rejected before reaching the
# structural validation and buffer building that carry the memory-safety risk —
# measured, 300 random mutations all bounced. Skips only the COMPARISON: bounds
# checks still run ahead of every checksum, so this never converts a rejected
# read into an out-of-bounds one. See skene/include/skene/checksum.h, and
# `make -C skene check-no-fuzz-flag`, which asserts no shipping build sets it.
SKENE_DEFS := -DSKENE_FUZZING_SKIP_CHECKSUM

# --- rugo parquet -------------------------------------------------------------
PARQUET_INC  := -I$(ROOT)/rugo/src -I$(ROOT)/rugo/src/parquet $(DRAKEN_INC) \
                -I$(ZSTD_DIR) -I$(ROOT)/third_party/bshoshany \
                -I$(ROOT)/third_party/snappy -I$(ROOT)/third_party/lz4 \
                -I$(ROOT)/third_party/miniz
# %_test.cpp carries its own main() and would collide with replay_main.cpp's.
PARQUET_SRCS := $(filter-out %_test.cpp,$(wildcard $(ROOT)/rugo/src/parquet/*.cpp)) \
                $(ROOT)/third_party/snappy/snappy.cc \
                $(ROOT)/third_party/snappy/snappy-sinksource.cc \
                $(ROOT)/third_party/snappy/snappy-stubs-internal.cc \
                $(ROOT)/third_party/lz4/lz4.c \
                $(ROOT)/third_party/miniz/miniz_tinfl.cpp \
                $(ROOT)/draken/core/vector_alloc.cpp \
                $(ROOT)/draken/core/trace_bridge.cpp \
                $(ZSTD_SRCS)
PARQUET_DEFS := -DRUGO_PARQUET_CREATED_BY='"opteryx-fuzz"' -DHAVE_ZSTD

# --- rugo jsonl / csv ---------------------------------------------------------
# The column builders are excluded from both: they return PyObject* and so
# cannot run outside an interpreter. Everything upstream of them — the scanners,
# the document map, the quote FSM — is pure C++ and is where the bounds
# arithmetic lives.
TEXT_INC   := -I$(ROOT)/rugo/src $(DRAKEN_INC) -I$(ROOT)/third_party/fastfloat \
              -I$(ROOT)/third_party/bshoshany -I$(ROOT)/src/cpp
JSONL_SRCS := $(filter-out %/column_builder.cpp,$(wildcard $(ROOT)/rugo/src/jsonl/core/*.cpp))
CSV_SRCS   := $(filter-out %/csv_column_builder.cpp,$(wildcard $(ROOT)/rugo/src/csv/core/*.cpp))

# Overridable so a release pipeline gates on its OWN parsers and nothing else:
# skene's floor should not go red because a Parquet corpus input regressed, and
# rugo's should not go red because a .skene one did.
#   make replay TARGETS=skene
#   make replay TARGETS="parquet jsonl csv"
TARGETS ?= skene parquet jsonl csv

# Rebuild when the code under test changes, not only when a harness does.
# Without these a developer edits skene/reader.cpp, runs `make fuzz-native`, and
# replays a stale binary that reports clean — the same class of trap as a build
# that ignores header changes. Headers are globbed alongside the sources
# because they carry as much of these parsers as the .cpp files do.
SKENE_DEPS   := $(SKENE_SRCS) $(wildcard $(ROOT)/skene/include/skene/*.h) \
                $(wildcard $(ROOT)/skene/src/*.h)
PARQUET_DEPS := $(PARQUET_SRCS) $(wildcard $(ROOT)/rugo/src/parquet/*.hpp)
JSONL_DEPS   := $(JSONL_SRCS) $(wildcard $(ROOT)/rugo/src/jsonl/core/*.hpp)
CSV_DEPS     := $(CSV_SRCS) $(wildcard $(ROOT)/rugo/src/csv/core/*.hpp)

.PHONY: all replay fuzz run clean check-fuzzer corpus-dirs

all: replay

# ---------------------------------------------------------------------------
# Replay drivers — corpus in, sanitizer report out. No libFuzzer needed.
# ---------------------------------------------------------------------------
$(BUILD):
	@mkdir -p $(BUILD)

$(BUILD)/skene_replay: fuzz_skene.cpp replay_main.cpp $(SKENE_DEPS) | $(BUILD)
	$(CXX) $(BASE) $(SKENE_DEFS) $(SKENE_INC) fuzz_skene.cpp replay_main.cpp $(SKENE_SRCS) -o $@

$(BUILD)/parquet_replay: fuzz_parquet.cpp replay_main.cpp $(PARQUET_DEPS) | $(BUILD)
	$(CXX) $(BASE) $(PARQUET_DEFS) $(PARQUET_INC) fuzz_parquet.cpp replay_main.cpp $(PARQUET_SRCS) -o $@

$(BUILD)/jsonl_replay: fuzz_jsonl.cpp replay_main.cpp $(JSONL_DEPS) | $(BUILD)
	$(CXX) $(BASE) $(TEXT_INC) fuzz_jsonl.cpp replay_main.cpp $(JSONL_SRCS) -o $@

$(BUILD)/csv_replay: fuzz_csv.cpp replay_main.cpp $(CSV_DEPS) | $(BUILD)
	$(CXX) $(BASE) $(TEXT_INC) fuzz_csv.cpp replay_main.cpp $(CSV_SRCS) -o $@

REPLAY_BINS := $(addprefix $(BUILD)/,$(addsuffix _replay,$(TARGETS)))

# Replays every corpus input, including any crash input a fuzzing run has
# committed. Empty corpus is not silently a pass: it reports the count.
replay: $(REPLAY_BINS)
	@fail=0; \
	for t in $(TARGETS); do \
	  inputs=$$(ls $(CORPUS)/$$t/* 2>/dev/null); \
	  count=$$(printf '%s\n' "$$inputs" | grep -c . || true); \
	  echo "== $$t ($$count input(s))"; \
	  if [ "$$count" -gt 0 ]; then \
	    $(BUILD)/$${t}_replay $$inputs >/dev/null || fail=1; \
	  fi; \
	done; \
	if [ $$fail -ne 0 ]; then echo "REPLAY FAILED"; exit 1; fi; \
	echo "corpus replayed clean"

# ---------------------------------------------------------------------------
# libFuzzer targets — search for new inputs.
# ---------------------------------------------------------------------------
# Fail with the actual remedy rather than a linker error about a missing .a.
check-fuzzer: | $(BUILD)
	@echo 'extern "C" int LLVMFuzzerTestOneInput(const unsigned char*, unsigned long){return 0;}' > $(BUILD)/.probe.cpp
	@$(CXX) $(CXXSTD) -fsanitize=fuzzer $(BUILD)/.probe.cpp -o $(BUILD)/.probe 2>/dev/null || { \
	  echo "ERROR: this toolchain has no libFuzzer runtime."; \
	  echo "       Apple's Command Line Tools ship libFuzzer's headers but not"; \
	  echo "       libclang_rt.fuzzer_osx.a, so -fsanitize=fuzzer cannot link."; \
	  echo "       Use 'make replay' locally; 'make fuzz' runs in CI on Linux."; \
	  exit 1; }
	@rm -f $(BUILD)/.probe $(BUILD)/.probe.cpp

$(BUILD)/skene_fuzz: fuzz_skene.cpp $(SKENE_DEPS) | $(BUILD)
	$(CXX) $(BASE) -fsanitize=fuzzer $(SKENE_DEFS) $(SKENE_INC) $< $(SKENE_SRCS) -o $@

$(BUILD)/parquet_fuzz: fuzz_parquet.cpp $(PARQUET_DEPS) | $(BUILD)
	$(CXX) $(BASE) -fsanitize=fuzzer $(PARQUET_DEFS) $(PARQUET_INC) $< $(PARQUET_SRCS) -o $@

$(BUILD)/jsonl_fuzz: fuzz_jsonl.cpp $(JSONL_DEPS) | $(BUILD)
	$(CXX) $(BASE) -fsanitize=fuzzer $(TEXT_INC) $< $(JSONL_SRCS) -o $@

$(BUILD)/csv_fuzz: fuzz_csv.cpp $(CSV_DEPS) | $(BUILD)
	$(CXX) $(BASE) -fsanitize=fuzzer $(TEXT_INC) $< $(CSV_SRCS) -o $@

FUZZ_BINS := $(addprefix $(BUILD)/,$(addsuffix _fuzz,$(TARGETS)))

fuzz: check-fuzzer $(FUZZ_BINS)
	@echo "built: $(FUZZ_BINS)"

# A crashing input is written into the corpus, so committing it turns the
# discovery into a permanent regression case for `make replay`.
run: fuzz corpus-dirs
	@fail=0; \
	for t in $(TARGETS); do \
	  echo "== fuzzing $$t for $(FUZZ_SECONDS)s"; \
	  $(BUILD)/$${t}_fuzz $(CORPUS)/$$t \
	    -max_total_time=$(FUZZ_SECONDS) -print_final_stats=1 \
	    -artifact_prefix=$(CORPUS)/$$t/ || fail=1; \
	done; \
	if [ $$fail -ne 0 ]; then echo "FUZZING FOUND A CRASH — see corpus/ for the input"; exit 1; fi

corpus-dirs:
	@for t in $(TARGETS); do mkdir -p $(CORPUS)/$$t; done

clean:
	rm -rf $(BUILD)
