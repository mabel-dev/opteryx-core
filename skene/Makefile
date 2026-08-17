# skene — .skene columnar file format (C++, no Python, no dependencies).
#
#   make            build the library and tests
#   make test       build and run every test
#   make clean
#
# Plain make rather than CMake: cmake is not reliably present on the dev
# machine, `make` is the sanctioned interface in opteryx-core, and this repo
# folds into opteryx-core's setuptools build eventually — so a build system with
# zero install footprint is the right amount of machinery.

# ---------------------------------------------------------------------------
# draken
#
# skene SERIALIZES draken's own structures, so it imports draken's headers. It
# does NOT copy them: DrakenType and LogicalType are draken's, imported never
# copied (opteryx-core .claude/CLAUDE.md §14), and a hand-maintained duplicate
# of a frozen 40-byte ABI pinned by static_asserts is exactly the drift that
# freeze exists to prevent.
#
# skene now lives INSIDE opteryx-core (moved 2026-08-07); DRAKEN_ROOT defaults
# to the repo root one level up. Override for an out-of-tree build:
#
#   make DRAKEN_ROOT=/path/to/opteryx-core
# ---------------------------------------------------------------------------
DRAKEN_ROOT ?= $(CURDIR)/..

CXX      ?= c++
CC       ?= cc
# C++20 because that is what opteryx-core compiles draken with (build_common.py
# CPP_FLAGS). skene compiles the SAME draken headers, so a different standard
# would be a real hazard, not a preference: draken's frozen structs and inline
# functions would be built under different language rules in the two consumers.
CXXSTD   ?= -std=c++20
OPT      ?= -O2
WARN     := -Wall -Wextra -Wno-unused-parameter
# draken/simd is draken's shared SIMD layer (simd_hash/simd_env/cpu_features/
# simd_dispatch — moved out of opteryx's src/cpp 2026-08-07). Four draken op
# headers include "simd_hash.h"; only declarations are needed, nothing links
# simd_hash.cpp. skene depends on draken alone.
INCLUDES := -Iinclude \
            -I$(DRAKEN_ROOT)/third_party/zstd \
            -I$(DRAKEN_ROOT)/third_party/lz4 \
            -I$(DRAKEN_ROOT)/draken \
            -I$(DRAKEN_ROOT)/draken/core \
            -I$(DRAKEN_ROOT)/draken/simd \
            -I$(DRAKEN_ROOT)/third_party/mabel/carchar \
            -I$(DRAKEN_ROOT)/third_party/cyan4973
# -MMD -MP makes the compiler emit a .d file listing every header each object
# actually included; the `-include` below feeds those back to make. Without it
# NO rule here depended on a header at all, so editing include/skene/format.h —
# or, worse, one of draken's headers outside this tree — rebuilt nothing.
#
# That is not a stale-comment nuisance, it is a layout mismatch: `VectorOwner`
# gained fields on 2026-08-09 while build/ still held objects from 2026-08-08.
# The library and the test binaries then disagreed about the struct's size, and
# six of eleven test binaries died in ~VectorOwner decrementing a refcount
# through a garbage pointer — a crash with nothing wrong in any source file,
# which is why it sat undiagnosed from 2026-08-11. `make clean` fixed it; this
# stops it coming back.
DEPFLAGS := -MMD -MP
CXXFLAGS := $(CXXSTD) $(OPT) $(WARN) $(INCLUDES) $(DEPFLAGS)

BUILD := build

# draken's lazily-grown global identity/zero selection vectors live in a .cpp,
# and each consuming binary is documented as owning its own copy of those
# globals (draken/core/vector_alloc.cpp header comment), so compiling it in is
# correct rather than a layering violation.
LIB_SRCS := src/checksum.cpp src/probe.cpp src/writer.cpp src/reader.cpp src/reader_v1.cpp src/value_order.cpp src/statistics.cpp src/encoding.cpp src/bloom.cpp src/file_io.cpp src/patch.cpp $(DRAKEN_ROOT)/draken/core/vector_alloc.cpp
LIB_OBJS := $(BUILD)/checksum.o $(BUILD)/probe.o $(BUILD)/writer.o $(BUILD)/reader.o $(BUILD)/reader_v1.o $(BUILD)/value_order.o $(BUILD)/statistics.o $(BUILD)/encoding.o $(BUILD)/bloom.o $(BUILD)/file_io.o $(BUILD)/patch.o $(BUILD)/draken_vector_alloc.o $(BUILD)/lz4.o

TEST_SRCS := $(wildcard tests/test_*.cpp)
TEST_BINS := $(patsubst tests/%.cpp,$(BUILD)/%,$(TEST_SRCS))

# ---------------------------------------------------------------------------
# Benchmark (opt-in) — links rugo's Parquet writer for comparison.
#
# NOT part of libskene.a, and deliberately a separate target: the rule is that
# neither LIBRARY imports the other, and this is a third-party harness holding
# both ends. `make` does not build it; `make bench` does.
#
# rugo's writer is header-only. zstd is vendored in opteryx-core and compiled in
# here so the production-realistic comparison (job results are zstd Parquet
# today) can be measured rather than guessed at.
# ---------------------------------------------------------------------------
# zstd is vendored in opteryx-core and compiled into libskene: per-section
# compression is part of the format now, not a benchmark-only extra.
ZSTD_DIR   := $(DRAKEN_ROOT)/third_party/zstd
ZSTD_SRCS  := $(wildcard $(ZSTD_DIR)/common/*.cpp) \
              $(wildcard $(ZSTD_DIR)/compress/*.cpp) \
              $(wildcard $(ZSTD_DIR)/decompress/*.cpp)
ZSTD_OBJS  := $(patsubst $(ZSTD_DIR)/%.cpp,$(BUILD)/zstd/%.o,$(ZSTD_SRCS))
# zstd's huf_decompress.cpp references BMI2 asm-loop symbols on x86-64
# (defined in huf_decompress_amd64.S); build_common.py does the same.
ifneq (,$(filter x86_64 amd64,$(shell uname -m)))
ZSTD_OBJS  += $(BUILD)/zstd/decompress/huf_decompress_amd64.o
endif

BENCH_INCLUDES := $(INCLUDES) \
                  -I$(DRAKEN_ROOT)/rugo/src \
                  -I$(DRAKEN_ROOT)/third_party/zstd
BENCH_FLAGS    := $(CXXSTD) $(OPT) $(BENCH_INCLUDES) -DHAVE_ZSTD

.PHONY: all test clean check-draken bench probe convert bakeoff fsst slots coverage bloomsz prune

all: check-draken $(BUILD)/libskene.a $(BUILD)/skene $(TEST_BINS)

# Fail loud at build time with an actionable message, rather than emitting a
# wall of "file not found" from the first include.
check-draken:
	@test -f "$(DRAKEN_ROOT)/draken/core/buffers.h" || { \
	  echo "ERROR: draken headers not found under DRAKEN_ROOT='$(DRAKEN_ROOT)'"; \
	  echo "       expected '$(DRAKEN_ROOT)/draken/core/buffers.h'"; \
	  echo "       run: make DRAKEN_ROOT=/path/to/opteryx-core"; exit 1; }
	@test -f "$(DRAKEN_ROOT)/third_party/cyan4973/xxhash.h" || { \
	  echo "ERROR: xxhash.h not found under DRAKEN_ROOT='$(DRAKEN_ROOT)'"; \
	  echo "       expected '$(DRAKEN_ROOT)/third_party/cyan4973/xxhash.h'"; \
	  echo "       (draken/core/string_slot.h includes it)"; exit 1; }

$(BUILD):
	@mkdir -p $(BUILD)

$(BUILD)/checksum.o: src/checksum.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD)/probe.o: src/probe.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD)/writer.o: src/writer.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -Isrc -c $< -o $@

$(BUILD)/reader.o: src/reader.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -Isrc -c $< -o $@

$(BUILD)/reader_v1.o: src/reader_v1.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -Isrc -c $< -o $@

$(BUILD)/value_order.o: src/value_order.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -Isrc -c $< -o $@

$(BUILD)/statistics.o: src/statistics.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -Isrc -c $< -o $@

$(BUILD)/file_io.o: src/file_io.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD)/encoding.o: src/encoding.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -Isrc -c $< -o $@

$(BUILD)/bloom.o: src/bloom.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -Isrc -c $< -o $@

$(BUILD)/patch.o: src/patch.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -Isrc -c $< -o $@

$(BUILD)/draken_vector_alloc.o: $(DRAKEN_ROOT)/draken/core/vector_alloc.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# lz4 is C, unlike the vendored zstd sources, which this tree carries renamed to
# .cpp. It is compiled with the C compiler rather than fed to $(CXX) as the
# bench targets do: lz4.h wraps its declarations in extern "C" either way, so the
# linkage matches, and this keeps the vendored source under the language it was
# written for. -w because it is vendored, not ours to warn about — same posture
# as the zstd rule below.
$(BUILD)/lz4.o: $(DRAKEN_ROOT)/third_party/lz4/lz4.c | $(BUILD)
	$(CC) $(OPT) -I$(DRAKEN_ROOT)/third_party/lz4 -w -c $< -o $@

$(BUILD)/libskene.a: $(LIB_OBJS) $(ZSTD_OBJS)
	@rm -f $@
	ar rcs $@ $(LIB_OBJS) $(ZSTD_OBJS)

$(BUILD)/skene: tools/skene_cli.cpp $(BUILD)/libskene.a
	$(CXX) $(CXXFLAGS) $< $(BUILD)/libskene.a -o $@

$(BUILD)/%: tests/%.cpp $(BUILD)/libskene.a
	$(CXX) $(CXXFLAGS) -Itests -Isrc $< $(BUILD)/libskene.a -o $@

test: all check-disjoint check-no-fuzz-flag
	@fail=0; \
	for t in $(TEST_BINS); do \
	  echo "== $$t"; \
	  $$t || fail=1; \
	done; \
	if [ $$fail -ne 0 ]; then echo "TESTS FAILED"; exit 1; fi; \
	echo "all tests passed"

bench: check-draken $(BUILD)/libskene.a $(ZSTD_OBJS)
	@test -f "$(DRAKEN_ROOT)/rugo/src/parquet/_parquet_writer.hpp" || { \
	  echo "ERROR: rugo's parquet writer not found under DRAKEN_ROOT='$(DRAKEN_ROOT)'"; \
	  exit 1; }
	$(CXX) $(BENCH_FLAGS) bench/bench_vs_parquet.cpp $(BUILD)/libskene.a \
	  -o $(BUILD)/bench_vs_parquet
	@echo "built $(BUILD)/bench_vs_parquet — run it with [rows] [repeats]"

# probe — what handoff form does rugo's decoder give each column of a Parquet
# file? Decided at decode time, so the footer cannot answer it.
# Excludes *_test.cpp: rle_direct_dict_test.cpp is a standalone driver with its
# own main() (see `make rle-dict-test`), not a library source — wildcarding it
# in here collides with this file's own main() at link time.
RUGO_SRCS := $(filter-out %_test.cpp,$(wildcard $(DRAKEN_ROOT)/rugo/src/parquet/*.cpp))

probe: check-draken $(ZSTD_OBJS)
	$(CXX) $(BENCH_FLAGS) -DRUGO_PARQUET_CREATED_BY='"skene-probe"' \
	  -I$(DRAKEN_ROOT)/third_party/bshoshany \
	  -I$(DRAKEN_ROOT)/third_party/snappy \
	  -I$(DRAKEN_ROOT)/third_party/lz4 \
	  -I$(DRAKEN_ROOT)/third_party/miniz \
	  bench/probe_parquet.cpp $(RUGO_SRCS) \
	  $(DRAKEN_ROOT)/third_party/snappy/snappy.cc \
	  $(DRAKEN_ROOT)/third_party/snappy/snappy-sinksource.cc \
	  $(DRAKEN_ROOT)/third_party/snappy/snappy-stubs-internal.cc \
	  $(DRAKEN_ROOT)/third_party/lz4/lz4.c \
	  $(DRAKEN_ROOT)/third_party/miniz/miniz_tinfl.cpp \
	  $(DRAKEN_ROOT)/draken/core/vector_alloc.cpp \
	  $(DRAKEN_ROOT)/draken/core/trace_bridge.cpp \
	  $(ZSTD_OBJS) -o $(BUILD)/probe_parquet
	@echo "built $(BUILD)/probe_parquet — run it with <file.parquet> [row_group]"

# convert — parquet -> .skene. Links rugo's reader AND opteryx's IPC
# deserializer: a third-party harness holding both, exactly like the bench.
# libskene.a itself stays disjoint (see check-disjoint).
convert: check-draken $(BUILD)/libskene.a $(ZSTD_OBJS)
	$(CXX) $(BENCH_FLAGS) -DRUGO_PARQUET_CREATED_BY='"skene-convert"' \
	  -I$(DRAKEN_ROOT)/third_party/bshoshany \
	  -I$(DRAKEN_ROOT)/third_party/snappy \
	  -I$(DRAKEN_ROOT)/third_party/lz4 \
	  -I$(DRAKEN_ROOT)/third_party/miniz \
	  -I$(DRAKEN_ROOT)/src/cpp \
	  bench/convert_parquet.cpp $(RUGO_SRCS) \
	  $(DRAKEN_ROOT)/src/cpp/ipc_deserialize.cpp \
	  $(DRAKEN_ROOT)/src/cpp/memory_pool.cpp \
	  $(DRAKEN_ROOT)/draken/core/trace_bridge.cpp \
	  $(DRAKEN_ROOT)/third_party/snappy/snappy.cc \
	  $(DRAKEN_ROOT)/third_party/snappy/snappy-sinksource.cc \
	  $(DRAKEN_ROOT)/third_party/snappy/snappy-stubs-internal.cc \
	  $(DRAKEN_ROOT)/third_party/lz4/lz4.c \
	  $(DRAKEN_ROOT)/third_party/miniz/miniz_tinfl.cpp \
	  $(BUILD)/libskene.a -o $(BUILD)/convert_parquet
	@echo "built $(BUILD)/convert_parquet — <in.parquet> <out-prefix> [max_row_groups]"

# bakeoff — which section kinds compress, with which codec, at what speed.
bakeoff: check-draken $(BUILD)/libskene.a
	$(CXX) $(BENCH_FLAGS) \
	  -I$(DRAKEN_ROOT)/third_party/lz4 \
	  -I$(DRAKEN_ROOT)/third_party/snappy \
	  bench/compression_bakeoff.cpp \
	  $(DRAKEN_ROOT)/third_party/lz4/lz4.c \
	  $(DRAKEN_ROOT)/third_party/snappy/snappy.cc \
	  $(DRAKEN_ROOT)/third_party/snappy/snappy-sinksource.cc \
	  $(DRAKEN_ROOT)/third_party/snappy/snappy-stubs-internal.cc \
	  $(BUILD)/libskene.a -o $(BUILD)/compression_bakeoff
	@echo "built $(BUILD)/compression_bakeoff — <raw.skene ...>"

# fsst — string-specific compression on the arena, against zstd-1 on the same
# bytes. Vendored under skene's OWN third_party rather than opteryx-core's: this
# work is speculative and must not add a dependency to that tree until it earns
# one. `-w` because FSST is vendored source, not ours to warn about.
FSST_DIR  := third_party/fsst
FSST_SRCS := $(FSST_DIR)/libfsst.cpp $(FSST_DIR)/fsst_avx512.cpp

fsst: check-draken $(BUILD)/libskene.a
	$(CXX) $(BENCH_FLAGS) -w -I$(FSST_DIR) \
	  bench/fsst_arena.cpp $(FSST_SRCS) \
	  $(BUILD)/libskene.a -o $(BUILD)/fsst_arena
	@echo "built $(BUILD)/fsst_arena — <file.skene ...>"

# slots — is the slot array laid out badly for a compressor?
slots: check-draken $(BUILD)/libskene.a
	$(CXX) $(BENCH_FLAGS) bench/slot_layout.cpp \
	  $(BUILD)/libskene.a -o $(BUILD)/slot_layout
	@echo "built $(BUILD)/slot_layout — <file.skene ...>"

# coverage — what fraction of columns can actually be pruned before decoding.
coverage: check-draken $(BUILD)/libskene.a
	$(CXX) $(BENCH_FLAGS) bench/index_coverage.cpp \
	  $(BUILD)/libskene.a -o $(BUILD)/index_coverage
	@echo "built $(BUILD)/index_coverage — [-v] <file.skene ...>"

# bloomsz — bloom filter cost against the accuracy it actually delivers.
bloomsz: check-draken $(BUILD)/libskene.a
	$(CXX) $(BENCH_FLAGS) -Isrc bench/bloom_sizing.cpp \
	  $(BUILD)/libskene.a -o $(BUILD)/bloom_sizing
	@echo "built $(BUILD)/bloom_sizing"

# prune — what the bloom adds over statistics and zone maps, by type.
prune: check-draken $(BUILD)/libskene.a
	$(CXX) $(BENCH_FLAGS) -Isrc bench/prune_value.cpp \
	  $(BUILD)/libskene.a -o $(BUILD)/prune_value
	@echo "built $(BUILD)/prune_value — <target.skene> <probe-source.skene>"

$(BUILD)/zstd/%.o: $(ZSTD_DIR)/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXSTD) $(OPT) -I$(ZSTD_DIR) -I$(ZSTD_DIR)/common -w -c $< -o $@

$(BUILD)/zstd/%.o: $(ZSTD_DIR)/%.S
	@mkdir -p $(dir $@)
	$(CXX) $(OPT) -I$(ZSTD_DIR) -I$(ZSTD_DIR)/common -c $< -o $@

# The library must never see a rugo header. This is the rule, checked rather
# than trusted, because a stray include would be invisible until the standalone
# wheel failed to build.
.PHONY: check-disjoint
# Matches real dependencies — an #include of a rugo header, or use of its
# namespace — not the word itself, which comments legitimately use to explain
# WHY the two are kept apart.
check-disjoint:
	@if grep -rnE '#include[[:space:]]*[<"][^">]*rugo|rugo_pq_write|rugo::' \
	     src/ include/ 2>/dev/null; then \
	  echo "ERROR: libskene sources depend on rugo; the two must stay disjoint"; \
	  exit 1; \
	fi
	@echo "ok: libskene has no rugo dependency"

# SKENE_FUZZING_SKIP_CHECKSUM makes the reader ACCEPT files whose checksums do
# not match (see include/skene/checksum.h). It exists so the native fuzzer can
# reach past the integrity layer, and it must never reach a shippable artifact.
# Checked rather than trusted, exactly like check-disjoint: a stray -D in a
# build definition would be invisible until a corrupt file was read in anger.
.PHONY: check-no-fuzz-flag
# Matches the ways the macro can actually be DEFINED (-D on a command line, or
# a define_macros entry in setuptools), not the bare name, which the comment
# above legitimately uses. The bracket in the pattern keeps this rule from
# matching its own text.
check-no-fuzz-flag:
	@if grep -nE '(-D|["'"'"'])SKENE_FUZZING[_]SKIP_CHECKSUM' \
	     Makefile setup.py pyproject.toml \
	     $(DRAKEN_ROOT)/setup.py $(DRAKEN_ROOT)/build_common.py \
	     $(DRAKEN_ROOT)/rugo/setup.py 2>/dev/null; then \
	  echo "ERROR: the fuzzing checksum-skip flag is set in a build that ships"; \
	  exit 1; \
	fi
	@echo "ok: no checksum-skip flag in any shipping build"

clean:
	rm -rf $(BUILD)

# Header dependencies emitted by -MMD, for every object AND every test binary.
# Wildcarded and `-include`d so a missing .d (first build, or after clean) is
# not an error. Kept at the end so it cannot shadow a real rule.
-include $(shell find $(BUILD) -name '*.d' 2>/dev/null)
