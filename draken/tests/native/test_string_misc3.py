"""
Native correctness tests for Milestone E.17: vector_soundex — pure nanobind C++.

Coverage:
  vector_soundex:
    classic fixtures: Robert/Rupert → R163, Rubin → R150
    all-uppercase input
    all-lowercase input
    mixed case
    H/W do not collapse adjacent codes (separator-only)
    adjacent duplicate codes collapsed
    short input (single letter) → padded with zeros
    single letter → letter + '000'
    empty string → null output
    null input row → null output row
    batch: multiple rows
    non-Vector input → TypeError
"""

import importlib.util
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
import draken.draken_native as dn


def _load_module(name, rel_path):
    base = os.path.join(os.path.dirname(__file__), "..", "..", "..", rel_path)
    import glob
    candidates = glob.glob(base + "*.so") + glob.glob(base + "*.pyd")
    if not candidates:
        raise FileNotFoundError(f"Compiled module not found: {base}*.so")
    spec = importlib.util.spec_from_file_location(name, candidates[0])
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_misc3 = _load_module(
    "vector_string_misc3",
    "opteryx/compiled/nanobind/vector_string_misc3.cpython",
)
vector_soundex = _misc3.vector_soundex


def make(lst):
    return dn.vector_from_string_sequence(lst)


class TestVectsqlundex:
    # --- Classic algorithm fixtures ---

    def test_robert(self):
        assert vector_soundex(make(["Robert"])).to_pylist() == ["R163"]

    def test_rupert(self):
        # R-163: R(R), P(1 → skipped? no — R=6, u=skip(vowel resets), P=1, e=reset, R=6, t=3
        # Per American Soundex: Rupert → R163
        assert vector_soundex(make(["Rupert"])).to_pylist() == ["R163"]

    def test_rubin(self):
        assert vector_soundex(make(["Rubin"])).to_pylist() == ["R150"]

    def test_ashcraft(self):
        # A-261: A(A), s=2, h=sep(no reset), c=2(dup→skip), r=6, a=reset, f=1, t=3
        # American Soundex: Ashcraft → A261
        assert vector_soundex(make(["Ashcraft"])).to_pylist() == ["A261"]

    def test_euler(self):
        # E-460: E(E), u=reset, l=4, e=reset, r=6, 0=pad
        assert vector_soundex(make(["Euler"])).to_pylist() == ["E460"]

    def test_ellery(self):
        # E-460: E(E), l=4(dup next l skip), e=reset, r=6, y=reset, 0=pad
        assert vector_soundex(make(["Ellery"])).to_pylist() == ["E460"]

    # --- Case handling ---

    def test_uppercase_input(self):
        assert vector_soundex(make(["ROBERT"])).to_pylist() == ["R163"]

    def test_lowercase_input(self):
        assert vector_soundex(make(["robert"])).to_pylist() == ["R163"]

    def test_mixed_case(self):
        assert vector_soundex(make(["RoBerT"])).to_pylist() == ["R163"]

    # --- Single letter ---

    def test_single_letter(self):
        # "A" → first letter A, no more chars → pad to "A000"
        assert vector_soundex(make(["A"])).to_pylist() == ["A000"]

    def test_single_vowel(self):
        assert vector_soundex(make(["E"])).to_pylist() == ["E000"]

    # --- Empty / null ---

    def test_empty_string_is_null(self):
        result = vector_soundex(make([""])).to_pylist()
        assert result[0] is None

    def test_null_input_is_null_output(self):
        result = vector_soundex(make([None])).to_pylist()
        assert result[0] is None

    def test_null_among_valid(self):
        result = vector_soundex(make([None, "Robert", None])).to_pylist()
        assert result[0] is None
        assert result[1] == "R163"
        assert result[2] is None

    def test_all_null(self):
        result = vector_soundex(make([None, None])).to_pylist()
        assert result == [None, None]

    # --- Output properties ---

    def test_output_length_always_four(self):
        rows = ["Robert", "A", "X", "Smith"]
        result = vector_soundex(make(rows)).to_pylist()
        for code in result:
            assert len(code) == 4

    def test_output_type_is_varchar(self):
        result = vector_soundex(make(["Robert"]))
        assert "VARCHAR" in str(result.type)

    # --- Duplicate code collapsing ---

    def test_adjacent_duplicates_collapsed(self):
        # "Pfister": P(P), f=1, i=reset, s=2, t=3, e=reset, r=6 → P236
        # (f and v share code 1; p is first letter; f=1 then i resets)
        # Simpler: "BB" → B(B), second B has code 1 same as prev → B000
        result = vector_soundex(make(["BB"])).to_pylist()
        assert result[0] == "B000"

    def test_hw_separator_no_reset(self):
        # Per algorithm: H and W do NOT reset prev_code.
        # "Ashcraft": A-s=2, h=sep(no reset), c=2(dup→skip) → second digit uses r=6
        assert vector_soundex(make(["Ashcraft"])).to_pylist() == ["A261"]

    # --- Batch ---

    def test_batch_multiple_rows(self):
        names = ["Robert", "Rupert", "Rubin", None, ""]
        result = vector_soundex(make(names)).to_pylist()
        assert result[0] == "R163"
        assert result[1] == "R163"
        assert result[2] == "R150"
        assert result[3] is None
        assert result[4] is None

    # --- Error handling ---

    def test_non_vector_raises(self):
        with pytest.raises(TypeError):
            vector_soundex("not_a_vector")

    def test_empty_vector(self):
        result = vector_soundex(make([])).to_pylist()
        assert result == []
