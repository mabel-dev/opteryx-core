import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest
import jellyfish

import draken.draken_native as dn
from opteryx.compiled.nanobind.vectors3 import vector_soundex

# Test cases for soundex algorithm - these are just the input names
# We'll compare against jellyfish (reference implementation) rather than hardcoded values
TEST_NAMES = [
    'Test',
    'Therkelsen',
    'Troccoli',
    'Zelenski',
    'Zielonka',
    'Smith',
    'Johnson',
    'Williams',
    'Jones',
    'Brown',
    'Davis',
    'Miller',
    'Wilson',
    'Moore',
    'Taylor',
    'Anderson',
    'Thomas',
    'Jackson',
    'White',
    'Harris',
    'Martin',
    'Thompson',
    'Garcia',
    'Martinez',
    'Robinson',
    'Xi',
    'Lee',
    'Zz',
    'Kkk',
    'Aa',
    'Mmmmm',
    'O\'Neil',
    'Van der Sar',
    'St. John',
    'D\'Amico',
    'McDonald',
    'de la Cruz',
    'O\'Connor',
    'Von Trapp',
    'Al',
    'Bo',
    'Cy',
    'Du',
    'Ek',
    '',
    'Washington',
    'Jefferson',
    'Lincoln',
    'Roosevelt',
    'Kennedy',
    'Reagan',
    'Bush',
    'Clinton',
    'Obama',
    'Trump',
    'Biden',
    'Harrison',
    'Cleveland',
    'McKinley',
    'Coolidge',
    'Hoover',
    'Truman',
    'Eisenhower',
    'Nixon',
    'Ford',
    'Carter',
    'Adams',
    'Madison',
    'Monroe',
    'Jackson',
    'Polk',
    'Taylor',
    'Fillmore',
    'Pierce',
    'Buchanan',
    'Grant',
    'Hayes',
    'Garfield',
    'Arthur',
    'Taft',
    'Harding',
    # additional edge cases: short names
    'A',
    'B',
    'I',
    'Z',
    # additional edge cases: names with hyphens
    'Smith-Jones',
    'Mary-Ann',
    'Jean-Luc',
    # additional edge cases: names with apostrophes  
    'O\'Reilly',
    'D\'Angelo',
    'L\'Enfant',
    # additional edge cases: double letters
    'Phillip',
    'Matthew',
    'Lloyds',
    'Becker',
    # additional edge cases: silent letters
    'Knight',
    'Wright',
    'Knuth',
    'Pneumonia',  # (name used as test)
    # additional edge cases: names starting with vowels
    'Ashcroft',
    'Ellsworth',
    'Ingram',
    'Underwood',
    # additional edge cases: repeated consonants
    'Bennett',
    'Garrett',
    'Harriett',
    'Jarrett',
    # additional edge cases: common international names
    'Singh',
    'Zhang',
    'Nguyen',
    'Schmidt',
    'Mueller',
    'Kowalski',
    # additional edge cases: Welsh names
    'Llewellyn',
    'Cadwallader',
    'Rhys',
    # additional edge cases: Irish names
    'McCarthy',
    'Gallagher',
    'Sullivan',
    # additional edge cases: Scottish names
    'MacGregor',
    'MacDonald',
    'Campbell',
    # additional edge cases: names with special patterns
    'Schwarzenegger',
    'Tchotchke',
    'Pfeiffer',
    'Czajkowski',
]


@pytest.mark.parametrize("input_name", TEST_NAMES)
def test_soundex_against_reference(input_name):
    """Test that native vector_soundex matches the jellyfish reference implementation."""
    expected = None if input_name == "" else jellyfish.soundex(input_name)
    actual = vector_soundex(_build_sv(input_name)).to_pylist()[0]
    assert actual == expected, f"for '{input_name}' - expected: '{expected}', got: '{actual}'"


def _build_sv(*values):
    """Build a Draken string vector from Python strings/bytes/None."""
    # vector_from_string_sequence is bytes-only — encode str inputs to bytes.
    return dn.vector_from_string_sequence(
        [v.encode("utf-8") if isinstance(v, str) else v for v in values]
    )


def _null_mask(sv):
    """Return a list of bools: True = valid (non-null), False = null."""
    return [sv[i] is not None for i in range(len(sv))]


def test_soundex_vector_all_nonempty_no_nulls():
    """All non-empty inputs → output null_count must be zero."""
    sv = _build_sv(b"Smith", b"Johnson", b"Williams")
    result = vector_soundex(sv)
    assert result.to_pylist() == ["S530", "J525", "W452"]


def test_soundex_vector_empty_input_produces_null():
    """A row with empty-bytes input among non-empty rows → that row null, others non-null."""
    sv = _build_sv(b"Smith", b"", b"Williams")
    result = vector_soundex(sv)
    mask = _null_mask(result)
    assert mask[0] is True,  "Smith should be non-null"
    assert mask[1] is False, "empty string should produce null"
    assert mask[2] is True,  "Williams should be non-null"


def test_soundex_vector_null_input_stays_null():
    """A row that was already null in the input → output row is null."""
    sv = _build_sv(b"Smith", None, b"Williams")
    result = vector_soundex(sv)
    mask = _null_mask(result)
    assert mask[0] is True,  "Smith should be non-null"
    assert mask[1] is False, "null input should stay null"
    assert mask[2] is True,  "Williams should be non-null"


def test_soundex_vector_mixed_null_empty_nonempty():
    """Mixed: null-in-input, empty-input, non-empty-input → first two null, last non-null."""
    sv = _build_sv(None, b"", b"Jones")
    result = vector_soundex(sv)
    mask = _null_mask(result)
    assert mask[0] is False, "null-input row should be null"
    assert mask[1] is False, "empty-input row should be null"
    assert mask[2] is True,  "Jones should be non-null"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(TEST_NAMES)} TESTS")
    failed_count = 0
    
    for test_name in TEST_NAMES:
        try:
            test_soundex_against_reference(test_name)
            print("\033[38;2;26;185;67m.\033[0m", end="\n")
        except AssertionError as e:
            print(f"Test failed for {test_name} with error: {e}")
            failed_count += 1

    print()
    if failed_count == 0:
        print("✅ All tests passed!")
    else:
        print(f"❌ {failed_count} tests failed")
