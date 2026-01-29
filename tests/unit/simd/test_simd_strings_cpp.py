import pytest


@pytest.fixture
def strings_module():
    try:
        from opteryx.compiled import simd_strings
        return simd_strings
    except ImportError:
        pytest.skip("C++ simd_strings module not available")


class TestSIMDStringsCPP:
    def test_search_basic(self, strings_module):
        data = b"hello world"
        result = strings_module.find_char(data, ord('o'))
        assert result == 4

    def test_count_basic(self, strings_module):
        data = b"hello world"
        result = strings_module.count_char(data, ord('l'))
        assert result == 3

    def test_find_all_basic(self, strings_module):
        data = b"hello world"
        result = strings_module.find_all_char(data, ord('l'))
        assert result == [2, 3, 9]

    def test_to_upper_lower(self, strings_module):
        data = b"Hello World!"
        up = strings_module.to_upper(data)
        down = strings_module.to_lower(data)
        assert up == b"HELLO WORLD!"
        assert down == b"hello world!"