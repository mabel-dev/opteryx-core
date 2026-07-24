# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import platform
from itertools import permutations
from typing import Iterable, Optional

from opteryx.compiled.functions.strings import count_instances
from opteryx.third_party.mbleven import compare
from opteryx.utils._sql_utils import (
    lru_cache_with_expiry,
    random_int,
    random_string,
    single_item_cache,
)

__all__ = [
    "lru_cache_with_expiry",
    "random_int",
    "random_string",
    "single_item_cache",
    "suggest_alternative",
    "Timer",
    "is_windows",
]


@single_item_cache
def is_windows() -> bool:
    return platform.system().lower() == "windows"


# Rearranging is factorial in the number of parts; beyond this we only try the
# name as written. 5 parts is 120 orderings, 6 would be 720.
MAX_REARRANGE_PARTS: int = 5


def _split_name(value: str) -> list:
    """
    Split a name into its parts, breaking on non-alphanumeric characters and on
    camel case boundaries.

    'DATE_FORMAT' -> ['DATE', 'FORMAT']
    'BigLittle'   -> ['Big', 'Little']
    'HTTPServer'  -> ['HTTP', 'Server']
    """
    parts = []
    for token in ("".join(ch if ch.isalnum() else " " for ch in value)).split():
        start = 0
        for index in range(1, len(token)):
            previous = token[index - 1]
            current = token[index]
            if not current.isupper():
                continue
            # 'gL' in 'BigLittle', or the 'PS' in 'HTTPServer' (upper run followed
            # by a lower - the last upper starts the next word)
            if not previous.isupper() or (index + 1 < len(token) and token[index + 1].islower()):
                parts.append(token[start:index])
                start = index
        parts.append(token[start:])
    return [part for part in parts if part]


def suggest_alternative(value: str, candidates: Iterable[str]) -> Optional[str]:
    """
    Find closest match using a variation of Levenshtein Distance with additional
    handling for rearranging function name parts.

    This implementation:
    - Is limited to searching for distance less than three.
    - Is case insensitive and ignores non-alphanumeric characters.
    - Tries rearranging parts of the name if an exact or close match is not found
      in its original form. Parts are identified by non-alphanumeric separators
      ('DATE_FORMAT') and by camel case boundaries ('BigLittle').

    This function is designed for quickly identifying likely matches when a user
    is entering field or function names and may have minor typos, casing or
    punctuation mismatches, or even jumbled parts of the name.

    Parameters:
        value: str
            The value to find matches for.
        candidates: List[str]
            A list of candidate names to match against.

    Returns:
        Optional[str]: The best match found, or None if no match is found.
    """
    name = "".join(char for char in value if char.isalnum())
    best_match_column = None
    best_match_score = 100  # Large number indicates no match found yet.

    # Function to find the best match based on levenstein distance
    def find_best_match(name: str):
        nonlocal best_match_column, best_match_score
        for raw, candidate in ((ca, "".join(ch for ch in ca if ch.isalnum())) for ca in candidates):
            my_dist = compare(candidate.lower(), name.lower())
            if my_dist == 0:  # if we find an exact match, return that immediately
                return raw
            if 0 <= my_dist < best_match_score:
                best_match_score = my_dist
                best_match_column = raw

    # First, try to find a match with the original name
    result = find_best_match(name)
    if result:
        return result

    # If no match was found, try rearranging the parts of the name. Candidates are
    # compared with their non-alphanumeric characters removed, so the parts are
    # joined without a separator.
    parts = _split_name(value)
    if 1 < len(parts) <= MAX_REARRANGE_PARTS:
        original = tuple(parts)
        for combination in permutations(parts):
            if combination == original:  # already tried, as `name`
                continue
            result = find_best_match("".join(combination))
            if result:
                return result

    return best_match_column  # Return the best match found, or None if no suitable match is found.


class Timer:
    ticks: int = 0
    label: str = None

    def __init__(self, label: str = None):
        self.label = label

    def __del__(self):
        if self.label:
            print(f"Timer ({self.label}): {self.ticks / 1e9}")
        else:
            print(f"Timer: {self.ticks / 1e9}")
