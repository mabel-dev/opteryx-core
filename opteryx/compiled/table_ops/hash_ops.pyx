# cython: language_level=3
# This module has been intentionally removed.
#
# Background:
# Historically `opteryx.compiled.table_ops.hash_ops` provided a buffer-aware
# implementation of `compute_row_hashes(...)` operating over PyArrow tables.
# The project has migrated to Draken-native hashing: callers should now
# compute per-row hashes from a `Morsel` via `Morsel.hash(...)` (fast, nogil
# where possible) and use `opteryx.compiled.table_ops.null_avoidant_ops`
# helpers for non-null index extraction when required.
#
# To preserve the "fail fast" policy and avoid silent fallbacks, importing
# this module will raise an ImportError explaining the migration path.
#
# If you intentionally need the old Arrow-buffer hashing for some reason,
# please discuss the use-case with the architecture owners before reintroducing
# an implementation — we prefer using `Morsel.hash` and converting Arrow→Morsel
# at operator boundaries where necessary.

raise ImportError(
    "opteryx.compiled.table_ops.hash_ops has been removed.\n\n"
    "Please compute per-row hashes using the Draken Morsel API instead:\n\n"
    "  from opteryx.compiled.draken.morsels.morsel cimport Morsel\n\n"
    "  # if you already have a Morsel:\n"
    "  hashes = morsel.hash(columns)\n\n"
    "  # if you have a PyArrow table, convert to a Morsel first:\n"
    "  morsel = Morsel.from_arrow(pyarrow_table)\n"
    "  hashes = morsel.hash(columns)\n\n"
    "For non-null index extraction use:\n"
    "  from opteryx.compiled.table_ops.null_avoidant_ops cimport non_null_row_indices\n\n"
    "If you believe a small compatibility shim is required for a specific\n"
    "call-site, migrate the call-site to use Morsel.hash(...) or open a\n"
    "discussion so we can design the correct Draken-native replacement."
)
