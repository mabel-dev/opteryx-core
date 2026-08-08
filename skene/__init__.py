"""skene — Opteryx's tuned columnar file format.

A `.skene` file holds one or more row groups of draken vectors, serialized
losslessly: the DrakenType, LogicalType descriptor, flags, and dictionary
selection all round-trip exactly. Parquet remains the interchange format;
skene is for query results, sort spill, and optimised datasets.

Reads are per row group and the row group index is always explicit —
`read_morsel(buf, row_group)`. Pruning goes through `read_metadata()`, which
parses only the small file-level index (schema, row group directory, and every
row group's per-column statistics) and opens no row group footer;
`read_row_group_metadata()` is the per-row-group detail and costs one.

Format spec: skene/FORMAT.md. The native core is C++ (skene/src); this
package is the Python boundary only.
"""

# draken must be imported first: it loads draken_native under RTLD_GLOBAL,
# which skene_native's draken-header inlines rely on at symbol resolution.
import draken  # noqa: F401

from skene.__version__ import __version__
from skene.skene_native import SkeneError
from skene.skene_native import SkeneWriter
from skene.skene_native import footer_extent
from skene.skene_native import probe_version
from skene.skene_native import read_metadata
from skene.skene_native import read_morsel
from skene.skene_native import read_row_group_metadata
from skene.skene_native import write_morsel

__all__ = [
    "SkeneError",
    "SkeneWriter",
    "__version__",
    "footer_extent",
    "probe_version",
    "read_metadata",
    "read_morsel",
    "read_row_group_metadata",
    "write_morsel",
]
