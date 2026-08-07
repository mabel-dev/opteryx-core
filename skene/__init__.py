"""skene — Opteryx's tuned columnar file format.

One `.skene` file is one row group of draken vectors, serialized losslessly:
the DrakenType, LogicalType descriptor, flags, and dictionary selection all
round-trip exactly. Parquet remains the interchange format; skene is for
query results, sort spill, and optimised datasets.

Format spec: skene/FORMAT.md. The native core is C++ (skene/src); this
package is the Python boundary only.
"""

# draken must be imported first: it loads draken_native under RTLD_GLOBAL,
# which skene_native's draken-header inlines rely on at symbol resolution.
import draken  # noqa: F401

from skene.skene_native import SkeneError
from skene.skene_native import footer_extent
from skene.skene_native import probe_version
from skene.skene_native import read_metadata
from skene.skene_native import read_morsel
from skene.skene_native import write_morsel

__all__ = [
    "SkeneError",
    "footer_extent",
    "probe_version",
    "read_metadata",
    "read_morsel",
    "write_morsel",
]
