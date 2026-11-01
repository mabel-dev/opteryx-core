# grpc_alts — vendored ALTS record layer

The AES-128-GCM rekeying crypter and the ALTS iovec record protocol from
[grpc/grpc](https://github.com/grpc/grpc), used by the DirectPath GCS reader
(`src/cpp/gcs_grpc/alts.cpp`, design `docs/GCS_DIRECTPATH_READS_DESIGN.md`,
ruling R1).

- Upstream commit: `df3dd4dcb1409c826341a0df6eb808f741e58dea` (2026-10-30)
- License: Apache-2.0 (`LICENSE`, unchanged)

| File | Upstream path |
|---|---|
| `gsec.h`, `gsec.cc`, `aes_gcm.cc` | `src/core/tsi/alts/crypt/` |
| `alts_counter.h`, `alts_counter.cc` | `src/core/tsi/alts/frame_protector/` |
| `alts_iovec_record_protocol.h`, `alts_iovec_record_protocol.cc` | `src/core/tsi/alts/zero_copy_frame_protector/` |

## Local changes

They are mechanical only. The crypto code is unchanged.

- Includes of grpc, gpr and absl headers are replaced by `grpc_alts_shim.h`, which provides:
  - `grpc_status_code`, with the four values these files use;
  - `gpr_malloc`/`gpr_zalloc`/`gpr_free` (abort on OOM, as gpr does);
  - a minimal `Span`;
  - `struct iovec` from `<sys/uio.h>`.
- Includes of other vendored files now use the file's own name, with no `src/core/...` path.
- `absl::Span`/`absl::MakeSpan` become `grpc_alts::Span`/`grpc_alts::MakeSpan`.
- `namespace grpc_core` becomes `namespace grpc_alts`, so no symbol can collide with a real gRPC loaded in the same process.
- The unused `#include "src/core/util/crash.h"` is removed.

To refresh, copy the files above from the new commit and repeat the same edits.
