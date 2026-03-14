## Vendored Source

This directory vendors the upstream `RedisBloom/t-digest-c` implementation.

- Upstream: `https://github.com/RedisBloom/t-digest-c`
- Pinned commit: `50edef336eb27ed5b19e7f9be05494683ca58515`
- License: MIT, see [`LICENSE.md`](LICENSE.md)

Vendored files:

- `src/tdigest.c`
- `src/tdigest.h`
- `src/td_malloc.h`
- `UPSTREAM_README.md`

Only the production source/header files and license/readme metadata are vendored.
Examples, tests, CI config, and build-system files are intentionally omitted.
