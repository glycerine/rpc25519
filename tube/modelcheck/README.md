# Tube model checking

Run `make -C tube tla` from the repository root to check the Tube model and
replay the archived traces. The target runs offline with Java 11 or later,
Python 3, Make, Bash and gzip. See the [Tube model and runner guide](tube/README.md)
for individual checks and instructions for replaying new traces.

## Bundled JARs

| File | Used for | Upstream release |
| --- | --- | --- |
| `tla2tools.jar` | Parsing TLA+ and running TLC | [TLA+ tools v1.8.0](https://github.com/tlaplus/tlaplus/releases/tag/v1.8.0), build `2026.11.19.180144` at [revision bc1b506](https://github.com/tlaplus/tlaplus/tree/bc1b506bdcad80bb518bf3776a9161c34ff0fc4c) |
| `CommunityModules-deps.jar` | Reading the Go traces with the JSON and I/O modules | [Community Modules 202505152026](https://github.com/tlaplus/CommunityModules/releases/tag/202505152026) |

Both files are unmodified upstream release artifacts. From this directory,
verify them against the pinned [SHA-256 checksums](SHA256SUMS) with

```sh
sha256sum -c SHA256SUMS
```

The [third-party notices](THIRD-PARTY-NOTICES.txt) accompany the JARs.

The other Raft models in this directory are collected references.
