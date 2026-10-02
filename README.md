# dotk-indexer

The indexer of [dotk.name](https://dotk.name), the `.k` name registry on Kaspa. It follows the
virtual chain into Postgres and serves the registry over HTTP. Its self-test proves the served
registry against the node's UTXO set, and each answer carries what a reader needs to check it
against the chain. It also evicts expired registrations for their bounty.

The deployments of mainnet and testnet-10 are built in. `--network` selects one, and the default
is mainnet.

## Run with Docker Compose

`docker-compose.yaml` runs the indexer beside a Postgres. Put your node's wRPC URL in `--rpc-url`
and your Kaspa address in `--evictor-address`, then start both:

```bash
docker compose up -d
```

Open <http://localhost:7799/> for the status page. The API reference and the OpenAPI document are
under `/v1`.

An empty database bootstraps itself from the published snapshot of its network. The indexer proves
every row of it against the node before it reports healthy. If the bootstrap fails, the indexer
exits, and the next start tries again. `--snapshot-url none` starts from an empty registry instead.
A `snapshot.json` in the working directory (`--snapshot-file`) wins over any `--snapshot-url`, and a
corrupt one must be removed by hand. The image's working directory is `/data`. Mount a directory
writable by uid 13337 there, not a single file, so the indexer can delete the snapshot after the
import.

`--rpc-url` names the node, which must run with `--utxoindex`, for example
`--rpc-url=ws://<host>:17110`. The value `resolver` picks a public node, but those public nodes are
often overloaded.

With `--evictor-address`, the evictor clears expired registrations, but their bounty is unsigned
and anyone can take it. A key in its place funds and signs every evict, so the key's address
collects the bounty. Pass it as the `DOTK_EVICTOR_KEY` environment variable (64 hex characters),
because a `--evictor-key` argument is readable by every local user in the process list. Without
either flag the evictor is off.

## Run a release binary

Each [release](https://github.com/supertypo/dotk-indexer/releases) carries gzipped Linux binaries
for amd64 and arm64, which need glibc 2.35 or newer.

## Build and run from source

It needs [rustup](https://rustup.rs), which installs the Rust version that `rust-toolchain.toml`
names, and a Postgres.

```bash
cargo build --release
./target/release/dotk-indexer \
  --rpc-url ws://<host>:17110 \
  --database-url postgres://postgres:postgres@localhost:5432/postgres \
  --evictor-address kaspa:<your address>
```

`--help` lists the flags an operator sets. A few hidden timing flags are in `src/config.rs`. The
tests start their own Postgres through testcontainers, so they need Docker. They run against a
simulated node.

```bash
cargo test
```

## License

AGPL-3.0-only, with the additional terms in [NOTICE](NOTICE). The fonts in `src/fonts` are under the SIL
Open Font License 1.1, in [src/fonts/OFL.txt](src/fonts/OFL.txt).
