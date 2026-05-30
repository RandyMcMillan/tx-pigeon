# tx-pigeon 🐦

Send a little pigeon out to all the **libre-relay**  nodes and poop on them with whatever transaction you like. 

in the name of censorship resistance money 

- If your tx is accepted in a block, GetData(inv(tx)) returns the tx from the lastest block, so that will make garbage man nodes apear as normal libre relay nodes.

## Library

The core tx blasting logic is now exposed as a Rust library:

```rust
let delivered = tx_pigeon::blast_transaction_hex(tx_hex).await?;
```

## CLI

The binary now exposes the CLI in `src/cli.rs` and supports these modes:

```bash
# Blast a transaction hex
cargo run -- --tx <hex>

# Run the topic network
cargo run --bin tx-pigeon -- topic

# Fetch peer transactions
cargo run --bin tx-pigeon -- fetch --limit 20

# Relay peer transactions in a loop
cargo run --bin tx-pigeon -- relay --limit 20 --interval-secs 15

# Watch gossip activity with a libp2p protocol prefix
cargo run --bin tx-pigeon -- gossip --label gossip-client --local --remote --protocol /gnostr

# Or pin a protocol prefix and version explicitly
cargo run --bin tx-pigeon -- gossip --label gossip-client --protocol /gnostr --protocol-version 1.0.0
```

Gossip mode defaults to both `--local` and `--remote` when neither flag is set:

- `--local` prints local mempool polling and local transaction summaries
- `--remote` prints gossipsub / hole-punch transaction summaries
- Default protocol is `gnostr/0.0.1` when no flags are passed
- `--protocol /gnostr` sets a libp2p protocol prefix
- `--protocol-version 0.0.1` (or any suffix) appends to `--protocol` as a full libp2p protocol id

The live relay runtime now mirrors the test swarm more closely: multiple relay workers run concurrently and rebroadcast independently instead of a single serialized loop.

Relay workers still exit on `ctrl_c` and log `stopping p2p relay loop` before breaking out of the cycle.


## Setup

```bash
# 1. Install the Rust toolchain (if you haven’t already)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 2. Clone & build the project
git clone https://github.com/stutxo/tx-pigeon.git
cd tx-pigeon

# 3. Fire away 🐦💩
cargo run -- --tx \
  020000000001019d8c84a78cb5e032c20ce46868a64c0a2f88090f790ab57320b53804484c7a31 \
  0000000000fdffffff026517000000000000225120ead5bf5032f0564c3f3689d1057c4895311390d11500b94510f5a0fb9f2ba98 \
  7000000000000000fd94016a4d9001f09f92a9f09f92a9f09f92a9f09f92a9f09f92a9f09f92a9f09f92a9f09f92a9f09f92a9f09f92a9f09 \
  f92a9f09f92a9f09f92a9f09f92a9f09f92a9f09f92a9f09f92a9f09f92a9f09f92a9f09f92a9f09f92a9f09f92a9f09f92a9f09f92a9f09 \
  f92a9f09f92a90140f38ed8b347a8ec4a5a32ea85d9283a54dc0d18d3298cd1ddb98c6f2ceef24ed6f6b19fab372aafc30185941ac5558f262fe13b3680ff01df3ebe7f37ab0f3de700000000
```

Heres one i relayed earlier: 

https://mempool.space/tx/449653d50f4a9fad160e4eb10c3c4e6d3013a516e871187df6f1017fda58abe4

![alt text](image.png)