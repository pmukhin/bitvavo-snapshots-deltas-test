# Bitvavo Snapshots & Deltas Test

This project demonstrates how to retrieve, apply, and verify order book snapshots and incremental updates ("deltas")
from the [Bitvavo](https://bitvavo.com) cryptocurrency exchange.

It connects to the Bitvavo WebSocket API, collects a snapshot from the REST API, listens for updates over WebSocket, and
applies updates to validate the consistency of the order book data.

## Features

- Fetch initial order book snapshots via HTTP
- Subscribe to live book updates via WebSocket
- Trim trailing zeroes from price and size values
- Merge incremental updates with the original snapshot
- Display order book data side-by-side in CLI tables
- Uses `BigDecimal` for precise numerical operations

## Tech Stack

- Language: Rust
- Async Runtime: [`tokio`](https://crates.io/crates/tokio)
- WebSocket: [`tokio-tungstenite`](https://crates.io/crates/tokio-tungstenite)
- HTTP Client: [`reqwest`](https://crates.io/crates/reqwest)
- CLI Table Output: [`prettytable-rs`](https://crates.io/crates/prettytable-rs)
- Decimal Handling: [`bigdecimal`](https://crates.io/crates/bigdecimal)
- CLI Args: [`clap`](https://crates.io/crates/clap)

## Getting Started

### Prerequisites

- Rust (version >= 1.74)
- Internet connection to reach Bitvavo API

### Install

Clone the repository:

```bash
git clone https://github.com/pmukhin/bitvavo-snapshots-deltas-test.git
cd bitvavo-snapshots-deltas-test
```

Build the project:

```bash
cargo build --release
```

### Run

```bash
cargo run --release -- \
    --market AAVE-EUR \
    --api-url https://api.bitvavo.com \
    --ws-url wss://ws.bitvavo.com \
    --depth 5 \
    --num-snapshots 10
```

Arguments:

| Flag              | Description                            | Example                   |
|-------------------|----------------------------------------|---------------------------|
| `--market`        | Trading pair to subscribe to           | `AAVE-EUR`                |
| `--api-url`       | Bitvavo REST API base URL              | `https://api.bitvavo.com` |
| `--ws-url`        | Bitvavo WebSocket base URL             | `wss://ws.bitvavo.com`    |
| `--depth`         | Order book depth to fetch              | `5`                       |
| `--num-snapshots` | Number of updates to apply before exit | `10`                      |

### Example Output

```
+---------------------+--------------------------+------------------------------------------------+
| Price level index   | Price level | Received snapshot bids | Snapshot bids with applied updates |
+---------------------+--------------------------+------------------------------------------------+
| 0                   | 5.5         | 99.99                  | 99.99                              |
| 1                   | 5.5         | 99.98                  | 99.98                              |
...
```

## Testing

```bash
cargo test
```

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

> Built by [pmukhin](https://github.com/pmukhin) to explore real-time order book integrity.
