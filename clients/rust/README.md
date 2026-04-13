# Chronos Rust Client

`Client` is the Rust application-facing Chronos client implementation.

Support level: Primary.

## Public shape

- `let client = Client::connect(addr, timeline_key).await?`
- `client.allocate_timestamps(count).await?`

Advanced configuration should use `ClientConfig` builder methods.

## Build and test

```bash
cargo clippy --all-targets -- -D warnings
cargo test client::tests --lib
```

## Minimal usage

```rust
use chronos::Client;

type AppResult<T> = Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> AppResult<()> {
    let client = Client::connect("127.0.0.1:50051", "orders.primary").await?;
    let ranges = client.allocate_timestamps(1).await?;
    println!("tso={}", ranges[0].start_tso);
    Ok(())
}
```

## Behavior

- The client ensures the bound timeline on first connect
- The client fetches and caches the current route internally
- Stale-route errors are handled with one refresh-and-retry cycle
- Route refresh reconnects allocation traffic to the current owner endpoint
- Other RPC failures are returned to the caller

## Files

- Implementation: `src/client.rs`
- README: `clients/rust/README.md`
