# Rust Chronos Client Example

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
