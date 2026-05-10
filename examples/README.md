# Chronos Examples

These examples show the application-facing client path:

1. Connect to one Chronos endpoint
2. Bind the client to one `timeline_key`
3. Allocate timestamps

The examples use plaintext transport for a local `CHRONOS_SECURITY_MODE=dev-insecure` server. Use
TLS/mTLS configuration for shared or production environments.

## Run a local server

```bash
export CHRONOS_SECURITY_MODE=dev-insecure
export CHRONOS_BIND_ADDR=127.0.0.1:50051
export CHRONOS_ADVERTISE_ENDPOINT=127.0.0.1:50051
cargo run --bin chronos
```

## Language Examples

- Rust: `cargo run --example client_example`
- Go: `cd examples/go && go run .`
- Java: `cd clients/java && gradle runExample`
- C++: built by `clients/cpp/CMakeLists.txt` as `client_example`

Each client hides timeline creation, route lookup, owner routing, and stale-route refresh during
normal allocation.
