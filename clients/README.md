# Chronos Clients

Chronos provides application-facing `Client` implementations for multiple languages.

All clients follow the same model:

1. Create one `Client` bound to one timeline
2. Call one allocation API
3. Close or destroy the client when done

Shared behavior:

- The client ensures the bound timeline on first connect
- The client fetches and caches the current route internally
- Stale-route errors are handled with one refresh-and-retry cycle
- Route refresh reconnects allocation traffic to the current owner endpoint
- Other RPC failures are returned to the caller

## Languages

- Go: `clients/go/README.md`
- Rust: `clients/rust/README.md`
- Java: `clients/java/README.md`
- C++: `clients/cpp/README.md`
