# Chronos Clients

Chronos provides application-facing `Client` implementations for multiple languages.

All clients follow the same model:

1. Create one `Client` bound to one timeline
2. Call one allocation API
3. Close or destroy the client when done

Shared behavior:

- The client ensures the bound timeline on first connect
- The client fetches and caches the current route internally
- Stale-route errors are handled internally with refresh-and-retry behavior
- Route refresh reconnects allocation traffic to the current owner endpoint
- Allocations omit request-record idempotency by default to keep the hot path metadata-free;
  enable the language-specific idempotency option when replay protection is required
- Other RPC failures are returned to the caller

The protobuf route-management RPCs remain part of the wire contract, but they are treated as
internal client machinery rather than the preferred application integration surface.

## Languages

- Go: `clients/go/README.md`
- Rust: `clients/rust/README.md`
- Java: `clients/java/README.md`
- C++: `clients/cpp/README.md`

## Support level

| Language | Status | Notes |
|---|---|---|
| Rust | Primary | In-repo implementation, linted, and tested |
| Go | Primary | Packaged submodule with generated proto and tests |
| Java | Repository-local | In-repo implementation and tests, not yet a published SDK artifact |
| C++ | Repository-local | In-repo implementation and tests, not yet a published SDK artifact |
