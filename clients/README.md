# Chronos Clients

Chronos clients are the preferred application integration surface.

The model is the same in every language:

1. Create one `Client` bound to one timeline
2. Call one allocation API
3. Close or destroy the client when done

The client handles the operational details that applications should not duplicate:

- The client ensures the bound timeline on first connect
- The client fetches and caches the current route internally
- Stale-route errors are handled internally with a configurable refresh-and-retry budget
- Route refresh reconnects allocation traffic to the current owner endpoint
- Allocation idempotency is optional; enable it only when callers need replay protection for
  ambiguous retries

Route-management RPCs remain in the protobuf contract for control-plane use, but they are not the
normal application API.

Common client options are available in every language:

- Desired resource tier for first timeline ensure
- Per-allocation request timeout forwarded to Chronos
- Stale-route retry attempts and retry backoff
- Optional allocation request idempotency
- TLS, mTLS, server-name override, and explicit plaintext for local development

## Languages

- Go: `clients/go/README.md`
- Rust: `clients/rust/README.md`
- Java: `clients/java/README.md`
- C++: `clients/cpp/README.md`

## Examples

- Rust: `cargo run --example client_example`
- Go: `cd examples/go && go run .`
- Java: `cd clients/java && gradle runExample`
- C++: `cmake -S clients/cpp -B clients/cpp/build && cmake --build clients/cpp/build --target client_example`

## Support level

| Language | Status | Notes |
|---|---|---|
| Rust | Primary | In-repo implementation, linted, and tested |
| Go | Primary | Packaged submodule with generated proto and tests |
| Java | Repository-local | In-repo implementation, tests, and Maven publication metadata |
| C++ | Repository-local | In-repo implementation, tests, and CMake install/export target |

## Compatibility

Client examples and library entry points are CI-gated. Rust and Go are the stable application
surfaces for published integrations. Java and C++ track the same API shape in this repository and
are suitable for source-based integrations; publish them externally only with an explicit
versioning and distribution step.

The cross-language client contract lives in `clients/client-contract.md` and is checked by
`make client-conformance-check`. Repository-local package metadata is checked by
`make client-package-check`.
