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
- Owner `UNAVAILABLE` failures trigger bounded route recovery through the
  stable construction endpoint
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

The endpoint passed to the constructor is a control-plane endpoint, not an owner-pod address. Use a
stable Service or load balancer in production so it remains reachable after an owner exits.

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

Client examples and library entry points are CI-gated. This repository has not published its first
client release yet, so the current checkout remains a source-based integration. Tag-triggered
release automation is available for Go, Java, and C++; only artifacts produced by that workflow are
published releases.

The Go module lives at `github.com/rleungx/chronos/clients/go`. Release it with a matching
subdirectory tag such as `clients/go/v0.1.0`.

The versioning, distribution, and public compatibility policy is defined in
`clients/compatibility.md`.

The cross-language client contract lives in `clients/client-contract.md` and is checked by
`make client-conformance-check`. Repository-local package metadata is checked by
`make client-package-check`.
