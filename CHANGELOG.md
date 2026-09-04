# Changelog

All notable changes will be documented here.

The project follows Semantic Versioning. Protocol compatibility, generated clients, release
artifacts, and retained failure-recovery evidence must pass the release gate before a version is
tagged.

## [Unreleased]

- Add a backpressure-aware bidirectional timestamp allocation stream while preserving the unary
  client contract.
- Update `h2` to 0.4.16 to reject unbounded empty DATA frames on long-lived HTTP/2 connections.
- Round identity lease grant requests up to whole seconds, use the server-selected grant TTL for
  the initial keepalive deadline, and expose configured versus requested identity TTL values.
- Recover client routes after owner transport failures.
- Validate failover and rebalance evidence without masking benchmark failures.
- Align the Go module path and package metadata with the repository layout.
- Add independent Go, Java, and C++ client release automation and a public compatibility policy.
- Gate client releases against prior published versions, attest packaged artifacts, and document
  supported client toolchains and platforms.
- Add automated Go source API, Java source/binary API, and C++ source/ABI compatibility gates.
