# Changelog

All notable changes will be documented here.

The project follows Semantic Versioning. Protocol compatibility, generated clients, release
artifacts, and retained failure-recovery evidence must pass the release gate before a version is
tagged.

## [Unreleased]

- Recover client routes after owner transport failures.
- Validate failover and rebalance evidence without masking benchmark failures.
- Align the Go module path and package metadata with the repository layout.
- Add independent Go, Java, and C++ client release automation and a public compatibility policy.
- Gate client releases against prior published versions, attest packaged artifacts, and document
  supported client toolchains and platforms.
- Add automated Go source API, Java source/binary API, and C++ source/ABI compatibility gates.
