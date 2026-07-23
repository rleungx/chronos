# Client versioning and compatibility

Chronos clients are versioned independently from the server and from each other:

| Client | Release tag | Distribution |
| --- | --- | --- |
| Go | `clients/go/vX.Y.Z` | Go module proxy and GitHub Release |
| Java | `clients/java/vX.Y.Z` | GitHub Packages Maven registry and attested GitHub Release JARs |
| C++ | `clients/cpp/vX.Y.Z` | Attested CMake package archive and SHA-256 checksums on GitHub Releases |

Only a successful tag-triggered `Client Release` workflow is a published client release. Source on
the default branch and untagged artifacts are not releases.

## Supported toolchains and platforms

| Client | Minimum toolchain | Published artifact support |
| --- | --- | --- |
| Go | Go 1.24 | Platforms supported by the Go toolchain |
| Java | JDK 21 | JVM platforms capable of running Java 21 |
| C++ | C++17 and CMake 3.20 | Ubuntu 24.04 x86-64 package; source builds elsewhere |

The C++ archive is compiled against the gRPC and protobuf development packages from Ubuntu 24.04.
Other distributions, architectures, and operating systems must build the client from source against
their local dependencies; the project does not claim cross-distribution C++ ABI compatibility.

## Compatibility commitment

- All clients use Semantic Versioning.
- For `0.y.z` releases, patch updates preserve public source compatibility; a minor update may make
  breaking changes and must document them in `CHANGELOG.md`.
- Starting with `1.0.0`, public source APIs remain backward compatible throughout the same major
  version. A removal requires prior deprecation in at least one minor release.
- Releases that promise source compatibility are compared with the previous published release by
  Go `apidiff`, Java `japicmp`, and C++ ABI Compliance Checker. On the supported Ubuntu C++ target,
  the same compatibility window also preserves the packaged library ABI.
- The protobuf wire contract remains backward compatible across supported client versions. The
  release workflow compares it with the previous client release tag before publishing.
- Client retry, routing, TLS, idempotency, and timeout behavior is shared by the cross-language
  contract and its conformance gate.
- Server features may be added without requiring a client update. A client release must not require
  an unreleased server unless its release notes explicitly declare that minimum server version.

The first release of each client establishes its compatibility baseline. Until that tag exists, the
client remains pre-release even when its repository-local tests pass. CI self-compares each current
artifact so a missing or broken compatibility tool cannot silently disable the first subsequent
release comparison.

## Release procedure

1. Run `make release-check-core` on the exact commit.
2. Update `CHANGELOG.md` with client-visible changes and compatibility impact.
3. Create exactly one client tag using the patterns above.
4. Wait for the `Client Release` workflow to finish successfully.
5. Verify the GitHub Release assets and, for Java, the Maven package coordinates.

Do not move or reuse a published tag. Publish a new patch release for corrections.
