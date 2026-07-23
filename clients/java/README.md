# Chronos Java Client

`Client` is the Java application-facing Chronos client implementation.

Support level: Release-ready implementation with GitHub Packages publication metadata.

## Install

Published coordinates are `io.github.rleungx:chronos-java-client:<version>` in the repository's
GitHub Packages Maven registry. GitHub Packages requires Maven credentials, including for public
packages. Releases also attach the binary, sources, and Javadoc JARs to the corresponding GitHub
Release tag, such as `clients/java/v0.1.0`, together with SHA-256 checksums and build provenance.

Gradle Kotlin DSL consumers can configure the authenticated repository and dependency as follows.
`GITHUB_TOKEN` must be a classic personal access token with `read:packages` permission outside
GitHub Actions.

```kotlin
repositories {
    maven {
        url = uri("https://maven.pkg.github.com/rleungx/chronos")
        credentials {
            username = providers.gradleProperty("gpr.user").orNull
                ?: System.getenv("GITHUB_ACTOR")
            password = providers.gradleProperty("gpr.key").orNull
                ?: System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    implementation("io.github.rleungx:chronos-java-client:0.1.0")
}
```

See `../compatibility.md` for the compatibility commitment.

## Public shape

- `client = new Client(addr, timelineKey)`
- `config = Client.Config.defaults()...`
- `client = new Client(addr, timelineKey, config)`
- `client.allocateTimestamps(count)`
- `client.close()`

`Client.Config` covers desired resource tier, request timeout, route-recovery retry attempts/backoff,
idempotency, and transport. The transport config supports TLS, mTLS, authority override, and
explicit plaintext for local development.

## Build and test

```bash
cd clients/java
gradle test
```

To validate the example without adding it to the library artifact:

```bash
cd clients/java
gradle compileExampleJava
```

## Behavior

- The client ensures the bound timeline on first connect
- The client fetches and caches the current route internally
- Stale-route errors are handled with a configurable refresh-and-retry budget
- Route refresh reconnects allocation traffic to the current owner endpoint
- Owner transport failures refresh through the stable construction endpoint; use a Service or load
  balancer rather than an owner-pod address in production
- Allocation request-record idempotency is disabled by default; use
  `Client.Config.defaults().withIdempotency(true)` when callers need replay protection
- Normal allocation calls are safe to run concurrently on one client instance
- Other RPC failures are returned to the caller

## Example

```bash
cd clients/java
gradle runExample
```
