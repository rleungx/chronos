# Security Review

- Generator CAS conflicts preserve fail-closed fencing: a stale lease may remain tracked for maintenance retry, but its ready token is cleared until authoritative metadata is reloaded.
- Existing expired-lease failover tests were not relaxed.
- New timeout metric labels are a finite two-value set: `serializer_wait` and `allocation`.
- Lease warning suppression retains exact metrics while bounding log volume; no request payloads, credentials, or new network surfaces are logged.
- CI action SHAs remain pinned; only the Go toolchain version needed by the pinned actionlint release changed.
- `cargo deny check` passed for advisories, bans, licenses, and sources.
- No dependency, protobuf, public API, credential, or persisted-schema change was introduced.
