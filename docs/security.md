# Security model and limitations

The goal is to expose an explicit selection of local model/harness APIs to paired phones, over authenticated encrypted connections. This is powerful remote access and is not a promise that nobody can ever access a user's data.

## Trust boundaries

- **Phone:** holds the device credential and home trust material. A compromised paired phone can invoke configured APIs. Protect the device and revoke it if lost.
- **Home computer:** holds connector keys, local state and upstream credentials. A host administrator or malware can read those and any local model/harness activity. The connector is not a host sandbox.
- **OpenZiti fabric:** authenticates network participants and routes remote connections. Additional home-pinned TLS is intended to keep application contents confidential even from that infrastructure. Operators still see connection metadata and can deny service.
- **Runtime/harness:** its own API is the permission boundary. Localzie does not reduce a coding harness to a text-only model. It may execute commands or read and write files, under its own process permissions. External model providers and harness telemetry have their own policies.

## Implemented controls

TLS 1.3; pairing-anchored home trust; independent per-phone credentials hashed at rest; single-use ten-minute pairing secrets; an operator-owned allowlist of literal loopback destinations; bounded pre-authentication reads and pairing concurrency; device revocation; endpoint identity isolation; idempotent accepted-request receipts and bounded replay.

The LAN listener binds private/link-local interface addresses rather than a wildcard/public address. This does not make an untrusted LAN safe by itself: TLS and authentication remain mandatory. Do not publish this LAN port with a router, reverse proxy or public tunnel.

## Operational limits

- Developer preview, not a completed independent security audit or a blanket production-hardening claim.
- Remote enrollment and network policies are operator-managed. A policy mistake can expose network reachability even though home authentication is still required.
- The local database and replay journal are not encrypted at rest. Protect the state directory and backups. Pairing codes, identity JSON and JWT files are secrets.
- A paired phone can use all capabilities exposed by a configured harness. Codex tool permissions may auto-approve while that phone controls a session; questions are not auto-answered.
- Revocation drops access, but accepted jobs continue. Explicit Stop is separate, and upstream cancellation support varies.
- Replay is bounded (30-minute operations and recovery window, 4 MiB captured response limit, 16 MiB upload limit). It is not unbounded offline continuation. Uncertain work after restart is reported instead of automatically repeated.
- Trust rotation covers renewable leaves under the same home CA. Replacing a lost/expired home CA or resetting private state requires new trust setup.
- The older LAN CLI's relay client remains compatibility code. This repository does not supply an internet-safe managed relay.

Report vulnerabilities privately as described in [SECURITY.md](../SECURITY.md). Never include credentials or real chat histories in public issues.
