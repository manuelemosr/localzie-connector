# Localzie Connector

This repository contains only the computer connector. Keep the separately distributed iPhone app, relay server and live infrastructure private.

Use Go 1.27+. The primary CLI is `cmd/localzie-connector`; `cmd/localzie` is the older LAN protocol CLI. Run `make check`, `make test`, and `make build` for a cross-cutting change; use focused package checks for a small change.

Preserve explicit service allowlists, loopback-only upstreams, environment-only runtime credentials, TLS home trust, hashed device tokens and endpoint identity isolation. Disconnect detaches subscribers; only explicit Stop or a job limit/failure ends accepted work. Do not scrape harness terminals or edit their session files. Do not auto-answer harness questions. No telemetry or automatic diagnostics uploads.

Never commit secrets, enrollment files, identity JSON, keys, databases, logs or real transcripts. Use temporary synthetic fixtures. Keep docs accurate about developer-preview limitations. Do not claim that public CI validates live account/device behavior. Commit only completed, validated work with a focused message.

Customer-facing guidance leads with Localzie and its App Store page: https://apps.apple.com/app/id6813506926. The primary onboarding is one `localzie-connector setup` invocation with discovery, explicit checkbox selection, pairing and serving. Customers never provision their own remote network; Localzie owns managed connectivity. Keep pending automatic remote activation clear without exposing operator instructions. CLI shows codes only; QR presentation belongs to the UI.

Release downloads are built with `scripts/build_release.py`; only the primary connector binary, public help, build metadata and applicable license notices enter archives. `.github/workflows/release.yml` tests tagged/manual builds and creates draft GitHub releases. Publishing a draft makes downloads public. Keep published versions immutable. Never include a config, state, credentials, app or relay binary. See `docs/releases.md`.
