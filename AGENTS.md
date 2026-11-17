# Localzie Connector

This repository contains only the computer connector. Keep the separately distributed iPhone app, relay server and live infrastructure private.

Use Go 1.27+. The primary CLI is `cmd/localzie-connector`; `cmd/localzie` is the older LAN protocol CLI. Run `make check`, `make test`, and `make build` for a cross-cutting change; use focused package checks for a small change.

Preserve explicit service allowlists, loopback-only upstreams, environment-only runtime credentials, TLS home trust, hashed device tokens and endpoint identity isolation. Disconnect detaches subscribers; only explicit Stop or a job limit/failure ends accepted work. Do not scrape harness terminals or edit their session files. Do not auto-answer harness questions. No telemetry or automatic diagnostics uploads.

Never commit secrets, enrollment files, identity JSON, keys, databases, logs or real transcripts. Use temporary synthetic fixtures. Keep docs accurate about developer-preview limitations. Do not claim that public CI validates live account/device behavior. Commit only completed, validated work with a focused message.
