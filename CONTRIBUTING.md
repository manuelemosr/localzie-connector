# Contributing

Start with a small issue describing the problem and a reproducible synthetic example. For vulnerabilities, use [SECURITY.md](SECURITY.md) instead of a public issue.

## Local checks

Use Go 1.27 or newer on macOS or Linux:

```sh
make check
make test
make build
```

Most tests use temporary directories and local fixtures. Do not enable live integration tests in shared CI. Live tests use `LOCALZIE_CODEX_LIVE` or `LOCALZIE_ZITI_TEST_HOME`, `LOCALZIE_ZITI_TEST_PHONE`, and `LOCALZIE_ZITI_TEST_SERVICE`; use only your own disposable identities. Never commit their files or real transcripts.

Keep accepted work independent of subscriber connections; Stop must be explicit. Preserve home/endpoint identity separation, pinned TLS, hashed phone tokens, environment-only upstream secrets, and the loopback allowlist. Harness questions must never be auto-answered. Use APIs, not terminal scraping or session-file mutation.

Format Go with `gofmt`, add focused regression coverage for behavior changes, and include the exact checks and user-visible impact in the pull request. Do not add analytics, silent diagnostics uploads, live infrastructure addresses or unrelated app code.

Contributions are under the repository's Apache 2.0 license. Third-party code requires its original notices and compatible licensing.
