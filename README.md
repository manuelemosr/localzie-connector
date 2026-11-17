# Localzie Connector

[![CI](https://github.com/manuelemosr/localzie-connector/actions/workflows/ci.yml/badge.svg)](https://github.com/manuelemosr/localzie-connector/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Use the AI models and coding agents on your own computer from Localzie on your iPhone. The connector makes only the services you configure available to paired phones, over your private LAN and optionally [OpenZiti](https://openziti.io/).

**Status: developer preview.** The connector is usable from source. Remote access currently requires an operator-provisioned OpenZiti network and enrollment files. There is no hosted enrollment service or automatic installation/start-at-login wizard in this release.

## What it connects

| Service | Configuration kind | How it connects |
| --- | --- | --- |
| Ollama, llama.cpp, LM Studio, vLLM and other OpenAI-compatible servers | `openai-compatible` | Your existing loopback HTTP API |
| OpenCode | `opencode` | Your existing local service; can discover its service descriptor |
| Hermes | `hermes` | Your authenticated local dashboard API |
| Codex (experimental) | `codex` | A connector-owned `codex app-server` process using your existing CLI installation |

Localzie's iPhone app is distributed separately. This repository contains the computer connector, its tests and documentation. It does not contain the app or a relay server.

## Build and install

Prerequisites: **Go 1.27 or newer**, macOS or Linux, and at least one running model server or supported harness. Codex additionally needs its CLI installed and authenticated on the computer. The Codex integration was developed against CLI 0.167.0; other versions need compatibility testing.

```sh
git clone https://github.com/manuelemosr/localzie-connector.git
cd localzie-connector
make build
./bin/localzie-connector --help
```

Or install the primary CLI into your Go binary directory:

```sh
go install github.com/manuelemosr/localzie-connector/cmd/localzie-connector@latest
```

No prebuilt binaries are published yet. Windows has not been validated.

## Start on your home network

1. Start your model server on the same computer. The example uses an OpenAI-compatible API at `http://127.0.0.1:1234` (change that port to match your server).
2. Copy the example and review which services you want to share:

   ```sh
   cp examples/connector.local.json connector.json
   ./bin/localzie-connector serve --config connector.json
   ```

3. Keep that terminal running. In a second terminal, create a single-use pairing code:

   ```sh
   ./bin/localzie-connector pair --config connector.json
   ```

4. On your iPhone, open **Localzie > Connect to a server**, open the remote/setup-code banner and paste the complete `LZ1:` code. Despite that banner's name, the local-only example does not enable remote access. Your configured services appear in the normal server list. The phone and computer must be on the same reachable private network for this example.

Codes expire after 10 minutes and work once. **Treat them like a password to your computer.** Copy the code privately; never put it in an issue, screenshot, build log or public terminal recording. The CLI prints a code; QR presentation belongs to the separate UI.

The default private state directory is `LocalzieZiti` under the operating system's user configuration directory. Keep that existing name when upgrading from the pilot to preserve paired phones. Use the same `--data-dir /absolute/private/path` for `serve`, `pair`, `devices` and `revoke` if you override it. The foreground server must stay running, and the computer must stay awake. There is no background-service installer yet.

## Enable access away from home

See [remote setup](docs/remote-setup.md). You need a configured OpenZiti service, a home identity allowed to **Bind**, and one single-use phone enrollment allowed to **Dial**. Running the connector alone does not create that infrastructure.

The same home connector serves LAN and OpenZiti routes with the same phone authentication and service list. A compatible Localzie app prefers a verified local connection at home and uses the paired remote path when away. No phone VPN or public port forwarding is needed.

## Commands

| Command | Purpose |
| --- | --- |
| `serve --config connector.json` | Serve the allowlisted services until stopped |
| `pair --config connector.json [--jwt phone.jwt]` | Print one expiring setup code; `--jwt` is required for remote mode |
| `enroll --jwt home.jwt --identity home.json` | Enroll the home OpenZiti identity; refuses to overwrite an existing file |
| `devices` | List paired device IDs and revocation state as JSON |
| `revoke --device DEVICE_ID` | Revoke a phone's connector access |
| `version` | Print the build version |

Run any command with `--help` for flags. [Configuration](docs/configuration.md) documents authentication, multiple harnesses and state. The older `cmd/localzie` LAN protocol CLI remains in source for compatibility; the primary command above is the LAN/OpenZiti connector.

## Security and privacy

- TLS 1.3 protects phone-to-home traffic independently of the OpenZiti fabric. Pairing anchors home certificate trust; phone bearer tokens are independently checked and hashed at rest.
- Pairing secrets are single-use and expire. The home computer's configuration is the service allowlist. Upstream HTTP destinations must be literal loopback addresses with explicit ports.
- The connector listens on concrete private/link-local LAN addresses, never a wildcard/public interface. OpenZiti handles the remote route. Do not forward the LAN port through your router.
- Coding harnesses can read/write files and execute commands under their own permissions. Tool permissions may be automatically approved while the phone controls Codex; interactive questions are not auto-answered. Pairing grants powerful access to the configured harness, not just model text completion.
- Accepted work belongs to the connector. Losing a connection detaches the phone; explicit Stop, a limit, upstream failure or host shutdown ends the work.
- No application analytics or automatic diagnostic uploads are added by the connector. Your selected runtimes and harnesses have their own networking, authentication and data policies.

Encryption does not make a compromised phone or home computer safe. OpenZiti infrastructure can observe connection metadata and disrupt availability; the additional TLS layer is intended to protect application content from that infrastructure. The connector keeps private state and a bounded request replay journal on the home computer, so host administrators can access that local data. Read the [threat model and limitations](docs/security.md).

## Development

```sh
make check
make test
make build
```

CI builds, vets and runs race-enabled tests on Linux and macOS. Most tests use synthetic local services. Live Codex and OpenZiti tests are opt-in and require your own private credentials; they do not run in public CI. See [CONTRIBUTING.md](CONTRIBUTING.md), [architecture](docs/architecture.md) and [integration pins](docs/integrations.md).

## License

[Apache License 2.0](LICENSE). Dependencies retain their own licenses; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). Product and third-party trademarks remain with their owners.
