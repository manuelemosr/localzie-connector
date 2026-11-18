<div align="center">

# Localzie

[![CI](https://github.com/manuelemosr/localzie-connector/actions/workflows/ci.yml/badge.svg)](https://github.com/manuelemosr/localzie-connector/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)

### Your computer's AI, on your iPhone.

Use your own models and coding agents from your phone. Keep working on an OpenCode, Hermes or Codex project, or chat with the models running on your computer.

**[Get Localzie on the App Store](https://apps.apple.com/app/id6813506926)** · **[Download the computer connector](https://github.com/manuelemosr/localzie-connector/releases)**

The open-source computer companion for Localzie.

[![Localzie - create images, chat with agents, use home models](docs/images/localzie-banner.png)](https://apps.apple.com/app/id6813506926)

</div>

## How it works

```mermaid
flowchart LR
    Computer["Your computer\nThe models and agents you select"] <-->|"Secure connection\nHome Wi-Fi or away"| Phone["Localzie on your iPhone"]
```

## Connect your computer

Once the connector is installed, run:

```sh
localzie-connector setup
```

It finds your models and coding agents. Use the arrow keys to move, **Space** to check the ones you want, then **Enter** to connect.

```text
Localzie
Your computer's AI, on your iPhone.

What would you like to use in Localzie?

  [x] Ollama
  [x] OpenCode
  [ ] Codex

Space to select · Enter to connect
```

Setup saves your choices, starts the connector and gives you a private, one-use code. On your iPhone, open **Localzie → Connect to a server** and paste the code. Your selected services appear in the usual server list.

That's one command for setup and starting the connection. Keep the terminal open while you use Localzie; closing it stops the connector. Run `setup` again to change your choices. The CLI uses a copyable code; QR codes are for the UI version.

## At home and away

At home, Localzie prefers your home Wi-Fi. Remote access uses a secure connection managed by Localzie, with the same servers and chats in the app. You do not need a VPN app, public ports or a network to administer.

Run `localzie-connector setup` once to choose what to share. On computers enabled for the Localzie preview, that same command prepares a connection you can pair and use from another network. Other computers use home-network access. [Remote setup](docs/remote-setup.md).

## What can I connect?

- **Local models** through Ollama, LM Studio, llama.cpp, vLLM, Jan and other compatible model servers.
- **OpenCode** and **Hermes** for your existing coding-agent sessions.
- **Codex**, using the Codex installation already on your computer.

Start the service before running setup. OpenCode can be found through its existing local service information. Codex can be selected when installed. Services protected by a password or API key need their existing credentials to be available to the connector; see [help with discovery](docs/setup.md).

A computer without a display works too: run setup in its terminal. A text-only, screen-reader-friendly version is available with `localzie-connector setup --accessible`.

## Your computer stays yours

Only the services you check are shared with your paired phone. Your models and agents keep running on your computer. A setup code is private, expires after 10 minutes and works once.

Coding agents can work with files and run commands, so keep your pairing code safe, just as you would access to your computer. [Privacy and security details](docs/security.md).

## Download the connector

[GitHub Releases](https://github.com/manuelemosr/localzie-connector/releases) is where prebuilt downloads are published. Choose Mac for Apple silicon or Intel, or Linux x64/ARM64. Extract the archive and run `./install.sh`. It reports success or an error, and prints the exact command to start setup. You can also run `./localzie-connector setup` directly from the extracted folder.

The first preview release is being prepared. Mac binaries are not Apple Developer ID signed or notarized yet.

## Install from source

The connector currently supports macOS and Linux. With Go 1.27 or newer installed:

```sh
git clone https://github.com/manuelemosr/localzie-connector.git
cd localzie-connector
./install.sh
```

Already in this checkout? Run `./install.sh` or `make install`. The installer confirms installation and prints a copyable setup command. If you built with `make build`, you can also use `./bin/localzie-connector setup` directly.

For Go developers, `go install github.com/manuelemosr/localzie-connector/cmd/localzie-connector@main` remains available. Go's own install command is silent on success and installs into `go env GOBIN` (or `$(go env GOPATH)/bin` when GOBIN is empty); that folder must be on PATH. `@main` uses the current preview rather than an older cached release.

## Help and development

[Setup help](docs/setup.md) · [Report a problem](https://github.com/manuelemosr/localzie-connector/issues/new/choose) · [Contribute](CONTRIBUTING.md)

The computer connector is open source under [Apache 2.0](LICENSE). Localzie for iPhone is available separately on the App Store. Developer references: [architecture](docs/architecture.md), [advanced configuration](docs/configuration.md), [integration pins](docs/integrations.md) and [dependency notices](THIRD_PARTY_NOTICES.md). Maintainers: [building and publishing releases](docs/releases.md).
