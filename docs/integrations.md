# Integration provenance and compatibility

The connector uses existing local APIs; it does not scrape terminals or modify harness session files. These are the integration pins at extraction. Upgrading a dependency is not evidence that a harness protocol remains compatible; run focused fixtures and an opt-in live smoke test.

| Integration | Pin / interface | Official reference |
| --- | --- | --- |
| OpenZiti | `sdk-golang` v1.8.3, exact transitive versions in `go.mod`/`go.sum` | [SDK source](https://github.com/openziti/sdk-golang/tree/v1.8.3) |
| Codex | CLI 0.167.0; app-server JSON-RPC over stdio, native schema used during integration | [App-server guide](https://learn.chatgpt.com/docs/app-server.md) |
| OpenCode | Native service APIs; legacy compatibility gates and v2 adapter fixtures are in `internal/harness` | [Server documentation](https://opencode.ai/docs/server/) |
| OpenAI-compatible models | `/models`, streaming chat completions and runtime-specific reasoning compatibility | [OpenAI API reference](https://platform.openai.com/docs/api-reference/chat) |
| Hermes | Local authenticated dashboard API passed through by the primary connector; the phone supplies its client integration | [Hermes Agent](https://github.com/NousResearch/hermes-agent) |

Compatibility with every version of every OpenAI-compatible runtime is not implied. Local API behavior and cancellation semantics differ. Live integration tests are opt-in so public CI cannot reach a real account, home network or customer data.
