# Configuration

The primary CLI reads `connector.json` by default. Unknown JSON fields are rejected. A configuration has 1-16 uniquely named endpoints. Endpoint IDs are 1-64 ASCII letters, digits, `_` or `-`.

| Field | Meaning |
| --- | --- |
| `name` | Human-readable computer name |
| `local_only` | `true` disables OpenZiti and requires only a private LAN |
| `lan_port` | Private LAN TLS port; default `7443` |
| `identity_file` | Enrolled home OpenZiti identity JSON; required for remote mode; relative paths resolve beside the config |
| `service` | OpenZiti service name; required for remote mode |
| `endpoints` | Explicit API/harness allowlist |

Each endpoint has `id`, `name` and `kind`. An HTTP endpoint also needs `url`, which must use `http` or `https`, literal `127.0.0.1` or `[::1]`, and an explicit port. A base path such as `/v1` is allowed. Embedded URL credentials, query strings, fragments, DNS names, LAN addresses and public addresses are rejected.

Credential fields contain **environment variable names**, never key/password values:

- `api_key_env`: adds an upstream Bearer API key.
- `username` and `password_env`: adds upstream Basic authentication. Hermes requires both.
- An OpenCode endpoint without a URL reads its local service descriptor from `$XDG_STATE_HOME/opencode/service.json` (default `~/.local/state/opencode/service.json`) and discovers its existing credentials without writing them into the connector configuration or pairing code.

Set credentials in the process environment using your usual secret manager. Do not put secrets into the CLI argument list or commit `.env` files. Missing configured environment variables cause startup to fail.

## Example endpoints

Add only the endpoints you intend to expose; these are alternatives, not prerequisites:

```json
[
  {"id":"models","name":"Home models","kind":"openai-compatible","url":"http://127.0.0.1:1234"},
  {"id":"opencode","name":"OpenCode","kind":"opencode"},
  {"id":"hermes","name":"Hermes","kind":"hermes","url":"http://127.0.0.1:8642","username":"admin","password_env":"HERMES_PASSWORD"},
  {"id":"codex","name":"Codex","kind":"codex","directory":"/absolute/path/to/your/project"}
]
```

Codex uses native app-server stdio rather than a URL. `directory` must be absolute; if omitted it uses the connector's working directory. The project directory is not an operating-system sandbox: the Codex process and its configured tools retain their own permissions.

## State and upgrades

Keep the state directory private and persistent. It contains the home identity, TLS keys, device-token hashes, endpoint identity mappings and request journal. Files are local to your computer; there is no encrypted-at-rest database guarantee. Use operating-system account protection and disk encryption as appropriate.

Do not delete state to restart or upgrade. Use the same directory and config. Changing an endpoint's kind, URL or directory creates a new endpoint identity to avoid mixing runtime histories. The public CLI keeps the pilot's `LocalzieZiti` default directory so existing phones keep their trust.

`devices` lists pairings; `revoke --device ID` revokes the independent phone credential. It also drops that phone's active connections shortly afterward. Already accepted work continues; revoking a phone is not a cancellation command. An OpenZiti operator can separately revoke the phone's network identity.
