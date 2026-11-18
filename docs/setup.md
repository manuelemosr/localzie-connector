# Set up Localzie on your computer

Get [Localzie for iPhone](https://apps.apple.com/app/id6813506926), then run this on the computer with your models or coding agents:

```sh
localzie-connector setup
```

1. Start your model server, OpenCode or Hermes. Codex only needs to be installed and signed in.
2. In setup, check the services you want on your iPhone. Use arrow keys, Space and Enter.
3. Copy the `LZ1:` code into **Localzie > Connect to a server** on your iPhone. Choose a service from the server list.
4. Keep the terminal open while connected. Press Ctrl+C to stop the connector.

You do not need to run separate serving and pairing commands. Setup saves your choices privately. To change them, stop the running connector with Ctrl+C, then run setup again. An existing Localzie remote connection is kept. For first-time pairing across networks on a computer enabled for the Localzie preview, follow [remote setup](remote-setup.md).

## A service didn't appear

Make sure it is started on this computer. Setup checks the common local model-server addresses and reads OpenCode's existing service information. It does not scan your network or start a model response.

For OpenCode, start its local service first. For Codex, install and sign in to the Codex CLI first. The current directory becomes Codex's project directory when you first add it.

If a service needs a key or password, setup reuses configured credentials without saving the secret in the setup file:

- Model-server API keys can be supplied through your existing environment using `setup --api-key-env NAME_OF_VARIABLE`.
- OpenCode reuses its own service descriptor or the existing `OPENCODE_SERVER_PASSWORD` environment variable.
- Hermes discovery uses the existing `HERMES_USERNAME` and `HERMES_PASSWORD` environment variables. Otherwise an existing saved Hermes connection is retained. The dashboard must be running with the authentication expected by that connection.

Custom service addresses and less common configurations remain available through [advanced configuration](configuration.md). No network setup is required from customers for managed remote access.

## Headless computers and accessibility

A normal SSH terminal can run the same checkbox setup. For screen readers or a terminal without interactive key support:

```sh
localzie-connector setup --accessible
```

Enter a service number to check or uncheck it, then enter `0` to connect. Closing input before confirmation cancels setup and keeps your old choices.

## The phone says the code expired

Run setup again to get a fresh code. Codes expire after ten minutes and can be used once. Do not share codes in public issues, screenshots or recordings. For a new remote pairing code on a computer enabled for the preview, run `localzie-connector setup --remote` again.

## After restarting your computer

Run `localzie-connector setup` again. Your previous choices are checked for you. This preview runs in the foreground; an automatic background-service installer is not included yet.

## Check a paired phone

For this preview's home-network test, keep your iPhone and computer on the same Wi-Fi. Open a selected model or harness and send: “Reply with hello, without using tools.” Expect the normal Localzie chat and a reply. Try the setup code again on another connection: the used code should be refused.
