# Remote setup with OpenZiti

Remote setup is currently an operator workflow. This repository does not include a managed enrollment service, controller, edge router or infrastructure credentials. Local-only operation needs none of them.

## Provision your network

Use your own [OpenZiti network](https://openziti.io/docs/learn/quickstarts/). A reachable controller and edge router must already exist.

Create a service (for example `my-home-models`) and restrict policies so:

- The home connector identity can **Bind** only that service.
- Each phone identity can **Dial** only that home's service.
- Both identities can reach an appropriate edge router.
- Phones cannot bind the service or dial other users' home services.

Provision one single-use enrollment JWT for the home and a separate single-use enrollment JWT for each phone. Use short expirations. They are credentials, not public configuration. Do not share or reuse one phone enrollment across customers.

## Enroll and run the home connector

```sh
localzie-connector enroll --jwt /private/path/home.jwt --identity /private/path/home.json
cp examples/connector.remote.json connector.json
```

Edit `connector.json`: set the enrolled identity's absolute path, your service name and your allowlisted endpoints. Leave `local_only` false (or omitted).

```sh
localzie-connector serve --config connector.json
```

The private LAN path can operate while the remote fabric reconnects. Keep this process running on an awake computer.

## Pair a phone

In another terminal using the same private state directory:

```sh
localzie-connector pair --config connector.json --jwt /private/path/phone.jwt
```

Paste the complete `LZ1:` code into Localzie's setup-code entry under **Connect to a server**. The phone enrolls its own OpenZiti identity, verifies the home TLS trust from the code, and exchanges the one-use home secret for an independent phone credential. The code expires after 10 minutes; the OpenZiti enrollment may expire earlier, so generate it close to use.

A second phone needs its own enrollment and a new pairing code. The terminal does not display a QR. A future UI can present the same setup payload as a QR.

## Check the route

1. At home, pair and open a configured model/harness in Localzie.
2. Turn off the phone's Wi-Fi so it uses mobile data.
3. Open the same saved server and send a short message. It should connect through OpenZiti without a VPN or a second server entry.
4. Return to the home network. A compatible phone app should prefer the verified local route.

If remote setup fails, check enrollment expiry, service Bind/Dial policy, controller/router reachability and the connector's foreground process. Never fix this by forwarding the LAN port publicly or disabling TLS verification.
