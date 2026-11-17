# Third-party dependencies

Localzie Connector uses modules downloaded by Go; third-party source is not vendored here. Their original licenses and notices remain in their module distributions. This project's Apache 2.0 license does not replace those terms. `go.mod` and `go.sum` are the exact dependency inventory, including transitive dependencies.

Direct dependencies at extraction:

| Module | License | Purpose |
| --- | --- | --- |
| github.com/coder/websocket | ISC | Harness WebSockets / integration fixtures |
| github.com/hashicorp/mdns | MIT | LAN discovery |
| github.com/openziti/sdk-golang | Apache-2.0 | Remote network transport and enrollment |
| github.com/skip2/go-qrcode | MIT | Setup-payload QR capacity regression tests; the CLI does not generate QR images |
| golang.org/x/net | BSD-3-Clause | Network connection limiting |
| modernc.org/sqlite | BSD-3-Clause | Local persistence |

A distributor publishing linked binaries must also distribute the applicable dependency license/notice material, including transitive dependencies. No prebuilt binary release is included in this initial source publication.
