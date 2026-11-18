# Use Localzie away from home

Remote connectivity is managed by Localzie. Customers do not install or administer a network, open router ports, or run a phone VPN.

Use the same connection and server list in [Localzie for iPhone](https://apps.apple.com/app/id6813506926). At home, the app prefers a verified local route. Away from home, a Localzie-enabled remote connection uses the secure remote route.

Remote setup is currently available on computers authorized for the Localzie preview. On one of those computers, stop any running connector and run `localzie-connector setup`. Select the services to share, then wait for **Remote pairing is ready**. Keep the terminal open and paste the code into **Connect to a server** on your iPhone. The phone may be on mobile data or another Wi-Fi network.

Setup automatically enables this on authorized preview computers. The terminal says **Remote pairing is ready** when the code can be used away from home. On other computers it prints **This code works on your home network**.

Start with [setup](setup.md). Developers can read the [architecture](architecture.md) and [security model](security.md).
