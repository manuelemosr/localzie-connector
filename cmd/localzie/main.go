// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/manuelemosr/localzie-connector/internal/connector"
	"github.com/manuelemosr/localzie-connector/internal/discovery"
	"github.com/manuelemosr/localzie-connector/internal/harness"
	"github.com/manuelemosr/localzie-connector/internal/identity"
	"github.com/manuelemosr/localzie-connector/internal/protocol"
	"github.com/manuelemosr/localzie-connector/internal/runtime"
	"github.com/manuelemosr/localzie-connector/internal/store"
)

const version = "0.1.0-dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "pair":
		err = pair(os.Args[2:])
	case "doctor":
		err = doctor(os.Args[2:])
	case "discover":
		err = discoverCommand(os.Args[2:])
	case "identity":
		err = identityCommand(os.Args[2:])
	case "version":
		fmt.Println(version)
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "localzie:", err)
		os.Exit(1)
	}
}

func doctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	runtimeName := fs.String("runtime", "auto", "runtime: auto or openai-compatible")
	runtimeURL := fs.String("runtime-url", "", "runtime base URL")
	apiKeyEnvironment := fs.String("api-key-env", "", "environment variable containing a compatible runtime API key")
	opencodeURL, opencodeUsername, opencodePasswordEnv, harnessDirectory, _ := addOpenCodeFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	adapter, err := makeRuntime(*runtimeName, *runtimeURL, *apiKeyEnvironment)
	result := map[string]any{"harnesses": harness.NewProber().All(context.Background())}
	if err != nil {
		result["runtime_error"] = err.Error()
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		models, modelErr := adapter.Models(ctx)
		result["runtime"] = adapter.Name()
		result["models"] = models
		if modelErr != nil {
			result["runtime_error"] = modelErr.Error()
		}
	}
	if opencodeClient, err := makeOpenCodeClient(*opencodeURL, *opencodeUsername, *opencodePasswordEnv, *harnessDirectory); err != nil {
		result["opencode_error"] = err.Error()
	} else if opencodeClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		status := harness.ServerStatus{Attached: true, URL: opencodeClient.URL()}
		if version, err := opencodeClient.Health(ctx); err == nil {
			status.Healthy = true
			status.Version = version
		} else {
			status.Problem = err.Error()
		}
		if providers, err := opencodeClient.ConfigProviders(ctx); err == nil {
			count := 0
			for _, provider := range providers {
				for _, model := range provider.Models {
					if model.Status == "" || model.Status == "active" {
						count++
					}
				}
			}
			status.ModelCount = count
		}
		result["opencode"] = status
	}
	encoded, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(encoded))
	return nil
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	dataDir := fs.String("data-dir", defaultDataDir(), "connector data directory")
	listen := fs.String("listen", ":9443", "TLS listen address")
	runtimeName := fs.String("runtime", "auto", "runtime: auto or openai-compatible")
	runtimeURL := fs.String("runtime-url", "", "runtime base URL (uses the selected runtime default when empty)")
	apiKeyEnvironment := fs.String("api-key-env", "", "environment variable containing a compatible runtime API key")
	relayBaseURL := fs.String("relay-base-url", "", "relay base URL (remote transport enabled when set)")
	relayCredentialFile := fs.String("relay-credential-file", "", "0600 relay credential file")
	opencodeURL, opencodeUsername, opencodePasswordEnv, harnessDirectory, harnessAllowUnverified := addOpenCodeFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	var relayCredential string
	if *relayBaseURL != "" {
		if *relayCredentialFile == "" {
			return fmt.Errorf("relay credential file is required when relay is configured")
		}
		var secretErr error
		relayCredential, secretErr = readSecretFile(*relayCredentialFile)
		if secretErr != nil {
			return secretErr
		}
	}
	id, db, err := open(*dataDir)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.RecoverInterrupted(context.Background()); err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	adapter, err := makeRuntime(*runtimeName, *runtimeURL, *apiKeyEnvironment)
	if err != nil {
		// An attached OpenCode server is a usable connector on its own; a
		// harness-only machine must not be forced to run a model runtime.
		if *opencodeURL == "" {
			return err
		}
		logger.Warn("no model runtime found; serving the OpenCode harness backend only", "error", err)
		if *runtimeName == "auto" && *runtimeURL == "" {
			// Auto discovery is environmental: the model runtime may simply
			// start later than the connector. Keep retrying lazily instead of
			// pinning the startup failure for the process lifetime.
			apiKeyEnvironment := *apiKeyEnvironment
			adapter = newLazyRuntime(err.Error(), func() (runtime.Adapter, error) {
				return makeRuntime("auto", "", apiKeyEnvironment)
			})
		} else {
			// An explicit runtime configuration that fails is a config error;
			// retrying a fixed bad configuration is pointless.
			adapter = &unavailableRuntime{reason: err.Error()}
		}
	}
	root, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	handler, err := connector.NewServer(root, version, db, adapter, logger)
	if err != nil {
		return err
	}
	opencodeClient, err := makeOpenCodeClient(*opencodeURL, *opencodeUsername, *opencodePasswordEnv, *harnessDirectory)
	if err != nil {
		return err
	}
	if opencodeClient != nil {
		if err := probeOpenCode(opencodeClient); err != nil {
			return err
		}
		handler.AttachOpenCode(opencodeClient, *harnessAllowUnverified)
		logger.Info("harness backend attached", "opencode", opencodeClient.URL(), "directory", *harnessDirectory)
		if *harnessAllowUnverified {
			logger.Warn("harness conversations enabled without verified compatibility fixtures; approvals and tool activity are not translated")
		}
	}
	port, err := listenPort(*listen)
	if err != nil {
		return err
	}
	advertiser, discoveryErr := discovery.Start(handler.ConnectorID(), id.Fingerprint, port, advertisedAddresses(*listen, port))
	if discoveryErr != nil {
		logger.Warn("local discovery unavailable", "error", discoveryErr)
	} else {
		defer advertiser.Close()
		logger.Info("local discovery ready", "service", discovery.ServiceType)
	}
	server := &http.Server{Addr: *listen, Handler: handler.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13}}
	errors := make(chan error, 1)
	go func() {
		logger.Info("connector listening", "address", *listen, "fingerprint", id.Fingerprint)
		errors <- server.ListenAndServeTLS(id.CertificatePath, id.PrivateKeyPath)
	}()
	if *relayBaseURL != "" {
		cert, err := tls.LoadX509KeyPair(id.CertificatePath, id.PrivateKeyPath)
		if err != nil {
			return err
		}
		remoteListener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}})
		if err != nil {
			return err
		}
		remoteServer := &http.Server{Handler: handler.RemoteHandler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
		go func() { _ = remoteServer.Serve(remoteListener) }()
		client, err := connector.NewRelayClient(connector.RelayClientConfig{RelayBaseURL: *relayBaseURL, ConnectorID: handler.ConnectorID(), Credential: relayCredential, InternalAddress: remoteListener.Addr().String(), Logger: logger})
		if err != nil {
			return err
		}
		go func() { _ = client.Run(root) }()
		defer remoteServer.Shutdown(context.Background())
	}
	select {
	case <-root.Done():
		shutdownCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		return server.Shutdown(shutdownCtx)
	case err := <-errors:
		return err
	}
}

func makeRuntime(name, rawURL, apiKeyEnvironment string) (runtime.Adapter, error) {
	switch name {
	case "auto":
		apiKey, err := apiKeyFromEnvironment(apiKeyEnvironment)
		if err != nil {
			return nil, err
		}
		// An explicit URL is an override, not something discovery should probe;
		// it uses the provider-neutral OpenAI-compatible adapter, which also
		// serves Ollama.
		if rawURL != "" {
			return runtime.NewOpenAICompatible(rawURL, apiKey)
		}
		candidates, err := runtime.Discover(context.Background(), apiKey)
		if err != nil {
			return nil, err
		}
		switch len(candidates) {
		case 0:
			return nil, fmt.Errorf("no supported local runtime found; run 'localzie discover' or specify --runtime and --runtime-url")
		case 1:
			return makeRuntime(candidates[0].Runtime, candidates[0].URL, apiKeyEnvironment)
		default:
			choices := make([]string, 0, len(candidates))
			for _, candidate := range candidates {
				choices = append(choices, fmt.Sprintf("--runtime %s --runtime-url %s", candidate.Runtime, candidate.URL))
			}
			return nil, fmt.Errorf("multiple local runtimes found; choose one explicitly: %s", strings.Join(choices, "; "))
		}
	case "openai-compatible":
		apiKey, err := apiKeyFromEnvironment(apiKeyEnvironment)
		if err != nil {
			return nil, err
		}
		if rawURL == "" {
			rawURL = "http://127.0.0.1:1234"
		}
		return runtime.NewOpenAICompatible(rawURL, apiKey)
	default:
		return nil, fmt.Errorf("unsupported runtime %q", name)
	}
}

func apiKeyFromEnvironment(environment string) (string, error) {
	if environment == "" {
		return "", nil
	}
	key, ok := os.LookupEnv(environment)
	if !ok {
		return "", fmt.Errorf("API key environment variable %q is not set", environment)
	}
	return key, nil
}

// unavailableRuntime stands in for a model runtime when the connector serves
// only the OpenCode harness backend. Its failures surface through the normal
// runtime paths: the models listing degrades to harness-only, and any runtime
// conversation fails its job with the original discovery error.
type unavailableRuntime struct{ reason string }

func (u *unavailableRuntime) Name() string { return "unavailable" }
func (u *unavailableRuntime) Models(context.Context) ([]protocol.Model, error) {
	return nil, fmt.Errorf("no model runtime is available: %s", u.reason)
}
func (u *unavailableRuntime) Stream(context.Context, runtime.StreamRequest, func(string) error) error {
	return fmt.Errorf("no model runtime is available: %s", u.reason)
}

const lazyRuntimeRetryTTL = 15 * time.Second

// lazyRuntime keeps re-attempting runtime auto-discovery after a startup
// failure, so a model runtime launched later than the connector appears
// without a connector restart. Once discovery succeeds the adapter is pinned
// for the process lifetime.
type lazyRuntime struct {
	discover func() (runtime.Adapter, error)
	retryTTL time.Duration

	mu       sync.Mutex
	adapter  runtime.Adapter
	reason   string
	probedAt time.Time
}

func newLazyRuntime(reason string, discover func() (runtime.Adapter, error)) *lazyRuntime {
	return &lazyRuntime{discover: discover, retryTTL: lazyRuntimeRetryTTL, reason: reason}
}

func (l *lazyRuntime) Name() string {
	if adapter := l.resolved(); adapter != nil {
		return adapter.Name()
	}
	return "unavailable"
}

func (l *lazyRuntime) resolved() runtime.Adapter {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.adapter
}

func (l *lazyRuntime) resolve(ctx context.Context) (runtime.Adapter, error) {
	l.mu.Lock()
	if l.adapter != nil {
		adapter := l.adapter
		l.mu.Unlock()
		return adapter, nil
	}
	if !l.probedAt.IsZero() && time.Since(l.probedAt) < l.retryTTL {
		reason := l.reason
		l.mu.Unlock()
		return nil, fmt.Errorf("no model runtime is available yet: %s", reason)
	}
	l.probedAt = time.Now()
	l.mu.Unlock()
	adapter, err := l.discover()
	l.mu.Lock()
	defer l.mu.Unlock()
	if err != nil {
		l.reason = err.Error()
		return nil, err
	}
	l.adapter = adapter
	return adapter, nil
}

func (l *lazyRuntime) Models(ctx context.Context) ([]protocol.Model, error) {
	adapter, err := l.resolve(ctx)
	if err != nil {
		return nil, err
	}
	return adapter.Models(ctx)
}

func (l *lazyRuntime) Stream(ctx context.Context, request runtime.StreamRequest, onDelta func(string) error) error {
	adapter, err := l.resolve(ctx)
	if err != nil {
		return err
	}
	return adapter.Stream(ctx, request, onDelta)
}

func addOpenCodeFlags(fs *flag.FlagSet) (opencodeURL, opencodeUsername, opencodePasswordEnv, harnessDirectory *string, harnessAllowUnverified *bool) {
	opencodeURL = fs.String("opencode-url", "", "OpenCode server base URL (harness backend enabled when set; loopback only)")
	opencodeUsername = fs.String("opencode-username", "", "OpenCode basic-auth username (defaults to \"opencode\")")
	opencodePasswordEnv = fs.String("opencode-password-env", "", "environment variable containing the OpenCode basic-auth password")
	harnessDirectory = fs.String("harness-dir", "", "working directory OpenCode sessions run in (required when --opencode-url is set)")
	harnessAllowUnverified = fs.Bool("harness-allow-unverified", false, "enable unverified OpenCode conversations before compatibility fixtures pass")
	return
}

// makeOpenCodeClient builds the harness transport client. A nil, nil return
// means the harness backend is disabled.
func makeOpenCodeClient(rawURL, username, passwordEnvironment, directory string) (*harness.OpenCodeClient, error) {
	if rawURL == "" {
		if username != "" || passwordEnvironment != "" || directory != "" {
			return nil, fmt.Errorf("--opencode-url is required when other OpenCode flags are set")
		}
		return nil, nil
	}
	if directory == "" {
		return nil, fmt.Errorf("--harness-dir is required when --opencode-url is set; OpenCode sessions must be scoped to an explicit directory")
	}
	password, err := apiKeyFromEnvironment(passwordEnvironment)
	if err != nil {
		return nil, err
	}
	client, err := harness.NewOpenCode(rawURL, directory, username, password)
	if err != nil {
		return nil, fmt.Errorf("invalid OpenCode configuration: %w", err)
	}
	return client, nil
}

// probeOpenCode fails serve startup when the configured server is not usable.
// A silent attach would surface as confusing per-request failures later.
func probeOpenCode(client *harness.OpenCodeClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Health(ctx); err != nil {
		return fmt.Errorf("OpenCode server is not reachable: %w", err)
	}
	if _, err := client.ConfigProviders(ctx); err != nil {
		return fmt.Errorf("OpenCode providers could not be listed: %w", err)
	}
	return nil
}

func discoverCommand(args []string) error {
	fs := flag.NewFlagSet("discover", flag.ContinueOnError)
	jsonOut := fs.Bool("json", false, "emit JSON")
	apiKeyEnvironment := fs.String("api-key-env", "", "environment variable containing a compatible runtime API key")
	if err := fs.Parse(args); err != nil {
		return err
	}
	apiKey, err := apiKeyFromEnvironment(*apiKeyEnvironment)
	if err != nil {
		return err
	}
	candidates, err := runtime.Discover(context.Background(), apiKey)
	if err != nil {
		return err
	}
	if *jsonOut {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"candidates": candidates})
	}
	if len(candidates) == 0 {
		fmt.Println("No supported local runtime found. Start Ollama (served through its OpenAI-compatible API) or another OpenAI-compatible server, then run this command again.")
		return nil
	}
	for _, candidate := range candidates {
		fmt.Printf("%s\t%s\n", candidate.Runtime, candidate.URL)
	}
	return nil
}

func pair(args []string) error {
	fs := flag.NewFlagSet("pair", flag.ContinueOnError)
	dataDir := fs.String("data-dir", defaultDataDir(), "connector data directory")
	relayBaseURL := fs.String("relay-base-url", "", "relay base URL")
	relayCredentialFile := fs.String("relay-credential-file", "", "0600 relay credential file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	id, db, err := open(*dataDir)
	if err != nil {
		return err
	}
	defer db.Close()
	code, expires, err := db.CreatePairingCode(context.Background(), 10*time.Minute)
	if err != nil {
		return err
	}
	if *relayBaseURL != "" {
		token, err := readSecretFile(*relayCredentialFile)
		if err != nil {
			return err
		}
		if err := publishPairing(context.Background(), *relayBaseURL, db, id.Fingerprint, token, code, expires); err != nil {
			_ = db.InvalidatePairing(context.Background(), code)
			return fmt.Errorf("publish remote pairing code: %w", err)
		}
	}
	fmt.Println(code)
	return nil
}

func identityCommand(args []string) error {
	fs := flag.NewFlagSet("identity", flag.ContinueOnError)
	data := fs.String("data-dir", defaultDataDir(), "connector data directory")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	id, db, err := open(*data)
	if err != nil {
		return err
	}
	defer db.Close()
	cid, err := db.ConnectorID(context.Background())
	if err != nil {
		return err
	}
	if *jsonOut {
		return json.NewEncoder(os.Stdout).Encode(map[string]string{"connector_id": cid, "certificate_sha256": id.Fingerprint})
	}
	fmt.Println(cid)
	return nil
}

func listenPort(address string) (int, error) {
	_, rawPort, err := net.SplitHostPort(address)
	if err != nil {
		return 0, fmt.Errorf("invalid listen address %q: %w", address, err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("invalid listen port %q", rawPort)
	}
	return port, nil
}

func advertisedAddresses(listen string, port int) []string {
	host, _, err := net.SplitHostPort(listen)
	if err == nil && host != "" && host != "0.0.0.0" && host != "::" {
		return []string{"https://" + net.JoinHostPort(host, strconv.Itoa(port))}
	}
	return pairingAddresses("", port)
}

func pairingAddresses(override string, port int) []string {
	if override != "" {
		return []string{override}
	}
	type candidate struct {
		address string
		score   int
	}
	seen := map[string]bool{}
	var candidates []candidate
	interfaces, _ := net.Interfaces()
	for _, networkInterface := range interfaces {
		if networkInterface.Flags&net.FlagUp == 0 || networkInterface.Flags&net.FlagLoopback != 0 {
			continue
		}
		assigned, _ := networkInterface.Addrs()
		for _, address := range assigned {
			ip, _, err := net.ParseCIDR(address.String())
			if err != nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}
			host := "https://" + net.JoinHostPort(ip.String(), fmt.Sprint(port))
			if !seen[host] {
				seen[host] = true
				score := 3
				if ip.To4() != nil {
					score = 1
					if ip.IsPrivate() {
						score = 0
					}
				} else if ip.IsPrivate() {
					score = 2
				}
				candidates = append(candidates, candidate{address: host, score: score})
			}
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].score < candidates[j].score })
	addresses := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		addresses = append(addresses, candidate.address)
	}
	if len(addresses) == 0 {
		addresses = append(addresses, "https://127.0.0.1:"+fmt.Sprint(port))
	}
	return addresses
}

func open(dataDir string) (identity.Identity, *store.Store, error) {
	id, err := identity.Ensure(dataDir)
	if err != nil {
		return identity.Identity{}, nil, err
	}
	db, err := store.Open(filepath.Join(dataDir, "localzie.db"))
	if err != nil {
		return identity.Identity{}, nil, err
	}
	return id, db, nil
}
func defaultDataDir() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		return ".localzie"
	}
	return filepath.Join(dir, "Localzie")
}
func usage() {
	fmt.Fprintln(os.Stderr, "usage: localzie <serve|pair|identity|doctor|discover|version> [options]")
}
