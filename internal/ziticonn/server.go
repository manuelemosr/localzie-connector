// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

// Package ziticonn exposes explicitly configured loopback APIs over OpenZiti.
// Ziti is transport only: independent pinned TLS and device tokens protect the
// APIs even when the controller or an edge router is compromised.
package ziticonn

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/manuelemosr/localzie-connector/internal/agentbridge"
	"github.com/manuelemosr/localzie-connector/internal/identity"
	"github.com/manuelemosr/localzie-connector/internal/store"

	"github.com/openziti/sdk-golang/ziti"
	"golang.org/x/net/netutil"
)

type Endpoint struct {
	ID          string `json:"id"`
	InstanceID  string `json:"instanceID,omitempty"`
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	URL         string `json:"url,omitempty"`
	APIKeyEnv   string `json:"api_key_env,omitempty"`
	Username    string `json:"username,omitempty"`
	PasswordEnv string `json:"password_env,omitempty"`
	Directory   string `json:"directory,omitempty"`
}

type Config struct {
	IdentityFile       string     `json:"identity_file"`
	Service            string     `json:"service"`
	Name               string     `json:"name"`
	Endpoints          []Endpoint `json:"endpoints"`
	LANPort            int        `json:"lan_port,omitempty"`
	LocalOnly          bool       `json:"local_only,omitempty"`
	RequireRemoteReady bool       `json:"-"`
	stateDir           string
}

var safeID = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

const (
	maxPendingPairings = 4
	pairingBodyTimeout = 5 * time.Second
	preAuthReadTimeout = 10 * time.Second
)

func Load(path string) (Config, error) {
	var c Config
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 64<<10))
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil {
		return c, err
	}
	if (!c.LocalOnly && (c.IdentityFile == "" || !safeID.MatchString(c.Service))) || strings.TrimSpace(c.Name) == "" || len(c.Endpoints) == 0 || len(c.Endpoints) > 16 {
		return c, errors.New("identity_file, service, name and 1-16 endpoints are required")
	}
	if !filepath.IsAbs(c.IdentityFile) {
		c.IdentityFile = filepath.Join(filepath.Dir(path), c.IdentityFile)
	}
	seen := map[string]bool{}
	for i := range c.Endpoints {
		e := &c.Endpoints[i]
		if !safeID.MatchString(e.ID) || seen[e.ID] || strings.TrimSpace(e.Name) == "" {
			return c, errors.New("endpoint IDs must be unique and names must be present")
		}
		seen[e.ID] = true
		if e.Kind != "openai-compatible" && e.Kind != "opencode" && e.Kind != "hermes" && e.Kind != "codex" {
			return c, errors.New("unsupported endpoint kind")
		}
		if e.Kind == "codex" {
			if e.URL != "" || e.APIKeyEnv != "" || e.PasswordEnv != "" {
				return c, errors.New("Codex uses a private local app-server process, not a URL or runtime key")
			}
			if e.Directory == "" {
				e.Directory, _ = os.Getwd()
			}
			if !filepath.IsAbs(e.Directory) {
				return c, errors.New("Codex directory must be an absolute project path")
			}
			continue
		}
		if e.Kind == "opencode" && e.URL == "" {
			if err := discoverOpenCode(e); err != nil {
				return c, err
			}
		}
		if e.Kind == "hermes" && (e.Username == "" || e.PasswordEnv == "") {
			return c, errors.New("Hermes requires its dashboard username and password_env")
		}
		u, err := url.Parse(e.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || (u.Hostname() != "127.0.0.1" && u.Hostname() != "::1") || u.Port() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return c, errors.New("endpoints require an explicit http/https loopback address and port, without URL credentials")
		}
		for _, env := range []string{e.APIKeyEnv, e.PasswordEnv} {
			if env != "" && os.Getenv(env) == "" {
				return c, fmt.Errorf("credential environment variable %s is unset", env)
			}
		}
	}
	return c, nil
}

func OpenState(dir string) (identity.Identity, *store.Store, error) {
	id, err := identity.Ensure(dir)
	if err != nil {
		return id, nil, err
	}
	p := filepath.Join(dir, "remote.db")
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return id, nil, err
	}
	f.Close()
	if err = os.Chmod(p, 0600); err != nil {
		return id, nil, err
	}
	db, err := store.Open(p)
	return id, db, err
}

// Handler never accepts controller-provided forwarding configuration. The
// allowlist is read from the owner's local configuration file only.
func Handler(root context.Context, c Config, db *store.Store) http.Handler {
	return newHandler(root, c, db, nil)
}

func newHandler(root context.Context, c Config, db *store.Store, authorized func(context.Context, string), managed ...map[string]http.Handler) http.Handler {
	mux := http.NewServeMux()
	journal := newRequestJournal(c.stateDir)
	mux.HandleFunc("GET /localzie/requests/{request}", func(w http.ResponseWriter, r *http.Request) {
		device, _ := r.Context().Value(inferenceDeviceKey{}).(string)
		journal.status(w, r, device)
	})
	mux.HandleFunc("GET /localzie/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(manifest(r.Context(), c, db))
	})
	jobs := newInferenceJobs()
	bareEndpoints := map[string]bool{}
	for _, endpoint := range c.Endpoints {
		if endpoint.Kind == "openai-compatible" {
			bareEndpoints[endpoint.ID] = true
		}
	}
	mux.HandleFunc("POST /localzie/inference/{endpoint}/{operation}/stop", func(w http.ResponseWriter, r *http.Request) {
		endpoint, operation := r.PathValue("endpoint"), r.PathValue("operation")
		if !bareEndpoints[endpoint] || !safeID.MatchString(operation) {
			http.Error(w, "Unknown inference operation", 404)
			return
		}
		device, _ := r.Context().Value(inferenceDeviceKey{}).(string)
		if err := jobs.stop(inferenceKey{device, endpoint, operation}); err != nil {
			http.Error(w, err.Error(), 429)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	})
	pendingPairings := make(chan struct{}, maxPendingPairings)
	mux.HandleFunc("POST /localzie/pair", func(w http.ResponseWriter, r *http.Request) {
		// Pairing is unauthenticated at the home layer. Never let an incomplete
		// body consume every connection or keep the connection alive afterward.
		r.Close = true
		w.Header().Set("Connection", "close")
		controller := http.NewResponseController(w)
		// Do not let net/http's post-handler body drain hold a rejected or
		// completed pairing connection open on trailing, unfinished input.
		defer func() { _ = controller.SetReadDeadline(time.Now()) }()
		if err := controller.SetReadDeadline(time.Now().Add(pairingBodyTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
			http.Error(w, "Could not bound the pairing request", http.StatusServiceUnavailable)
			return
		}
		select {
		case pendingPairings <- struct{}{}:
			defer func() { <-pendingPairings }()
		default:
			w.Header().Set("Retry-After", "5")
			http.Error(w, "Too many pending pairing requests", http.StatusTooManyRequests)
			return
		}
		var req struct {
			Token string `json:"token"`
			Name  string `json:"name"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			var timeout net.Error
			if errors.As(err, &timeout) && timeout.Timeout() {
				http.Error(w, "The pairing request timed out", http.StatusRequestTimeout)
				return
			}
			http.Error(w, "Invalid pairing request", http.StatusBadRequest)
			return
		}
		if len(req.Name) == 0 || len(req.Name) > 128 {
			http.Error(w, "Invalid pairing request", 400)
			return
		}
		_, token, err := db.UpgradePairing(r.Context(), req.Token, req.Name, r.Header.Get("X-Localzie-Token"))
		if err != nil {
			http.Error(w, "Invalid or expired pairing code", 401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"token": token})
	})
	for _, endpoint := range c.Endpoints {
		e := endpoint
		if e.Kind == "codex" {
			var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "Codex is not running", 503) })
			if len(managed) > 0 && managed[0][e.ID] != nil {
				handler = managed[0][e.ID]
			}
			prefix := "/api/" + e.ID
			mux.Handle(prefix+"/", http.StripPrefix(prefix, handler))
			continue
		}
		target, _ := url.Parse(e.URL)
		proxy := httputil.NewSingleHostReverseProxy(target)
		proxy.FlushInterval = -1
		proxy.ErrorLog = log.New(io.Discard, "", 0)
		proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, "The selected home service is unavailable", 502)
		}
		original := proxy.Director
		proxy.Director = func(r *http.Request) {
			original(r)
			r.Host = target.Host
			r.Header.Del("X-Localzie-Token")
			r.Header.Del("X-Localzie-Endpoint")
			r.Header.Del("Forwarded")
			r.Header.Del("X-Forwarded-For")
			r.Header.Del("X-Forwarded-Host")
			r.Header.Del("X-Forwarded-Proto")
			if e.APIKeyEnv != "" {
				r.Header.Set("Authorization", "Bearer "+os.Getenv(e.APIKeyEnv))
			}
			if e.PasswordEnv != "" && e.Kind != "hermes" {
				r.SetBasicAuth(e.Username, os.Getenv(e.PasswordEnv))
			}
		}
		prefix := "/api/" + e.ID
		forward := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if e.Kind == "hermes" && r.Method == http.MethodPost && r.URL.Path == "/auth/password-login" {
				var form map[string]string
				if json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&form) != nil {
					http.Error(w, "Invalid harness sign-in request", 400)
					return
				}
				form["username"] = e.Username
				form["password"] = os.Getenv(e.PasswordEnv)
				body, err := json.Marshal(form)
				if err != nil {
					http.Error(w, "Invalid harness sign-in request", 400)
					return
				}
				r.Body.Close()
				r.Body = io.NopCloser(bytes.NewReader(body))
				r.ContentLength = int64(len(body))
				r.Header.Del("Content-Encoding")
				r.Header.Set("Content-Type", "application/json")
			}
			// The phone-owned OpenAI adapter may detach while a response is
			// running. Drain that accepted inference to completion, bounded by
			// host shutdown or the job limit, without retaining its content.
			if e.Kind == "openai-compatible" && r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/chat/completions") {
				operation := r.Header.Get("X-Localzie-Inference")
				if !safeID.MatchString(operation) {
					http.Error(w, "Update Localzie to send a stoppable inference operation", 400)
					return
				}
				device, _ := r.Context().Value(inferenceDeviceKey{}).(string)
				base := root
				if r.Context().Value(acceptedRequestKey{}) == true {
					base = r.Context()
				}
				ctx, release, err := jobs.begin(base, inferenceKey{device, e.ID, operation})
				if err != nil {
					status := 429
					if errors.Is(err, errInferenceStopped) {
						status = 409
					}
					http.Error(w, err.Error(), status)
					return
				}
				defer release()
				r.Header.Del("X-Localzie-Inference")
				proxy.ServeHTTP(&detachedWriter{ResponseWriter: w}, r.WithContext(ctx))
				return
			}
			proxy.ServeHTTP(w, r)
		})
		mux.Handle(prefix+"/", http.StripPrefix(prefix, forward))
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.URL.Path != "/localzie/pair" {
			device, ok := db.AuthenticateDevice(r.Context(), r.Header.Get("X-Localzie-Token"))
			if !ok {
				r.Close = true
				w.Header().Set("Connection", "close")
				_ = http.NewResponseController(w).SetReadDeadline(time.Now())
				http.Error(w, "Pair this phone before connecting", 401)
				return
			}
			if strings.HasPrefix(r.URL.Path, "/api/") {
				parts := strings.Split(r.URL.Path, "/")
				expected := r.Header.Get("X-Localzie-Endpoint")
				if len(parts) > 2 && expected != "" {
					for _, e := range c.Endpoints {
						if e.ID == parts[2] && e.InstanceID != expected {
							http.Error(w, "This endpoint changed. Reconnect before sending to it.", 409)
							return
						}
					}
				}
			}
			// Admission is complete. Slow authenticated uploads and hijacked
			// WebSockets must not inherit the short pre-authentication budget.
			_ = http.NewResponseController(w).SetReadDeadline(time.Time{})
			if authorized != nil {
				authorized(r.Context(), device)
			}
			r = r.WithContext(context.WithValue(agentbridge.WithDevice(r.Context(), device), inferenceDeviceKey{}, device))
		}
		if r.Method == "POST" && r.URL.Path != "/localzie/pair" && r.Header.Get("X-Localzie-Request") != "" {
			device, _ := r.Context().Value(inferenceDeviceKey{}).(string)
			journal.serve(root, w, r, device, mux)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

type detachedWriter struct {
	http.ResponseWriter
	detached bool
}

func (w *detachedWriter) Write(data []byte) (int, error) {
	if !w.detached {
		if _, err := w.ResponseWriter.Write(data); err != nil {
			w.detached = true
		}
	}
	return len(data), nil
}

func (w *detachedWriter) Flush() {
	if !w.detached {
		if f, ok := w.ResponseWriter.(http.Flusher); ok {
			f.Flush()
		}
	}
}

// NewContext waits for the SDK's asynchronous controller-version discovery
// before authentication mutates its shared HTTP transport. This avoids the
// upstream v1.8.3 startup race without modifying vendor code.
func NewContext(ctx context.Context, identityFile string) (ziti.Context, error) {
	z, err := ziti.NewContextFromFile(identityFile)
	if err != nil {
		return nil, err
	}
	ready := make(chan struct{})
	go func() {
		if implementation, ok := z.(*ziti.ContextImpl); ok {
			implementation.CtrlClt.API.ControllerSupportsOidc()
		}
		close(ready)
	}()
	select {
	case <-ready:
		return z, nil
	case <-ctx.Done():
		z.Close()
		return nil, ctx.Err()
	}
}

func disconnect(conn net.Conn) {
	// Revoke/host-shutdown is an abrupt detach. Closing the underlying Ziti
	// stream avoids concurrent TLS close-notify writes and SDK deadlines.
	if secure, ok := conn.(*tls.Conn); ok {
		secure.NetConn().Close()
	} else {
		conn.Close()
	}
}

func Serve(ctx context.Context, c Config, dir string) error {
	return ServeWithReady(ctx, c, dir, nil)
}

// ServeWithReady calls ready only after local listeners and managed harnesses
// are initialized. A callback failure shuts the connector down.
func ServeWithReady(ctx context.Context, c Config, dir string, ready func() error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := ensureEndpointIDs(&c, dir); err != nil {
		return err
	}
	managed := map[string]http.Handler{}
	for _, endpoint := range c.Endpoints {
		if endpoint.Kind == "codex" {
			adapter, err := agentbridge.StartCodex(ctx, endpoint.Directory)
			if err != nil {
				return fmt.Errorf("could not start Codex: %w", err)
			}
			defer adapter.Close()
			managed[endpoint.ID] = agentbridge.Handler(ctx, adapter)
		}
	}
	c.stateDir = dir
	id, db, err := OpenState(dir)
	if err != nil {
		return err
	}
	defer db.Close()
	cert, err := tls.LoadX509KeyPair(id.CertificatePath, id.PrivateKeyPath)
	if err != nil {
		return err
	}
	tlsConfig, err := homeTLS(dir, cert)
	if err != nil {
		return err
	}
	remoteListeners := make(chan net.Listener)
	remoteReady := make(chan struct{})
	var listeners []net.Listener
	lanListeners := map[string]net.Listener{}
	for _, address := range lanAddresses(c.LANPort) {
		listener, e := net.Listen("tcp", address)
		if e == nil {
			listeners = append(listeners, listener)
			lanListeners[address] = listener
		}
	}
	if !c.LocalOnly {
		// Ziti startup is independent: an unavailable controller must never block LAN.
		go func() {
			for ctx.Err() == nil {
				z, e := NewContext(ctx, c.IdentityFile)
				if e == nil {
					options := ziti.DefaultListenOptions()
					options.WaitForNEstablishedListeners = 1
					options.ConnectTimeout = 8 * time.Second
					listener, e := z.ListenWithOptions(c.Service, options)
					if e == nil {
						select {
						case remoteListeners <- listener:
							close(remoteReady)
						case <-ctx.Done():
							listener.Close()
						}
						<-ctx.Done()
					}
					z.Close()
				}
				select {
				case <-time.After(3 * time.Second):
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()
	closeAdvertisement := advertiseHome(ctx, c, db, id.Fingerprint)
	var connectionsMu sync.Mutex
	connections := map[net.Conn]string{}
	type connectionKey struct{}
	server := &http.Server{ReadHeaderTimeout: 15 * time.Second, ReadTimeout: preAuthReadTimeout, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	server.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
		return context.WithValue(ctx, connectionKey{}, conn)
	}
	server.ConnState = func(conn net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			connectionsMu.Lock()
			delete(connections, conn)
			connectionsMu.Unlock()
		}
	}
	server.Handler = newHandler(ctx, c, db, func(request context.Context, device string) {
		if conn, ok := request.Value(connectionKey{}).(net.Conn); ok {
			connectionsMu.Lock()
			connections[conn] = device
			connectionsMu.Unlock()
		}
	}, managed)
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				connectionsMu.Lock()
				active := make([]net.Conn, 0, len(connections))
				for conn := range connections {
					active = append(active, conn)
				}
				connectionsMu.Unlock()
				for _, conn := range active {
					disconnect(conn)
				}
				return
			case <-ticker.C:
				connectionsMu.Lock()
				active := make(map[net.Conn]string, len(connections))
				for conn, device := range connections {
					active[conn] = device
				}
				connectionsMu.Unlock()
				for conn, device := range active {
					if !db.DeviceActive(ctx, device) {
						disconnect(conn)
						connectionsMu.Lock()
						delete(connections, conn)
						connectionsMu.Unlock()
					}
				}
			}
		}
	}()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		server.Shutdown(shutdown)
	}()
	errorsCh := make(chan error, len(listeners)+1)
	serve := func(l net.Listener) {
		e := server.Serve(tls.NewListener(netutil.LimitListener(l, 64), tlsConfig))
		if !errors.Is(e, http.ErrServerClosed) && !errors.Is(e, net.ErrClosed) {
			select {
			case errorsCh <- e:
			case <-ctx.Done():
			}
		}
	}
	for _, l := range listeners {
		go serve(l)
	}
	go func() {
		defer func() { closeAdvertisement() }()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		previous := strings.Join(lanAddresses(c.LANPort), ",")
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				addresses := lanAddresses(c.LANPort)
				current := strings.Join(addresses, ",")
				wanted := map[string]bool{}
				for _, address := range addresses {
					wanted[address] = true
					if lanListeners[address] == nil {
						if listener, err := net.Listen("tcp", address); err == nil {
							lanListeners[address] = listener
							go serve(listener)
						}
					}
				}
				for address, listener := range lanListeners {
					if !wanted[address] {
						_ = listener.Close()
						delete(lanListeners, address)
					}
				}
				if current != previous {
					closeAdvertisement()
					closeAdvertisement = advertiseHome(ctx, c, db, id.Fingerprint)
					previous = current
				}
			}
		}
	}()
	go func() {
		select {
		case l := <-remoteListeners:
			serve(l)
		case <-ctx.Done():
		}
	}()
	if len(listeners) == 0 && c.LocalOnly {
		return errors.New("no private LAN interface could be opened")
	}
	if c.RequireRemoteReady {
		timer := time.NewTimer(25 * time.Second)
		defer timer.Stop()
		select {
		case <-remoteReady:
		case <-timer.C:
			return errors.New("Localzie could not establish the remote route. Check your internet connection and try setup again")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if ready != nil {
		if err := ready(); err != nil {
			return err
		}
	}
	select {
	case <-ctx.Done():
		return nil
	case err := <-errorsCh:
		return err
	}

}

type Pairing struct {
	Version     int        `json:"version"`
	Name        string     `json:"name"`
	Service     string     `json:"service"`
	Fingerprint string     `json:"fingerprint"`
	Token       string     `json:"token"`
	Enrollment  string     `json:"enrollment"`
	Expires     time.Time  `json:"expires"`
	Endpoints   []Endpoint `json:"endpoints"`
	HomeID      string     `json:"homeID,omitempty"`
	Addresses   []string   `json:"addresses,omitempty"`
	Authority   string     `json:"authority,omitempty"`
}

func Pair(c Config, dir, jwtPath string) (string, error) {
	if c.LocalOnly {
		return PairWithEnrollment(c, dir, "")
	}
	jwt, err := os.ReadFile(jwtPath)
	if err != nil {
		return "", err
	}
	return PairWithEnrollment(c, dir, string(jwt))
}

// PairWithEnrollment keeps managed enrollment credentials in memory. The
// actual one-use pairing secret and pinned home certificate originate here.
func PairWithEnrollment(c Config, dir, jwt string) (string, error) {
	if err := ensureEndpointIDs(&c, dir); err != nil {
		return "", err
	}
	jwt = strings.TrimSpace(jwt)
	if !c.LocalOnly && (len(jwt) > 8192 || strings.Count(jwt, ".") != 2) {
		return "", errors.New("phone enrollment file is not a JWT")
	}
	id, db, err := OpenState(dir)
	if err != nil {
		return "", err
	}
	defer db.Close()
	token, expires, err := db.CreatePairing(context.Background(), 10*time.Minute)
	if err != nil {
		return "", err
	}
	public := make([]Endpoint, 0, len(c.Endpoints))
	for _, e := range c.Endpoints {
		public = append(public, Endpoint{ID: e.ID, Name: e.Name, Kind: e.Kind, InstanceID: e.InstanceID})
	}
	legacy, err := tls.LoadX509KeyPair(id.CertificatePath, id.PrivateKeyPath)
	if err != nil {
		return "", err
	}
	if _, err = homeTLS(dir, legacy); err != nil {
		return "", err
	}
	c.stateDir = dir
	authority := manifest(context.Background(), c, db).Authority
	homeID, _ := db.ConnectorID(context.Background())
	service := c.Service
	if service == "" {
		service = "home-" + homeID
	}
	// Remote enrollment can bootstrap without address hints, then obtain them
	// from the authenticated home manifest. Keep the QR within byte capacity.
	var addresses []string
	if c.LocalOnly {
		addresses = lanAddresses(c.LANPort)
	}
	b, err := json.Marshal(Pairing{Version: 1, Name: c.Name, Service: service, Fingerprint: id.Fingerprint, Token: token, Enrollment: jwt, Expires: expires, Endpoints: public, HomeID: homeID, Addresses: addresses, Authority: authority})
	if err != nil {
		return "", err
	}
	return "LZ1:" + base64.RawURLEncoding.EncodeToString(b), nil
}
