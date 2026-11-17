// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package ziticonn

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/manuelemosr/localzie-connector/internal/discovery"
	"github.com/manuelemosr/localzie-connector/internal/store"
)

const defaultLANPort = 7443

type homeManifest struct {
	Version   int        `json:"version"`
	HomeID    string     `json:"homeID"`
	Addresses []string   `json:"addresses"`
	Authority string     `json:"authority,omitempty"`
	Endpoints []Endpoint `json:"endpoints"`
	Replay    bool       `json:"replay"`
}

func publicEndpoints(c Config) []Endpoint {
	out := make([]Endpoint, 0, len(c.Endpoints))
	for _, e := range c.Endpoints {
		out = append(out, Endpoint{ID: e.ID, Name: e.Name, Kind: e.Kind, InstanceID: e.InstanceID})
	}
	return out
}

// Only directly bound private interfaces are advertised/listened on. Never
// bind a wildcard, a public address, loopback, or a VPN interface.
func lanAddresses(port int) []string {
	if port == 0 {
		port = defaultLANPort
	}
	interfaces, _ := net.Interfaces()
	var result []string
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 || strings.HasPrefix(iface.Name, "utun") || strings.HasPrefix(iface.Name, "tun") {
			continue
		}
		addresses, _ := iface.Addrs()
		for _, a := range addresses {
			ip, _, err := net.ParseCIDR(a.String())
			if err != nil {
				continue
			}
			if !ip.IsPrivate() && !ip.IsLinkLocalUnicast() {
				continue
			}
			host := ip.String()
			if ip.To4() == nil && ip.IsLinkLocalUnicast() {
				host += "%" + iface.Name
			}
			result = append(result, net.JoinHostPort(host, strconv.Itoa(port)))
		}
	}
	return result
}

func manifest(ctx context.Context, c Config, db *store.Store) homeManifest {
	id, _ := db.ConnectorID(ctx)
	authority, _ := os.ReadFile(filepath.Join(c.stateDir, "home-ca.crt"))
	block, _ := pem.Decode(authority)
	encoded := ""
	if block != nil {
		encoded = base64.StdEncoding.EncodeToString(block.Bytes)
	}
	return homeManifest{2, id, lanAddresses(c.LANPort), encoded, publicEndpoints(c), true}
}

// The old leaf remains available to old clients (IP/no SNI). New clients use
// localhost SNI only after obtaining this home CA over their exact-pinned TLS.
// The CA is app-scoped; nothing is installed in the phone's system trust store.
func homeTLS(dir string, legacy tls.Certificate) (*tls.Config, error) {
	caPath, keyPath := filepath.Join(dir, "home-ca.crt"), filepath.Join(dir, "home-ca.key")
	caPEM, err := os.ReadFile(caPath)
	if errors.Is(err, os.ErrNotExist) {
		key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if e != nil {
			return nil, e
		}
		now := time.Now()
		template := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: "Localzie home authority"}, NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(10, 0, 0), IsCA: true, BasicConstraintsValid: true, MaxPathLen: 0, MaxPathLenZero: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
		der, e := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
		if e != nil {
			return nil, e
		}
		raw, e := x509.MarshalECPrivateKey(key)
		if e != nil {
			return nil, e
		}
		if e = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: raw}), 0600); e != nil {
			return nil, e
		}
		caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		if e = os.WriteFile(caPath, caPEM, 0600); e != nil {
			return nil, e
		}
	} else if err != nil {
		return nil, err
	}
	caPair, err := tls.LoadX509KeyPair(caPath, keyPath)
	if err != nil {
		return nil, err
	}
	ca, err := x509.ParseCertificate(caPair.Certificate[0])
	if err != nil {
		return nil, err
	}
	var mu sync.Mutex
	var leaf *tls.Certificate
	renew := func() (*tls.Certificate, error) {
		mu.Lock()
		defer mu.Unlock()
		if leaf != nil && time.Until(leaf.Leaf.NotAfter) > 30*24*time.Hour {
			return leaf, nil
		}
		key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if e != nil {
			return nil, e
		}
		now := time.Now()
		template := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: "Localzie home"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(90 * 24 * time.Hour), DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
		der, e := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caPair.PrivateKey)
		if e != nil {
			return nil, e
		}
		parsed, e := x509.ParseCertificate(der)
		if e != nil {
			return nil, e
		}
		leaf = &tls.Certificate{Certificate: [][]byte{der, ca.Raw}, PrivateKey: key, Leaf: parsed}
		return leaf, nil
	}
	if _, err := renew(); err != nil {
		return nil, err
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		if hello.ServerName == "localhost" {
			return renew()
		}
		return &legacy, nil
	}}, nil
}

func serial() *big.Int {
	n, e := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if e != nil {
		panic(e)
	}
	return n
}

func advertiseHome(ctx context.Context, c Config, db *store.Store, fingerprint string) func() {
	id, _ := db.ConnectorID(ctx)
	port := c.LANPort
	if port == 0 {
		port = defaultLANPort
	}
	ad, err := discovery.Start(id, fingerprint, port, nil)
	if err != nil {
		return func() {}
	}
	return func() { _ = ad.Close() }
}

// Repurposing an endpoint gets a new namespace; restarting or changing routes
// does not. The registry contains a config digest, never credentials or URLs.
func ensureEndpointIDs(c *Config, dir string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	type entry struct {
		Digest string
		ID     string
	}
	entries := map[string]entry{}
	path := filepath.Join(dir, "endpoints.json")
	if data, err := os.ReadFile(path); err == nil {
		if err = json.Unmarshal(data, &entries); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for i := range c.Endpoints {
		e := &c.Endpoints[i]
		raw := sha256.Sum256([]byte(e.Kind + "\x00" + e.URL + "\x00" + e.Directory))
		digest := hex.EncodeToString(raw[:])
		old := entries[e.ID]
		if old.Digest != digest || old.ID == "" {
			var b [16]byte
			if _, err := rand.Read(b[:]); err != nil {
				return err
			}
			old = entry{digest, hex.EncodeToString(b[:])}
			entries[e.ID] = old
		}
		e.InstanceID = old.ID
	}
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	if err = os.WriteFile(path+".tmp", data, 0600); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}
