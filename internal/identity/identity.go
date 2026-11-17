// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

type Identity struct {
	CertificatePath string
	PrivateKeyPath  string
	Fingerprint     string
}

func Ensure(dataDir string) (Identity, error) {
	certPath := filepath.Join(dataDir, "connector.crt")
	keyPath := filepath.Join(dataDir, "connector.key")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return Identity{}, fmt.Errorf("create identity directory: %w", err)
	}
	if _, err := os.Stat(certPath); err == nil {
		if _, keyErr := os.Stat(keyPath); keyErr != nil {
			return Identity{}, fmt.Errorf("inspect private key: %w", keyErr)
		}
		fingerprint, err := certificateFingerprint(certPath)
		return Identity{CertificatePath: certPath, PrivateKeyPath: keyPath, Fingerprint: fingerprint}, err
	} else if !os.IsNotExist(err) {
		return Identity{}, fmt.Errorf("inspect certificate: %w", err)
	}

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Identity{}, fmt.Errorf("generate private key: %w", err)
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return Identity{}, fmt.Errorf("generate certificate serial: %w", err)
	}
	now := time.Now()
	template := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "Localzie Connector"},
		NotBefore:    now.Add(-time.Hour),
		// Apple rejects long-lived self-signed TLS leaves, including when
		// explicitly pinned. Keep the total validity below 398 days.
		NotAfter:    now.Add(365 * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    []string{"localhost", "localzie.local"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return Identity{}, fmt.Errorf("create certificate: %w", err)
	}
	keyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return Identity{}, fmt.Errorf("marshal private key: %w", err)
	}
	if err := writeExclusive(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		return Identity{}, err
	}
	if err := writeExclusive(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes}), 0o600); err != nil {
		_ = os.Remove(certPath)
		return Identity{}, err
	}
	digest := sha256.Sum256(der)
	return Identity{CertificatePath: certPath, PrivateKeyPath: keyPath, Fingerprint: hex.EncodeToString(digest[:])}, nil
}

func certificateFingerprint(path string) (string, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read certificate: %w", err)
	}
	block, _ := pem.Decode(contents)
	if block == nil || block.Type != "CERTIFICATE" {
		return "", fmt.Errorf("decode certificate: invalid PEM")
	}
	digest := sha256.Sum256(block.Bytes)
	return hex.EncodeToString(digest[:]), nil
}

func writeExclusive(path string, data []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return file.Sync()
}
