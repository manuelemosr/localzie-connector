// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEnsureCreatesStableIdentityAndRejectsMissingKey(t *testing.T) {
	dir := t.TempDir()
	first, err := Ensure(dir)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Ensure(dir)
	if err != nil {
		t.Fatal(err)
	}
	if first.Fingerprint == "" || first.Fingerprint != second.Fingerprint {
		t.Fatalf("identity was not stable: first=%q second=%q", first.Fingerprint, second.Fingerprint)
	}
	contents, err := os.ReadFile(first.CertificatePath)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(contents)
	if block == nil {
		t.Fatal("certificate is not PEM")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if validity := certificate.NotAfter.Sub(certificate.NotBefore); validity > 398*24*time.Hour || validity < 365*24*time.Hour {
		t.Fatalf("certificate validity %s is outside the Apple-compatible window", validity)
	}
	if err := os.Remove(filepath.Join(dir, "connector.key")); err != nil {
		t.Fatal(err)
	}
	if _, err := Ensure(dir); err == nil {
		t.Fatal("missing private key unexpectedly accepted")
	}
}
