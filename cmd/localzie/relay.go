// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func readSecretFile(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("relay credential file is required")
	}
	info, e := os.Stat(path)
	if e != nil {
		return "", e
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("relay credential file must be a regular 0600 file")
	}
	b, e := os.ReadFile(path)
	if e != nil {
		return "", e
	}
	v := strings.TrimSpace(string(b))
	if v == "" {
		return "", fmt.Errorf("relay credential file is empty")
	}
	return v, nil
}
func publishPairing(ctx context.Context, base string, db interface {
	ConnectorID(context.Context) (string, error)
}, fp, token, code string, expires time.Time) error {
	id, e := db.ConnectorID(ctx)
	if e != nil {
		return e
	}
	u, e := url.Parse(base)
	if e != nil || u.Scheme != "https" {
		return fmt.Errorf("relay base URL must be HTTPS")
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/v1/connectors/" + id + "/pairing-codes"
	body, _ := json.Marshal(map[string]string{"code": code, "certificate_sha256": fp, "expires_at": expires.UTC().Format(time.RFC3339)})
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if e != nil {
		return e
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	c := &http.Client{Timeout: 10 * time.Second}
	resp, e := c.Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("relay returned %s", resp.Status)
	}
	return nil
}
