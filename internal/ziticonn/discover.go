// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package ziticonn

import (
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// Discover only the local service descriptor; never scan arbitrary destinations.
// Existing credentials enter the environment and are never written to config/QR.
func discoverOpenCode(endpoint *Endpoint) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	state := os.Getenv("XDG_STATE_HOME")
	if state == "" {
		state = filepath.Join(home, ".local", "state")
	}
	raw, err := os.ReadFile(filepath.Join(state, "opencode", "service.json"))
	if err != nil {
		return errors.New("Start OpenCode's local service, or set its loopback URL in connector.json")
	}
	var service struct {
		URL      string `json:"url"`
		Password string `json:"password"`
	}
	if json.Unmarshal(raw, &service) != nil || service.URL == "" {
		return errors.New("OpenCode's local service descriptor is invalid")
	}
	endpoint.URL = service.URL
	address, err := url.Parse(endpoint.URL)
	if err != nil {
		return errors.New("OpenCode's local service address is invalid")
	}
	if address.Hostname() == "0.0.0.0" || address.Hostname() == "localhost" {
		address.Host = "127.0.0.1:" + address.Port()
		endpoint.URL = address.String()
	}
	if address.Hostname() == "::" {
		address.Host = "[::1]:" + address.Port()
		endpoint.URL = address.String()
	}
	if service.Password != "" {
		endpoint.Username = "opencode"
		endpoint.PasswordEnv = "LOCALZIE_DISCOVERED_OPENCODE_" + strings.ToUpper(endpoint.ID)
		return os.Setenv(endpoint.PasswordEnv, service.Password)
	}
	return nil
}
