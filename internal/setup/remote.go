// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package setup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/manuelemosr/localzie-connector/internal/ziticonn"
)

// The service operator authorizes preview computers out of band. This file
// gives the connector permission to request phone enrollment for its own home.
type remotePreview struct {
	URL     string `json:"url"`
	Grant   string `json:"grant"`
	Service string `json:"service"`
}

func loadRemotePreview(dir string, config *ziticonn.Config) (remotePreview, error) {
	var preview remotePreview
	file, err := os.Open(filepath.Join(dir, "remote-preview.json"))
	if errors.Is(err, os.ErrNotExist) {
		return preview, errors.New("Remote setup has not been enabled for this computer")
	}
	if err != nil {
		return preview, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 4096 {
		return preview, errors.New("Remote setup authorization must be a private file")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&preview); err != nil {
		return preview, errors.New("Remote setup authorization is invalid")
	}
	var extra any
	destination, err := url.Parse(preview.URL)
	if decoder.Decode(&extra) != io.EOF || err != nil || destination.Scheme != "https" || destination.Hostname() == "" || destination.User != nil || destination.RawQuery != "" || destination.Fragment != "" || len(preview.Grant) != 64 || preview.Service == "" {
		return preview, errors.New("Remote setup authorization is invalid")
	}
	if !config.LocalOnly && config.Service != preview.Service {
		return preview, errors.New("This computer already has a different remote connection")
	}
	identity, err := filepath.Abs(filepath.Join(dir, "remote-preview-home.json"))
	if err != nil {
		return preview, err
	}
	info, err = os.Stat(identity)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return preview, errors.New("Remote setup needs the private home identity installed by Localzie")
	}
	config.LocalOnly, config.Service, config.IdentityFile = false, preview.Service, identity
	return preview, nil
}

func (preview remotePreview) phoneJWT(ctx context.Context, client *http.Client) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, preview.URL, nil)
	if err != nil {
		return "", errors.New("Remote setup URL is invalid")
	}
	req.Header.Set("Authorization", "Bearer "+preview.Grant)
	response, err := client.Do(req)
	if err != nil {
		return "", errors.New("Could not reach Localzie to activate phone pairing. Check your internet connection and try again")
	}
	defer response.Body.Close()
	switch response.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusForbidden:
		return "", errors.New("Localzie did not authorize remote setup for this computer")
	case http.StatusTooManyRequests:
		return "", errors.New("Remote setup is temporarily limited. Wait a minute and try again")
	default:
		return "", errors.New("Localzie could not create a remote pairing code. Try again shortly")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 16<<10))
	var result struct {
		Service string `json:"service"`
		JWT     string `json:"jwt"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var extra any
	if err != nil || decoder.Decode(&result) != nil || decoder.Decode(&extra) != io.EOF || result.Service != preview.Service || len(result.JWT) > 8192 || strings.Count(result.JWT, ".") != 2 {
		return "", errors.New("Localzie returned an invalid remote pairing credential")
	}
	return result.JWT, nil
}

func (preview remotePreview) pair(ctx context.Context, config ziticonn.Config, dir string) (string, error) {
	client := &http.Client{Timeout: 25 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	jwt, err := preview.phoneJWT(ctx, client)
	if err != nil {
		return "", err
	}
	return ziticonn.PairWithEnrollment(config, dir, jwt)
}
