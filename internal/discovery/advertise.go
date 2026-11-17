// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"fmt"
	"strings"

	"github.com/hashicorp/mdns"
)

const ServiceType = "_localzie._tcp"

type Advertisement struct {
	server *mdns.Server
}

func Start(connectorID, fingerprint string, port int, addresses []string) (*Advertisement, error) {
	service, err := mdns.NewMDNSService(instanceName(connectorID), ServiceType, "", "", port, nil, txtRecords(connectorID, fingerprint, addresses))
	if err != nil {
		return nil, fmt.Errorf("create local discovery service: %w", err)
	}
	server, err := mdns.NewServer(&mdns.Config{Zone: service})
	if err != nil {
		return nil, fmt.Errorf("start local discovery service: %w", err)
	}
	return &Advertisement{server: server}, nil
}

func (a *Advertisement) Close() error {
	if a == nil || a.server == nil {
		return nil
	}
	return a.server.Shutdown()
}

func instanceName(connectorID string) string {
	shortID := strings.ReplaceAll(connectorID, "-", "")
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}
	return "Localzie-" + shortID
}

func txtRecords(connectorID, fingerprint string, addresses []string) []string {
	records := []string{"v=1", "id=" + connectorID, "fp=" + strings.ToLower(fingerprint)}
	for index, address := range addresses {
		if index == 6 {
			break
		}
		records = append(records, fmt.Sprintf("url%d=%s", index, address))
	}
	return records
}
