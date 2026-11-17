// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package setup

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image/color"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	"charm.land/huh/v2"
	"github.com/manuelemosr/localzie-connector/internal/ziticonn"
)

type Options struct {
	ConfigPath      string
	DataDir         string
	Name            string
	APIKeyEnv       string
	PhoneEnrollment string
	Accessible      bool
}

type Wizard struct {
	Discover func(context.Context, []ziticonn.Endpoint, string) ([]Service, error)
	Select   func(context.Context, io.Reader, io.Writer, []Service, []string, bool) ([]string, error)
	Pair     func(ziticonn.Config, string, string) (string, error)
	Serve    func(context.Context, ziticonn.Config, string, func() error) error
}

func New() Wizard {
	return Wizard{Discover: Discover, Select: selectServices, Pair: preparePair, Serve: ziticonn.ServeWithReady}
}

// Run completes selection, saves private configuration, prints one setup code,
// and starts serving in the same invocation. Cancellation before selection
// changes neither saved configuration nor pairing state.
func (w Wizard) Run(ctx context.Context, input io.Reader, output io.Writer, options Options) error {
	fmt.Fprint(output, "\nLocalzie\nYour computer's AI, on your iPhone.\nhttps://apps.apple.com/app/id6813506926\n\n")
	config := ziticonn.Config{Name: options.Name, LocalOnly: true}
	var selected []string
	previous, err := os.ReadFile(options.ConfigPath)
	if err == nil {
		config = ziticonn.Config{}
		decoder := json.NewDecoder(bytes.NewReader(previous))
		decoder.DisallowUnknownFields()
		if len(previous) > 64<<10 || decoder.Decode(&config) != nil {
			return errors.New("your saved setup could not be read; it has been kept unchanged")
		}
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			return errors.New("your saved setup could not be read; it has been kept unchanged")
		}
		for _, endpoint := range config.Endpoints {
			selected = append(selected, endpoint.ID)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	fmt.Fprintln(output, "Looking for models and coding agents on this computer...")
	services, err := w.Discover(ctx, config.Endpoints, options.APIKeyEnv)
	if err != nil {
		return err
	}
	if len(services) == 0 {
		return errors.New("no services found; start your model server, OpenCode or Hermes, or install Codex, then run setup again")
	}
	selected, err = w.Select(ctx, input, output, services, selected, options.Accessible)
	if err != nil {
		return err
	}
	config.Endpoints = nil
	seen := map[string]bool{}
	for _, id := range selected {
		if seen[id] {
			return errors.New("the service selection contains a duplicate")
		}
		seen[id] = true
		match := false
		for _, service := range services {
			if service.Endpoint.ID == id {
				config.Endpoints = append(config.Endpoints, service.Endpoint)
				match = true
				break
			}
		}
		if !match {
			return errors.New("the service selection is no longer available; run setup again")
		}
	}
	if len(config.Endpoints) == 0 {
		return errors.New("choose at least one service to connect")
	}
	if len(config.Endpoints) > 16 {
		return errors.New("choose up to 16 services")
	}
	if config.Name == "" {
		config.Name = "My computer"
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = os.MkdirAll(options.DataDir, 0700); err != nil {
		return err
	}
	if err = os.Chmod(options.DataDir, 0700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	// Validate through the serving path before replacing any saved setup. This
	// also discovers OpenCode auth without persisting its password in JSON.
	parent := filepath.Dir(options.ConfigPath)
	if err = os.MkdirAll(parent, 0700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(parent, ".localzie-setup-*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	if _, err = temp.Write(append(raw, '\n')); err != nil {
		temp.Close()
		return err
	}
	if err = temp.Close(); err != nil {
		return err
	}
	validated, err := ziticonn.Load(temp.Name())
	if err != nil {
		return fmt.Errorf("could not connect the selected services: %w", err)
	}
	code, err := w.Pair(validated, options.DataDir, options.PhoneEnrollment)
	if err != nil {
		return fmt.Errorf("could not prepare your phone connection: %w", err)
	}
	return w.Serve(ctx, validated, options.DataDir, func() error {
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = os.Rename(temp.Name(), options.ConfigPath); err != nil {
			return err
		}
		fmt.Fprintln(output, "\nSelected for Localzie:")
		for _, endpoint := range config.Endpoints {
			fmt.Fprintln(output, "  ✓ "+safeLabel(endpoint.Name))
		}
		if code != "" {
			fmt.Fprintln(output, "\nOn your iPhone, open Localzie > Connect to a server and paste this setup code.")
			fmt.Fprint(output, "It works once and expires in 10 minutes. Keep it private.\n\n")
			fmt.Fprintln(output, code)
		} else {
			fmt.Fprintln(output, "\nOpen your saved connection in Localzie on your iPhone. Your existing pairing is ready.")
		}
		if validated.LocalOnly {
			fmt.Fprintln(output, "\nReady on your home network.")
		} else {
			fmt.Fprintln(output, "\nYour Localzie remote connection is enabled.")
		}
		fmt.Fprint(output, "Keep this terminal open while using Localzie. Press Ctrl+C to stop.\n\n")
		return nil
	})
}

func selectServices(ctx context.Context, input io.Reader, output io.Writer, services []Service, selected []string, accessible bool) ([]string, error) {
	if accessible {
		return selectAccessible(ctx, input, output, services, selected)
	}
	options := make([]huh.Option[string], 0, len(services))
	for _, service := range services {
		options = append(options, huh.NewOption(safeLabel(service.Endpoint.Name)+" - "+service.Detail, service.Endpoint.ID))
	}
	field := huh.NewMultiSelect[string]().Title("What would you like to use in Localzie?").Description("Space to select · Enter to connect\nCoding agents can run commands and edit files.").Options(options...).Value(&selected).Validate(func(value []string) error {
		if len(value) == 0 {
			return errors.New("choose at least one service")
		}
		if len(value) > 16 {
			return errors.New("choose up to 16 services")
		}
		return nil
	})
	err := huh.NewForm(huh.NewGroup(field)).WithTheme(setupTheme()).WithInput(input).WithOutput(output).WithAccessible(accessible).RunWithContext(ctx)
	if errors.Is(err, huh.ErrUserAborted) {
		return nil, errors.New("setup canceled; your saved connections were kept")
	}
	return selected, err
}

func safeLabel(value string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, value)
}

// Accessible mode uses a bounded line reader and treats EOF as cancellation,
// rather than retrying forever when a pipe or screen-reader session closes.
func selectAccessible(ctx context.Context, input io.Reader, output io.Writer, services []Service, selected []string) ([]string, error) {
	chosen := map[string]bool{}
	for _, id := range selected {
		chosen[id] = true
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 128), 4096)
	fmt.Fprintln(output, "Choose services for Localzie. Enter a number to toggle its checkbox, then 0 to connect.")
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for i, service := range services {
			mark := " "
			if chosen[service.Endpoint.ID] {
				mark = "x"
			}
			fmt.Fprintf(output, "  %d [%s] %s\n", i+1, mark, safeLabel(service.Endpoint.Name))
		}
		fmt.Fprint(output, "> ")
		line, err := scanSetupLine(ctx, scanner)
		if err != nil {
			return nil, err
		}
		value, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil || value < 0 || value > len(services) {
			fmt.Fprintln(output, "Enter a listed number, or 0 to connect.")
			continue
		}
		if value > 0 {
			id := services[value-1].Endpoint.ID
			chosen[id] = !chosen[id]
			continue
		}
		result := []string{}
		for _, service := range services {
			if chosen[service.Endpoint.ID] {
				result = append(result, service.Endpoint.ID)
			}
		}
		if len(result) == 0 || len(result) > 16 {
			fmt.Fprintln(output, "Choose between 1 and 16 services.")
			continue
		}
		return result, nil
	}
}

func preparePair(config ziticonn.Config, dir, enrollment string) (string, error) {
	if config.LocalOnly || enrollment != "" {
		return ziticonn.Pair(config, dir, enrollment)
	}
	_, db, err := ziticonn.OpenState(dir)
	if err != nil {
		return "", err
	}
	defer db.Close()
	devices, err := db.ListDevices(context.Background())
	if err != nil {
		return "", err
	}
	for _, device := range devices {
		if device.RevokedAt == nil {
			return "", nil
		}
	}
	return "", errors.New("Localzie needs to activate this computer before a new phone can connect remotely; your saved connection has been kept")
}

func setupTheme() huh.Theme {
	return huh.ThemeFunc(func(dark bool) *huh.Styles {
		styles := huh.ThemeCharm(dark)
		accent := color.RGBA{R: 0x25, G: 0x88, B: 0x66, A: 0xff}
		if dark {
			accent = color.RGBA{R: 0x71, G: 0xd8, B: 0xb0, A: 0xff}
		}
		for _, field := range []*huh.FieldStyles{&styles.Focused, &styles.Blurred} {
			field.SelectedPrefix = field.SelectedPrefix.SetString("[x] ").Foreground(accent)
			field.UnselectedPrefix = field.UnselectedPrefix.SetString("[ ] ")
			field.Title = field.Title.Foreground(accent)
		}
		return styles
	})
}

func scanSetupLine(ctx context.Context, scanner *bufio.Scanner) (string, error) {
	type result struct {
		line string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		if scanner.Scan() {
			done <- result{line: scanner.Text()}
			return
		}
		err := scanner.Err()
		if err == nil {
			err = errors.New("setup canceled before services were confirmed")
		}
		done <- result{err: err}
	}()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case value := <-done:
		return value.line, value.err
	}
}
