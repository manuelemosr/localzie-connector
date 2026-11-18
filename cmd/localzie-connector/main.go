// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

// localzie-connector serves explicitly configured local APIs over private LAN
// and OpenZiti, protected by independent TLS and phone authentication.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/manuelemosr/localzie-connector/internal/setup"
	"github.com/manuelemosr/localzie-connector/internal/ziticonn"
	"github.com/openziti/sdk-golang/ziti"
	"github.com/openziti/sdk-golang/ziti/enroll"
	"github.com/sirupsen/logrus"
)

var version = "dev"

func main() {
	// The Ziti SDK can include session credentials in its error logs. The
	// CLI reports connection failures without exporting SDK diagnostics.
	logrus.SetOutput(io.Discard)
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "localzie-connector:", err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) < 2 || os.Args[1] == "help" || os.Args[1] == "--help" || os.Args[1] == "-h" {
		fmt.Println("Localzie Connector - your home models and coding agents on iPhone")
		fmt.Println("Usage: localzie-connector <setup|serve|pair|enroll|devices|revoke|version> [options]")
		fmt.Println("Start here: localzie-connector setup")
		fmt.Println("Localzie for iPhone: https://apps.apple.com/app/id6813506926")
		return nil
	}
	if os.Args[1] == "version" || os.Args[1] == "--version" {
		fmt.Println(version)
		return nil
	}
	switch os.Args[1] {
	case "setup", "serve", "pair", "enroll", "devices", "revoke":
	default:
		return errors.New("unknown command; run localzie-connector --help")
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ContinueOnError)
	config := fs.String("config", "connector.json", "local service allowlist")
	userConfig, _ := os.UserConfigDir()
	dir := fs.String("data-dir", filepath.Join(userConfig, "LocalzieZiti"), "private connector state")
	jwt := fs.String("jwt", "", "one-time Ziti enrollment file")
	identity := fs.String("identity", "", "output Ziti identity file (enroll)")
	device := fs.String("device", "", "device ID to revoke")
	name := fs.String("name", "My computer", "computer name in Localzie")
	keyEnv := fs.String("api-key-env", "", "environment variable holding a model server API key")
	accessible := fs.Bool("accessible", false, "use screen-reader-friendly setup prompts")
	if err := fs.Parse(os.Args[2:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected arguments; run this command with --help")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if os.Args[1] == "setup" {
		configPath := *config
		explicitConfig := false
		fs.Visit(func(f *flag.Flag) {
			if f.Name == "config" {
				explicitConfig = true
			}
		})
		if !explicitConfig {
			configPath = filepath.Join(*dir, "connector.json")
			if _, err := os.Stat(configPath); errors.Is(err, os.ErrNotExist) {
				if _, err := os.Stat("connector.json"); err == nil {
					configPath = "connector.json"
				}
			}
		}
		return setup.New().Run(ctx, os.Stdin, os.Stdout, setup.Options{ConfigPath: configPath, DataDir: *dir, Name: *name, APIKeyEnv: *keyEnv, PhoneEnrollment: *jwt, Accessible: *accessible})
	}
	if os.Args[1] == "enroll" {
		if *jwt == "" || *identity == "" {
			return errors.New("enroll requires --jwt and --identity")
		}
		if _, err := os.Stat(*identity); !os.IsNotExist(err) {
			return errors.New("identity output already exists or cannot be inspected")
		}
		b, err := os.ReadFile(*jwt)
		if err != nil {
			return err
		}
		claims, parsed, err := enroll.ParseToken(string(b))
		if err != nil {
			return errors.New("invalid enrollment token")
		}
		var keyAlgorithm ziti.KeyAlgVar
		if err := keyAlgorithm.Set("EC"); err != nil {
			return err
		}
		cfg, err := enroll.Enroll(enroll.EnrollmentFlags{Token: claims, JwtToken: parsed, JwtString: string(b), KeyAlg: keyAlgorithm})
		if err != nil {
			return errors.New("Ziti enrollment failed")
		}
		b, err = json.Marshal(cfg)
		if err != nil {
			return err
		}
		f, err := os.OpenFile(*identity, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = f.Write(b)
		return err
	}
	if os.Args[1] == "devices" || os.Args[1] == "revoke" {
		_, db, err := ziticonn.OpenState(*dir)
		if err != nil {
			return err
		}
		defer db.Close()
		if os.Args[1] == "revoke" {
			if *device == "" {
				return errors.New("--device is required")
			}
			return db.RevokeDevice(ctx, *device)
		}
		devices, err := db.ListDevices(ctx)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(devices)
	}
	c, err := ziticonn.Load(*config)
	if err != nil {
		return err
	}
	switch os.Args[1] {
	case "serve":
		return ziticonn.Serve(ctx, c, *dir)
	case "pair":
		code, err := ziticonn.Pair(c, *dir, *jwt)
		if err != nil {
			return err
		}
		fmt.Println("Pair in Localzie > Connect to a server > Connect remotely. This secret expires in 10 minutes and works once:")
		fmt.Println(code)
		return nil
	default:
		return errors.New("unknown command")
	}
}
