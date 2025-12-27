/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package bufstream

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Client provides operations for managing Bufstream resources
type Client struct {
	kgoClient   *kgo.Client
	adminClient *kadm.Client
}

// SASLConfig holds SASL authentication configuration for the client
type SASLConfig struct {
	Username  string
	Password  string
	Mechanism ScramMechanism
}

// NewClient creates a new Bufstream client with optional SASL authentication
func NewClient(ctx context.Context, bootstrapServers string, saslConfig *SASLConfig) (*Client, error) {
	// Create context with timeout for connection
	connCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	opts := []kgo.Opt{
		kgo.SeedBrokers(bootstrapServers),
		kgo.RequestTimeoutOverhead(10 * time.Second),
	}

	// In dev mode, redirect cluster DNS to localhost for local development
	// Enable by setting BUFSTREAM_DEV_MODE=true
	if os.Getenv("BUFSTREAM_DEV_MODE") == "true" {
		opts = append(opts, kgo.Dialer(devModeDialer()))
	}

	// Configure SASL authentication if provided
	if saslConfig != nil {
		var auth scram.Auth
		auth.User = saslConfig.Username
		auth.Pass = saslConfig.Password

		switch saslConfig.Mechanism {
		case ScramSHA256:
			opts = append(opts, kgo.SASL(auth.AsSha256Mechanism()))
		case ScramSHA512:
			opts = append(opts, kgo.SASL(auth.AsSha512Mechanism()))
		default:
			opts = append(opts, kgo.SASL(auth.AsSha512Mechanism()))
		}
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	// Test connection by pinging
	if err := client.Ping(connCtx); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to ping Bufstream: %w", err)
	}

	adminClient := kadm.NewClient(client)
	return &Client{
		kgoClient:   client,
		adminClient: adminClient,
	}, nil
}

// Close closes the client connections
func (c *Client) Close() {
	if c.adminClient != nil {
		c.adminClient.Close()
	}
	if c.kgoClient != nil {
		c.kgoClient.Close()
	}
}

// CheckHealth verifies Bufstream is healthy
func (c *Client) CheckHealth(ctx context.Context) error {
	_, err := c.adminClient.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	return nil
}

// devModeDialer returns a dialer that redirects cluster DNS to localhost.
// This is useful when running the operator locally with port-forwarding.
func devModeDialer() func(ctx context.Context, network, address string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		// If the address contains cluster DNS, replace with localhost
		if strings.Contains(strings.ToLower(address), ".svc.cluster.local:") {
			parts := strings.Split(address, ":")
			if len(parts) == 2 {
				address = "localhost:" + parts[1]
			}
		}
		return dialer.DialContext(ctx, network, address)
	}
}
