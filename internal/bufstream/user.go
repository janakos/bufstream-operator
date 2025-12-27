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

	"github.com/twmb/franz-go/pkg/kadm"
)

// ScramMechanism represents a SCRAM authentication mechanism
type ScramMechanism int8

const (
	ScramSHA256 ScramMechanism = 1
	ScramSHA512 ScramMechanism = 2
)

// UserCredentials holds SCRAM credential configuration
type UserCredentials struct {
	Username   string
	Password   string
	Mechanism  ScramMechanism
	Iterations int32
}

// UpsertUserCredentials creates or updates SCRAM credentials for a user
func (c *Client) UpsertUserCredentials(ctx context.Context, creds UserCredentials) error {
	iterations := creds.Iterations
	if iterations == 0 {
		iterations = 4096
	}

	// Map our mechanism to kadm's mechanism
	var mechanism kadm.ScramMechanism
	switch creds.Mechanism {
	case ScramSHA256:
		mechanism = kadm.ScramSha256
	case ScramSHA512:
		mechanism = kadm.ScramSha512
	default:
		mechanism = kadm.ScramSha512
	}

	upsert := kadm.UpsertSCRAM{
		User:       creds.Username,
		Password:   creds.Password,
		Mechanism:  mechanism,
		Iterations: iterations,
	}

	resp, err := c.adminClient.AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{upsert})
	if err != nil {
		return fmt.Errorf("failed to upsert user credentials: %w", err)
	}

	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("failed to upsert credentials for user %s: %w", r.User, r.Err)
		}
	}

	return nil
}

// DeleteUserCredentials deletes SCRAM credentials for a user
func (c *Client) DeleteUserCredentials(ctx context.Context, username string, mechanism ScramMechanism) error {
	// Map our mechanism to kadm's mechanism
	var kadmMechanism kadm.ScramMechanism
	switch mechanism {
	case ScramSHA256:
		kadmMechanism = kadm.ScramSha256
	case ScramSHA512:
		kadmMechanism = kadm.ScramSha512
	default:
		kadmMechanism = kadm.ScramSha512
	}

	deletion := kadm.DeleteSCRAM{
		User:      username,
		Mechanism: kadmMechanism,
	}

	resp, err := c.adminClient.AlterUserSCRAMs(ctx, []kadm.DeleteSCRAM{deletion}, nil)
	if err != nil {
		return fmt.Errorf("failed to delete user credentials: %w", err)
	}

	for _, r := range resp {
		if r.Err != nil {
			// Ignore "user not found" errors
			errStr := r.Err.Error()
			if errStr == "RESOURCE_NOT_FOUND" {
				continue
			}
			return fmt.Errorf("failed to delete credentials for user %s: %w", r.User, r.Err)
		}
	}

	return nil
}

// UserExists checks if a user with SCRAM credentials exists
func (c *Client) UserExists(ctx context.Context, username string) (bool, error) {
	resp, err := c.adminClient.DescribeUserSCRAMs(ctx, username)
	if err != nil {
		return false, fmt.Errorf("failed to describe user credentials: %w", err)
	}

	for _, r := range resp {
		if r.User == username && len(r.CredInfos) > 0 {
			return true, nil
		}
	}

	return false, nil
}
