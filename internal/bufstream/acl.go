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

// ACLConfig holds ACL configuration
type ACLConfig struct {
	Principal       string
	Host            string
	ResourceType    ACLResourceType
	ResourceName    string
	ResourcePattern ACLPatternType
	Operation       ACLOperation
	Permission      ACLPermission
}

// ACLResourceType represents the type of Kafka resource
type ACLResourceType int8

const (
	ACLResourceTypeTopic           ACLResourceType = 2
	ACLResourceTypeGroup           ACLResourceType = 3
	ACLResourceTypeCluster         ACLResourceType = 4
	ACLResourceTypeTransactionalId ACLResourceType = 5
)

// ACLPatternType represents how the resource name is matched
type ACLPatternType int8

const (
	ACLPatternTypeLiteral  ACLPatternType = 3
	ACLPatternTypePrefixed ACLPatternType = 4
)

// ACLOperation represents a Kafka operation
type ACLOperation int8

const (
	ACLOperationAll             ACLOperation = 1
	ACLOperationRead            ACLOperation = 2
	ACLOperationWrite           ACLOperation = 3
	ACLOperationCreate          ACLOperation = 4
	ACLOperationDelete          ACLOperation = 5
	ACLOperationAlter           ACLOperation = 6
	ACLOperationDescribe        ACLOperation = 7
	ACLOperationClusterAction   ACLOperation = 8
	ACLOperationDescribeConfigs ACLOperation = 9
	ACLOperationAlterConfigs    ACLOperation = 10
	ACLOperationIdempotentWrite ACLOperation = 11
)

// ACLPermission represents allow or deny
type ACLPermission int8

const (
	ACLPermissionDeny  ACLPermission = 2
	ACLPermissionAllow ACLPermission = 3
)

// CreateACL creates an ACL in Bufstream
func (c *Client) CreateACL(ctx context.Context, config ACLConfig) error {
	host := config.Host
	if host == "" {
		host = "*"
	}

	acl := c.buildACL(config, host)
	resp, err := c.adminClient.CreateACLs(ctx, acl)
	if err != nil {
		return fmt.Errorf("failed to create ACL: %w", err)
	}

	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("failed to create ACL: %w", r.Err)
		}
	}

	return nil
}

// DeleteACL deletes an ACL from Bufstream
func (c *Client) DeleteACL(ctx context.Context, config ACLConfig) error {
	host := config.Host
	if host == "" {
		host = "*"
	}

	acl := c.buildACL(config, host)
	resp, err := c.adminClient.DeleteACLs(ctx, acl)
	if err != nil {
		return fmt.Errorf("failed to delete ACL: %w", err)
	}

	for _, r := range resp {
		if r.Err != nil {
			// Ignore "not found" errors
			errStr := r.Err.Error()
			if errStr == "RESOURCE_NOT_FOUND" {
				continue
			}
			return fmt.Errorf("failed to delete ACL: %w", r.Err)
		}
	}

	return nil
}

// ACLExists checks if an ACL exists
func (c *Client) ACLExists(ctx context.Context, config ACLConfig) (bool, error) {
	host := config.Host
	if host == "" {
		host = "*"
	}

	acl := c.buildACL(config, host)
	resp, err := c.adminClient.DescribeACLs(ctx, acl)
	if err != nil {
		return false, fmt.Errorf("failed to describe ACLs: %w", err)
	}

	return len(resp) > 0, nil
}

// buildACL constructs an ACLBuilder based on the config
func (c *Client) buildACL(config ACLConfig, host string) *kadm.ACLBuilder {
	b := kadm.NewACLs()

	// Set permission and principal
	if config.Permission == ACLPermissionDeny {
		b = b.Deny(config.Principal)
		b = b.DenyHosts(host)
	} else {
		b = b.Allow(config.Principal)
		b = b.AllowHosts(host)
	}

	// Set pattern type
	switch config.ResourcePattern {
	case ACLPatternTypePrefixed:
		b = b.ResourcePatternType(kadm.ACLPatternPrefixed)
	default:
		b = b.ResourcePatternType(kadm.ACLPatternLiteral)
	}

	// Set resource type and name
	switch config.ResourceType {
	case ACLResourceTypeGroup:
		b = b.Groups(config.ResourceName)
	case ACLResourceTypeCluster:
		b = b.Clusters()
	case ACLResourceTypeTransactionalId:
		b = b.TransactionalIDs(config.ResourceName)
	default: // Topic
		b = b.Topics(config.ResourceName)
	}

	// Set operation
	var op kadm.ACLOperation
	switch config.Operation {
	case ACLOperationRead:
		op = kadm.OpRead
	case ACLOperationWrite:
		op = kadm.OpWrite
	case ACLOperationCreate:
		op = kadm.OpCreate
	case ACLOperationDelete:
		op = kadm.OpDelete
	case ACLOperationAlter:
		op = kadm.OpAlter
	case ACLOperationDescribe:
		op = kadm.OpDescribe
	case ACLOperationClusterAction:
		op = kadm.OpClusterAction
	case ACLOperationDescribeConfigs:
		op = kadm.OpDescribeConfigs
	case ACLOperationAlterConfigs:
		op = kadm.OpAlterConfigs
	case ACLOperationIdempotentWrite:
		op = kadm.OpIdempotentWrite
	case ACLOperationAll:
		op = kadm.OpAll
	}
	b = b.Operations(op)

	return b
}
