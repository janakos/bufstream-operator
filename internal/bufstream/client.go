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

// Client provides operations for managing Bufstream topics
type Client struct {
	kgoClient   *kgo.Client
	adminClient *kadm.Client
}

// TopicConfig holds topic configuration
type TopicConfig struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Config            map[string]string
}

// TopicInfo holds information about an existing topic
type TopicInfo struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Config            map[string]string
}

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

// TopicExists checks if a topic exists and returns its details
func (c *Client) TopicExists(ctx context.Context, topicName string) (bool, *TopicInfo, error) {
	topics, err := c.adminClient.ListTopics(ctx, topicName)
	if err != nil {
		return false, nil, fmt.Errorf("failed to list topics: %w", err)
	}

	if len(topics) == 0 {
		return false, nil, nil
	}

	topicDetail, exists := topics[topicName]
	if !exists {
		return false, nil, nil
	}

	// If topic has no partitions, treat it as not existing
	if len(topicDetail.Partitions) == 0 {
		return false, nil, nil
	}

	info := &TopicInfo{
		Name:       topicName,
		Partitions: int32(len(topicDetail.Partitions)),
	}

	// Get replication factor from first partition
	if len(topicDetail.Partitions) > 0 {
		info.ReplicationFactor = int16(len(topicDetail.Partitions[0].Replicas))
	}

	return true, info, nil
}

// CreateTopic creates a new topic in Bufstream
func (c *Client) CreateTopic(ctx context.Context, config TopicConfig) error {
	partitions := config.Partitions
	if partitions == 0 {
		partitions = 1
	}

	replicationFactor := config.ReplicationFactor
	if replicationFactor == 0 {
		replicationFactor = 1
	}

	// Build topic configs
	configs := make(map[string]*string)
	for k, v := range config.Config {
		val := v
		configs[k] = &val
	}

	resp, err := c.adminClient.CreateTopics(ctx, partitions, replicationFactor, configs, config.Name)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	for _, topicResp := range resp {
		if topicResp.Err != nil {
			return fmt.Errorf("failed to create topic %s: %w", topicResp.Topic, topicResp.Err)
		}
	}

	return nil
}

// UpdateTopic updates an existing topic's configuration
func (c *Client) UpdateTopic(ctx context.Context, topicName string, desiredPartitions int32, desiredReplicas int16, currentInfo *TopicInfo, config map[string]string) error {
	// Validate replication factor
	if currentInfo.ReplicationFactor != desiredReplicas {
		return fmt.Errorf("cannot change replication factor from %d to %d (requires partition reassignment)",
			currentInfo.ReplicationFactor, desiredReplicas)
	}

	// Handle partition changes
	if desiredPartitions == 0 {
		desiredPartitions = 1
	}

	if desiredPartitions > currentInfo.Partitions {
		resp, err := c.adminClient.CreatePartitions(ctx, int(desiredPartitions), topicName)
		if err != nil {
			return fmt.Errorf("failed to increase partitions: %w", err)
		}
		for _, partResp := range resp {
			if partResp.Err != nil {
				return fmt.Errorf("failed to increase partitions for %s: %w", partResp.Topic, partResp.Err)
			}
		}
	} else if desiredPartitions < currentInfo.Partitions {
		return fmt.Errorf("cannot decrease partitions from %d to %d", currentInfo.Partitions, desiredPartitions)
	}

	// Update topic configs if specified
	if len(config) > 0 {
		configs := make([]kadm.AlterConfig, 0, len(config))
		for k, v := range config {
			val := v
			configs = append(configs, kadm.AlterConfig{
				Op:    kadm.SetConfig,
				Name:  k,
				Value: &val,
			})
		}

		resp, err := c.adminClient.AlterTopicConfigs(ctx, configs, topicName)
		if err != nil {
			return fmt.Errorf("failed to alter topic configs: %w", err)
		}

		for _, configResp := range resp {
			if configResp.Err != nil {
				return fmt.Errorf("failed to alter config for %s: %w", configResp.Name, configResp.Err)
			}
		}
	}

	return nil
}

// DeleteTopic deletes a topic from Bufstream
func (c *Client) DeleteTopic(ctx context.Context, topicName string) error {
	resp, err := c.adminClient.DeleteTopics(ctx, topicName)
	if err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}

	for _, topicResp := range resp {
		if topicResp.Err != nil {
			// Ignore "topic not found" errors - topic may have already been deleted
			errStr := topicResp.Err.Error()
			if errStr == "UNKNOWN_TOPIC_OR_PARTITION" || errStr == "UNKNOWN_TOPIC" {
				continue
			}
			return fmt.Errorf("failed to delete topic %s: %w", topicResp.Topic, topicResp.Err)
		}
	}

	return nil
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
	ACLOperationAll             ACLOperation = 1
)

// ACLPermission represents allow or deny
type ACLPermission int8

const (
	ACLPermissionAllow ACLPermission = 3
	ACLPermissionDeny  ACLPermission = 2
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
