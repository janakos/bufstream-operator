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
