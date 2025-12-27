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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// TopicSpec defines the desired state of Topic
type TopicSpec struct {
	// TopicName is the name of the Kafka topic in Bufstream.
	// If not specified, defaults to the metadata.name of the resource.
	// +optional
	TopicName string `json:"topicName,omitempty"`

	// Partitions is the number of partitions for the topic.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	Partitions int32 `json:"partitions,omitempty"`

	// ReplicationFactor is the replication factor for the topic.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	ReplicationFactor int32 `json:"replicationFactor,omitempty"`

	// Config contains topic-level configuration overrides.
	// +optional
	Config map[string]string `json:"config,omitempty"`

	// Cluster references a Cluster resource in the same namespace.
	// +required
	Cluster string `json:"cluster"`

	// AssumeOwnership indicates whether to assume ownership of an existing topic.
	// If true, the operator will manage an existing topic even if it wasn't created by the operator.
	// +kubebuilder:default=false
	// +optional
	AssumeOwnership bool `json:"assumeOwnership,omitempty"`
}

// TopicStatus defines the observed state of Topic.
type TopicStatus struct {
	// ObservedGeneration is the most recent generation observed for this resource.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// TopicName is the actual name of the topic in Bufstream.
	// +optional
	TopicName string `json:"topicName,omitempty"`

	// Partitions is the current number of partitions.
	// +optional
	Partitions int32 `json:"partitions,omitempty"`

	// ReplicationFactor is the current replication factor.
	// +optional
	ReplicationFactor int32 `json:"replicationFactor,omitempty"`

	// Conditions represent the current state of the Topic resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Topic",type=string,JSONPath=`.status.topicName`
// +kubebuilder:printcolumn:name="Partitions",type=integer,JSONPath=`.status.partitions`
// +kubebuilder:printcolumn:name="Replication",type=integer,JSONPath=`.status.replicationFactor`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Topic is the Schema for the topics API
type Topic struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Topic
	// +required
	Spec TopicSpec `json:"spec"`

	// status defines the observed state of Topic
	// +optional
	Status TopicStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TopicList contains a list of Topic
type TopicList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Topic `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Topic{}, &TopicList{})
}
