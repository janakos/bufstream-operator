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

// ClusterSpec defines the desired state of Cluster
type ClusterSpec struct {
	// BootstrapServers is the Kafka bootstrap servers address for the Bufstream cluster.
	// Example: "bufstream.bufstream.svc.cluster.local:9092"
	// +required
	BootstrapServers string `json:"bootstrapServers"`

	// AdminCredentialsRef references a Secret containing admin credentials for SASL auth.
	// The Secret must have 'username' and 'password' (or 'plaintext') keys.
	// If not specified, the operator will connect without authentication.
	// +optional
	AdminCredentialsRef *AdminCredentialsReference `json:"adminCredentialsRef,omitempty"`
}

// AdminCredentialsReference references admin credentials for cluster authentication
type AdminCredentialsReference struct {
	// Name is the name of the Secret containing admin credentials.
	// +required
	Name string `json:"name"`

	// Namespace is the namespace of the Secret. Defaults to the Cluster's namespace.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// ClusterStatus defines the observed state of Cluster
type ClusterStatus struct {
	// ObservedGeneration is the most recent generation observed for this resource.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the current state of the Cluster resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Bootstrap Servers",type=string,JSONPath=`.spec.bootstrapServers`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Cluster is the Schema for the clusters API
type Cluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Cluster
	// +required
	Spec ClusterSpec `json:"spec"`

	// status defines the observed state of Cluster
	// +optional
	Status ClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ClusterList contains a list of Cluster
type ClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Cluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Cluster{}, &ClusterList{})
}
