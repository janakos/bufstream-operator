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

// ScramMechanism defines the SCRAM mechanism to use
// +kubebuilder:validation:Enum=SCRAM-SHA-256;SCRAM-SHA-512
type ScramMechanism string

const (
	ScramSHA256 ScramMechanism = "SCRAM-SHA-256"
	ScramSHA512 ScramMechanism = "SCRAM-SHA-512"
)

// UserSpec defines the desired state of User
type UserSpec struct {
	// Cluster references a Cluster resource in the same namespace.
	// +required
	Cluster string `json:"cluster"`

	// Username is the SASL username. If not specified, defaults to metadata.name.
	// +optional
	Username string `json:"username,omitempty"`

	// Mechanism is the SCRAM mechanism to use.
	// +kubebuilder:default="SCRAM-SHA-512"
	// +optional
	Mechanism ScramMechanism `json:"mechanism,omitempty"`

	// SecretRef references a Secret containing the password.
	// The Secret must have a key named "password".
	// +required
	SecretRef SecretReference `json:"secretRef"`

	// Iterations is the number of SCRAM iterations.
	// Higher values are more secure but slower.
	// +kubebuilder:validation:Minimum=4096
	// +kubebuilder:default=4096
	// +optional
	Iterations int32 `json:"iterations,omitempty"`
}

// SecretReference references a Secret
type SecretReference struct {
	// Name is the name of the Secret.
	// +required
	Name string `json:"name"`

	// Key is the key in the Secret containing the password.
	// +kubebuilder:default="password"
	// +optional
	Key string `json:"key,omitempty"`
}

// UserStatus defines the observed state of User
type UserStatus struct {
	// ObservedGeneration is the most recent generation observed for this resource.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Username is the actual username in Bufstream.
	// +optional
	Username string `json:"username,omitempty"`

	// Mechanism is the SCRAM mechanism configured.
	// +optional
	Mechanism ScramMechanism `json:"mechanism,omitempty"`

	// Conditions represent the current state of the User resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Username",type=string,JSONPath=`.status.username`
// +kubebuilder:printcolumn:name="Mechanism",type=string,JSONPath=`.status.mechanism`
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.cluster`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// User is the Schema for the users API
type User struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of User
	// +required
	Spec UserSpec `json:"spec"`

	// status defines the observed state of User
	// +optional
	Status UserStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// UserList contains a list of User
type UserList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []User `json:"items"`
}

func init() {
	SchemeBuilder.Register(&User{}, &UserList{})
}
