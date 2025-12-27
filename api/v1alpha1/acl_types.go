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

// ACLResourceType defines the type of Kafka resource
// +kubebuilder:validation:Enum=Topic;Group;Cluster;TransactionalId
type ACLResourceType string

const (
	ACLResourceTypeTopic           ACLResourceType = "Topic"
	ACLResourceTypeGroup           ACLResourceType = "Group"
	ACLResourceTypeCluster         ACLResourceType = "Cluster"
	ACLResourceTypeTransactionalId ACLResourceType = "TransactionalId"
)

// ACLPatternType defines how the resource name is matched
// +kubebuilder:validation:Enum=Literal;Prefixed
type ACLPatternType string

const (
	ACLPatternTypeLiteral  ACLPatternType = "Literal"
	ACLPatternTypePrefixed ACLPatternType = "Prefixed"
)

// ACLOperation defines the Kafka operation
// +kubebuilder:validation:Enum=Read;Write;Create;Delete;Alter;Describe;ClusterAction;DescribeConfigs;AlterConfigs;IdempotentWrite;All
type ACLOperation string

const (
	ACLOperationRead            ACLOperation = "Read"
	ACLOperationWrite           ACLOperation = "Write"
	ACLOperationCreate          ACLOperation = "Create"
	ACLOperationDelete          ACLOperation = "Delete"
	ACLOperationAlter           ACLOperation = "Alter"
	ACLOperationDescribe        ACLOperation = "Describe"
	ACLOperationClusterAction   ACLOperation = "ClusterAction"
	ACLOperationDescribeConfigs ACLOperation = "DescribeConfigs"
	ACLOperationAlterConfigs    ACLOperation = "AlterConfigs"
	ACLOperationIdempotentWrite ACLOperation = "IdempotentWrite"
	ACLOperationAll             ACLOperation = "All"
)

// ACLPermission defines whether access is allowed or denied
// +kubebuilder:validation:Enum=Allow;Deny
type ACLPermission string

const (
	ACLPermissionAllow ACLPermission = "Allow"
	ACLPermissionDeny  ACLPermission = "Deny"
)

// ACLResource defines the Kafka resource the ACL applies to
type ACLResource struct {
	// Type is the type of Kafka resource.
	// +required
	Type ACLResourceType `json:"type"`

	// Name is the resource name or pattern.
	// Use "*" for all resources of this type.
	// +required
	Name string `json:"name"`

	// PatternType defines how the name is matched.
	// +kubebuilder:default=Literal
	// +optional
	PatternType ACLPatternType `json:"patternType,omitempty"`
}

// ACLSpec defines the desired state of ACL
type ACLSpec struct {
	// Cluster references a Cluster resource in the same namespace.
	// +required
	Cluster string `json:"cluster"`

	// Principal is the user principal in the format "User:<username>".
	// Use "User:*" for all users.
	// +required
	Principal string `json:"principal"`

	// Resource defines the Kafka resource this ACL applies to.
	// +required
	Resource ACLResource `json:"resource"`

	// Permission defines whether access is allowed or denied.
	// +kubebuilder:default=Allow
	// +optional
	Permission ACLPermission `json:"permission,omitempty"`

	// Operation defines the Kafka operation this ACL applies to.
	// +required
	Operation ACLOperation `json:"operation"`

	// Host is the host from which access is allowed.
	// Use "*" for all hosts (recommended).
	// +kubebuilder:default="*"
	// +optional
	Host string `json:"host,omitempty"`
}

// ACLStatus defines the observed state of ACL
type ACLStatus struct {
	// ObservedGeneration is the most recent generation observed for this resource.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the current state of the ACL resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Principal",type=string,JSONPath=`.spec.principal`
// +kubebuilder:printcolumn:name="Resource",type=string,JSONPath=`.spec.resource.type`
// +kubebuilder:printcolumn:name="Name",type=string,JSONPath=`.spec.resource.name`
// +kubebuilder:printcolumn:name="Operation",type=string,JSONPath=`.spec.operation`
// +kubebuilder:printcolumn:name="Permission",type=string,JSONPath=`.spec.permission`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ACL is the Schema for the acls API
type ACL struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of ACL
	// +required
	Spec ACLSpec `json:"spec"`

	// status defines the observed state of ACL
	// +optional
	Status ACLStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ACLList contains a list of ACL
type ACLList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ACL `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ACL{}, &ACLList{})
}
