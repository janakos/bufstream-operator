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

package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bufstreamv1alpha1 "github.com/janakos/bufstream-operator/api/v1alpha1"
	"github.com/janakos/bufstream-operator/internal/bufstream"
)

const (
	// clusterNotReadyRequeue is how long to wait before retrying when cluster is not ready
	clusterNotReadyRequeue = 10 * time.Second
)

// GetCluster retrieves a Cluster resource by name in the given namespace.
func GetCluster(ctx context.Context, c client.Client, name, namespace string) (*bufstreamv1alpha1.Cluster, error) {
	cluster := &bufstreamv1alpha1.Cluster{}
	if err := c.Get(ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, cluster); err != nil {
		return nil, fmt.Errorf("failed to get Cluster %q: %w", name, err)
	}
	return cluster, nil
}

// IsClusterReady checks if a Cluster has a Ready condition set to True.
func IsClusterReady(cluster *bufstreamv1alpha1.Cluster) bool {
	readyCondition := meta.FindStatusCondition(cluster.Status.Conditions, typeReady)
	return readyCondition != nil && readyCondition.Status == metav1.ConditionTrue
}

// GetAdminCredentials retrieves admin credentials for a cluster from the referenced secret.
// Returns nil if no credentials are configured.
func GetAdminCredentials(ctx context.Context, c client.Client, cluster *bufstreamv1alpha1.Cluster) (*bufstream.SASLConfig, error) {
	if cluster.Spec.AdminCredentialsRef == nil {
		return nil, nil // No auth configured
	}

	secretNS := cluster.Spec.AdminCredentialsRef.Namespace
	if secretNS == "" {
		secretNS = cluster.Namespace
	}

	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{
		Name:      cluster.Spec.AdminCredentialsRef.Name,
		Namespace: secretNS,
	}, secret); err != nil {
		return nil, fmt.Errorf("failed to get admin credentials secret %q: %w", cluster.Spec.AdminCredentialsRef.Name, err)
	}

	username, ok := secret.Data["username"]
	if !ok {
		return nil, fmt.Errorf("admin credentials secret %q does not contain 'username' key", cluster.Spec.AdminCredentialsRef.Name)
	}

	// Try 'password' first, then 'plaintext' (for compatibility with bufstream secrets)
	password, ok := secret.Data["password"]
	if !ok {
		password, ok = secret.Data["plaintext"]
		if !ok {
			return nil, fmt.Errorf("admin credentials secret %q does not contain 'password' or 'plaintext' key", cluster.Spec.AdminCredentialsRef.Name)
		}
	}

	return &bufstream.SASLConfig{
		Username:  string(username),
		Password:  string(password),
		Mechanism: bufstream.ScramSHA512, // Default to SHA512
	}, nil
}
