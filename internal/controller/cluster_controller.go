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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	bufstreamv1alpha1 "github.com/janakos/bufstream-operator/api/v1alpha1"
	"github.com/janakos/bufstream-operator/internal/bufstream"
)

const (
	// clusterHealthCheckInterval is how often to check cluster health
	clusterHealthCheckInterval = 10 * time.Second
)

// ClusterReconciler reconciles a Cluster object
type ClusterReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=bufstream.buf.build,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=bufstream.buf.build,resources=clusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=bufstream.buf.build,resources=clusters/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile checks cluster connectivity and updates status
func (r *ClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Fetch the Cluster resource
	cluster := &bufstreamv1alpha1.Cluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Cluster resource not found, ignoring")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get Cluster")
		return ctrl.Result{}, err
	}

	// Check if being deleted
	if !cluster.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Get admin credentials if configured
	saslConfig, err := GetAdminCredentials(ctx, r.Client, cluster)
	if err != nil {
		log.Error(err, "Failed to get admin credentials")
		r.setCondition(cluster, typeReady, metav1.ConditionFalse, "CredentialsError", fmt.Sprintf("Failed to get admin credentials: %v", err))
		if statusErr := r.Status().Update(ctx, cluster); statusErr != nil {
			log.Error(statusErr, "Failed to update status")
		}
		r.Recorder.Event(cluster, "Warning", "CredentialsError", fmt.Sprintf("Failed to get admin credentials: %v", err))
		return ctrl.Result{RequeueAfter: clusterHealthCheckInterval}, nil
	}

	// Try to connect to Bufstream
	bsClient, err := bufstream.NewClientWithSASL(ctx, cluster.Spec.BootstrapServers, saslConfig)
	if err != nil {
		log.Info("Cluster connectivity check failed", "error", err)
		r.setCondition(cluster, typeReady, metav1.ConditionFalse, "ConnectionFailed", fmt.Sprintf("Failed to connect: %v", err))
		if statusErr := r.Status().Update(ctx, cluster); statusErr != nil {
			log.Error(statusErr, "Failed to update status")
		}
		r.Recorder.Event(cluster, "Warning", "ConnectionFailed", fmt.Sprintf("Failed to connect to Bufstream: %v", err))
		return ctrl.Result{RequeueAfter: clusterHealthCheckInterval}, nil
	}
	defer bsClient.Close()

	// Check health
	if err := bsClient.CheckHealth(ctx); err != nil {
		log.Info("Cluster health check failed", "error", err)
		r.setCondition(cluster, typeReady, metav1.ConditionFalse, "Unhealthy", fmt.Sprintf("Health check failed: %v", err))
		if statusErr := r.Status().Update(ctx, cluster); statusErr != nil {
			log.Error(statusErr, "Failed to update status")
		}
		r.Recorder.Event(cluster, "Warning", "Unhealthy", fmt.Sprintf("Health check failed: %v", err))
		return ctrl.Result{RequeueAfter: clusterHealthCheckInterval}, nil
	}

	// Cluster is healthy
	cluster.Status.ObservedGeneration = cluster.Generation
	r.setCondition(cluster, typeReady, metav1.ConditionTrue, "Connected", "Cluster is reachable and healthy")

	if err := r.Status().Update(ctx, cluster); err != nil {
		log.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	log.V(1).Info("Cluster health check passed")
	return ctrl.Result{RequeueAfter: clusterHealthCheckInterval}, nil
}

// setCondition sets a condition on the Cluster status
func (r *ClusterReconciler) setCondition(cluster *bufstreamv1alpha1.Cluster, conditionType string, status metav1.ConditionStatus, reason, message string) {
	condition := metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: cluster.Generation,
		LastTransitionTime: metav1.Now(),
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, condition)
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&bufstreamv1alpha1.Cluster{}).
		Named("cluster").
		Complete(r)
}
