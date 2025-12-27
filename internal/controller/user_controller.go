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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	bufstreamv1alpha1 "github.com/janakos/bufstream-operator/api/v1alpha1"
	"github.com/janakos/bufstream-operator/internal/bufstream"
)

const (
	// userFinalizer is added to User resources to ensure cleanup
	userFinalizer = "bufstream.buf.build/user-finalizer"
)

// UserReconciler reconciles a User object
type UserReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=bufstream.buf.build,resources=users,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=bufstream.buf.build,resources=users/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=bufstream.buf.build,resources=users/finalizers,verbs=update
// +kubebuilder:rbac:groups=bufstream.buf.build,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop
func (r *UserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Fetch the User resource
	user := &bufstreamv1alpha1.User{}
	if err := r.Get(ctx, req.NamespacedName, user); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("User resource not found, ignoring")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get User")
		return ctrl.Result{}, err
	}

	// Check if the resource is being deleted
	if !user.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, user)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(user, userFinalizer) {
		log.Info("Adding finalizer")
		controllerutil.AddFinalizer(user, userFinalizer)
		if err := r.Update(ctx, user); err != nil {
			log.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Reconcile the user
	return r.reconcileUser(ctx, user)
}

// reconcileDelete handles deletion of the User resource
func (r *UserReconciler) reconcileDelete(ctx context.Context, user *bufstreamv1alpha1.User) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(user, userFinalizer) {
		return ctrl.Result{}, nil
	}

	// Get cluster and check if ready before attempting deletion
	cluster, err := GetCluster(ctx, r.Client, user.Spec.Cluster, user.Namespace)
	if err != nil {
		log.Error(err, "Failed to get cluster during deletion")
		r.Recorder.Event(user, "Warning", "DeletionFailed", fmt.Sprintf("Failed to get cluster: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}
	if !IsClusterReady(cluster) {
		log.Info("Cluster not ready, waiting before deletion", "cluster", user.Spec.Cluster)
		return ctrl.Result{RequeueAfter: clusterNotReadyRequeue}, nil
	}

	log.Info("Deleting user from Bufstream")

	// Get admin credentials for SASL authentication
	saslConfig, err := GetAdminCredentials(ctx, r.Client, cluster)
	if err != nil {
		log.Error(err, "Failed to get admin credentials during deletion")
		r.Recorder.Event(user, "Warning", "DeletionFailed", fmt.Sprintf("Failed to get admin credentials: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}

	// Connect to Bufstream with SASL authentication
	bsClient, err := bufstream.NewClientWithSASL(ctx, cluster.Spec.BootstrapServers, saslConfig)
	if err != nil {
		log.Error(err, "Failed to connect to Bufstream during deletion, will retry")
		r.Recorder.Event(user, "Warning", "DeletionFailed", fmt.Sprintf("Failed to connect to Bufstream: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}
	defer bsClient.Close()

	// Get username and mechanism
	username := r.getUsername(user)
	mechanism := r.getMechanism(user)

	// Delete the user credentials
	log.Info("Deleting user credentials", "username", username)
	if err := bsClient.DeleteUserCredentials(ctx, username, mechanism); err != nil {
		log.Error(err, "Failed to delete user credentials")
		r.Recorder.Event(user, "Warning", "DeletionFailed", fmt.Sprintf("Failed to delete user: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}

	log.Info("User deleted successfully")
	r.Recorder.Event(user, "Normal", "UserDeleted", fmt.Sprintf("User %s deleted from Bufstream", username))

	// Remove finalizer
	controllerutil.RemoveFinalizer(user, userFinalizer)
	if err := r.Update(ctx, user); err != nil {
		log.Error(err, "Failed to remove finalizer")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// reconcileUser handles creation and updates of the user
func (r *UserReconciler) reconcileUser(ctx context.Context, user *bufstreamv1alpha1.User) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Check if we've already reconciled this generation
	if user.Status.ObservedGeneration == user.Generation {
		readyCondition := meta.FindStatusCondition(user.Status.Conditions, typeReady)
		if readyCondition != nil && readyCondition.Status == metav1.ConditionTrue {
			log.V(1).Info("User already reconciled and ready, skipping")
			return ctrl.Result{RequeueAfter: requeueHealthy}, nil
		}
	}

	// Get cluster and check if ready before attempting operations
	cluster, err := GetCluster(ctx, r.Client, user.Spec.Cluster, user.Namespace)
	if err != nil {
		log.Error(err, "Failed to get cluster")
		r.setCondition(user, typeDegraded, metav1.ConditionTrue, "ClusterError", fmt.Sprintf("Failed to get cluster: %v", err))
		r.setCondition(user, typeReady, metav1.ConditionFalse, "ClusterError", "Cannot get cluster")
		if statusErr := r.Status().Update(ctx, user); statusErr != nil {
			log.Error(statusErr, "Failed to update status")
		}
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}
	if !IsClusterReady(cluster) {
		log.Info("Cluster not ready, waiting", "cluster", user.Spec.Cluster)
		return ctrl.Result{RequeueAfter: clusterNotReadyRequeue}, nil
	}

	// Set progressing condition
	r.setCondition(user, typeProgressing, metav1.ConditionTrue, "Reconciling", "Reconciling user")

	// Get admin credentials for SASL authentication
	saslConfig, err := GetAdminCredentials(ctx, r.Client, cluster)
	if err != nil {
		log.Error(err, "Failed to get admin credentials")
		r.setCondition(user, typeDegraded, metav1.ConditionTrue, "ConfigurationError", fmt.Sprintf("Failed to get admin credentials: %v", err))
		r.setCondition(user, typeReady, metav1.ConditionFalse, "ConfigurationError", "Cannot get admin credentials")
		if statusErr := r.Status().Update(ctx, user); statusErr != nil {
			log.Error(statusErr, "Failed to update status")
		}
		r.Recorder.Event(user, "Warning", "ConfigurationError", fmt.Sprintf("Failed to get admin credentials: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}

	// Get password from secret
	password, err := r.getPassword(ctx, user)
	if err != nil {
		log.Error(err, "Failed to get password from secret")
		r.setCondition(user, typeDegraded, metav1.ConditionTrue, "SecretError", fmt.Sprintf("Failed to get password: %v", err))
		r.setCondition(user, typeReady, metav1.ConditionFalse, "SecretError", "Cannot get password from secret")
		if statusErr := r.Status().Update(ctx, user); statusErr != nil {
			log.Error(statusErr, "Failed to update status")
		}
		r.Recorder.Event(user, "Warning", "SecretError", fmt.Sprintf("Failed to get password: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}

	// Connect to Bufstream with SASL authentication
	bsClient, err := bufstream.NewClientWithSASL(ctx, cluster.Spec.BootstrapServers, saslConfig)
	if err != nil {
		log.Error(err, "Failed to connect to Bufstream")
		r.setCondition(user, typeDegraded, metav1.ConditionTrue, "ConnectionFailed", fmt.Sprintf("Failed to connect: %v", err))
		r.setCondition(user, typeReady, metav1.ConditionFalse, "ConnectionFailed", "Cannot connect to Bufstream")
		if statusErr := r.Status().Update(ctx, user); statusErr != nil {
			log.Error(statusErr, "Failed to update status")
		}
		r.Recorder.Event(user, "Warning", "ConnectionFailed", fmt.Sprintf("Failed to connect to Bufstream: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}
	defer bsClient.Close()

	// Get user details
	username := r.getUsername(user)
	mechanism := r.getMechanism(user)
	iterations := user.Spec.Iterations
	if iterations == 0 {
		iterations = 4096
	}

	// Upsert user credentials
	log.Info("Upserting user credentials", "username", username)
	creds := bufstream.UserCredentials{
		Username:   username,
		Password:   password,
		Mechanism:  mechanism,
		Iterations: iterations,
	}
	if err := bsClient.UpsertUserCredentials(ctx, creds); err != nil {
		log.Error(err, "Failed to upsert user credentials")
		r.setCondition(user, typeDegraded, metav1.ConditionTrue, "UpsertFailed", fmt.Sprintf("Failed to upsert user: %v", err))
		r.setCondition(user, typeReady, metav1.ConditionFalse, "UpsertFailed", "User upsert failed")
		if statusErr := r.Status().Update(ctx, user); statusErr != nil {
			log.Error(statusErr, "Failed to update status")
		}
		r.Recorder.Event(user, "Warning", "UpsertFailed", fmt.Sprintf("Failed to upsert user: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}

	log.Info("User credentials upserted successfully")
	r.Recorder.Event(user, "Normal", "UserUpserted", fmt.Sprintf("User %s upserted", username))

	// Update status
	user.Status.ObservedGeneration = user.Generation
	user.Status.Username = username
	user.Status.Mechanism = user.Spec.Mechanism
	if user.Status.Mechanism == "" {
		user.Status.Mechanism = bufstreamv1alpha1.ScramSHA512
	}

	// Set ready condition
	r.setCondition(user, typeDegraded, metav1.ConditionFalse, "Healthy", "User is healthy")
	r.setCondition(user, typeProgressing, metav1.ConditionFalse, "Reconciled", "User reconciled successfully")
	r.setCondition(user, typeReady, metav1.ConditionTrue, "Ready", "User is ready")

	if err := r.Status().Update(ctx, user); err != nil {
		log.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	log.Info("User reconciled successfully")
	return ctrl.Result{RequeueAfter: requeueHealthy}, nil
}

// getPassword retrieves the password from the referenced secret
func (r *UserReconciler) getPassword(ctx context.Context, user *bufstreamv1alpha1.User) (string, error) {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      user.Spec.SecretRef.Name,
		Namespace: user.Namespace,
	}, secret); err != nil {
		return "", fmt.Errorf("failed to get Secret %q: %w", user.Spec.SecretRef.Name, err)
	}

	key := user.Spec.SecretRef.Key
	if key == "" {
		key = "password"
	}

	password, ok := secret.Data[key]
	if !ok {
		return "", fmt.Errorf("secret %q does not contain key %q", user.Spec.SecretRef.Name, key)
	}

	return string(password), nil
}

// getUsername returns the username from spec or defaults to resource name
func (r *UserReconciler) getUsername(user *bufstreamv1alpha1.User) string {
	if user.Spec.Username != "" {
		return user.Spec.Username
	}
	return user.Name
}

// getMechanism returns the SCRAM mechanism
func (r *UserReconciler) getMechanism(user *bufstreamv1alpha1.User) bufstream.ScramMechanism {
	switch user.Spec.Mechanism {
	case bufstreamv1alpha1.ScramSHA256:
		return bufstream.ScramSHA256
	case bufstreamv1alpha1.ScramSHA512:
		return bufstream.ScramSHA512
	default:
		return bufstream.ScramSHA512
	}
}

// setCondition sets a condition on the User status
func (r *UserReconciler) setCondition(user *bufstreamv1alpha1.User, conditionType string, status metav1.ConditionStatus, reason, message string) {
	condition := metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: user.Generation,
		LastTransitionTime: metav1.Now(),
	}
	meta.SetStatusCondition(&user.Status.Conditions, condition)
}

// SetupWithManager sets up the controller with the Manager.
func (r *UserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&bufstreamv1alpha1.User{}).
		Named("user").
		Complete(r)
}
