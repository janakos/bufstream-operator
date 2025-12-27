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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	bufstreamv1alpha1 "github.com/janakos/bufstream-operator/api/v1alpha1"
	"github.com/janakos/bufstream-operator/internal/bufstream"
)

const (
	aclFinalizer = "bufstream.buf.build/acl-finalizer"
)

// ACLReconciler reconciles an ACL object
type ACLReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=bufstream.buf.build,resources=acls,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=bufstream.buf.build,resources=acls/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=bufstream.buf.build,resources=acls/finalizers,verbs=update

// Reconcile handles ACL resources
func (r *ACLReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Fetch the ACL resource
	acl := &bufstreamv1alpha1.ACL{}
	if err := r.Get(ctx, req.NamespacedName, acl); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("ACL resource not found, ignoring")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get ACL")
		return ctrl.Result{}, err
	}

	// Check if being deleted
	if !acl.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, acl)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(acl, aclFinalizer) {
		log.Info("Adding finalizer")
		controllerutil.AddFinalizer(acl, aclFinalizer)
		if err := r.Update(ctx, acl); err != nil {
			log.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Reconcile the ACL
	return r.reconcileACL(ctx, acl)
}

// reconcileDelete handles deletion of the ACL resource
func (r *ACLReconciler) reconcileDelete(ctx context.Context, acl *bufstreamv1alpha1.ACL) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(acl, aclFinalizer) {
		return ctrl.Result{}, nil
	}

	// Get cluster and check if ready
	cluster, err := GetCluster(ctx, r.Client, acl.Spec.Cluster, acl.Namespace)
	if err != nil {
		log.Error(err, "Failed to get cluster during deletion")
		r.Recorder.Event(acl, "Warning", "DeletionFailed", fmt.Sprintf("Failed to get cluster: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}
	if !IsClusterReady(cluster) {
		log.Info("Cluster not ready, waiting before deletion", "cluster", acl.Spec.Cluster)
		return ctrl.Result{RequeueAfter: clusterNotReadyRequeue}, nil
	}

	log.Info("Deleting ACL from Bufstream")

	// Get admin credentials
	saslConfig, err := GetAdminCredentials(ctx, r.Client, cluster)
	if err != nil {
		log.Error(err, "Failed to get admin credentials during deletion")
		r.Recorder.Event(acl, "Warning", "DeletionFailed", fmt.Sprintf("Failed to get admin credentials: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}

	// Connect to Bufstream
	bsClient, err := bufstream.NewClient(ctx, cluster.Spec.BootstrapServers, saslConfig)
	if err != nil {
		log.Error(err, "Failed to connect to Bufstream during deletion")
		r.Recorder.Event(acl, "Warning", "DeletionFailed", fmt.Sprintf("Failed to connect: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}
	defer bsClient.Close()

	// Delete the ACL
	aclConfig := r.toACLConfig(acl)
	if err := bsClient.DeleteACL(ctx, aclConfig); err != nil {
		log.Error(err, "Failed to delete ACL")
		r.Recorder.Event(acl, "Warning", "DeletionFailed", fmt.Sprintf("Failed to delete ACL: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}

	log.Info("ACL deleted successfully")
	r.Recorder.Event(acl, "Normal", "ACLDeleted", "ACL deleted from Bufstream")

	// Remove finalizer
	controllerutil.RemoveFinalizer(acl, aclFinalizer)
	if err := r.Update(ctx, acl); err != nil {
		log.Error(err, "Failed to remove finalizer")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// reconcileACL handles creation and updates of the ACL
func (r *ACLReconciler) reconcileACL(ctx context.Context, acl *bufstreamv1alpha1.ACL) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Check if already reconciled
	if acl.Status.ObservedGeneration == acl.Generation {
		readyCondition := meta.FindStatusCondition(acl.Status.Conditions, typeReady)
		if readyCondition != nil && readyCondition.Status == metav1.ConditionTrue {
			log.V(1).Info("ACL already reconciled and ready, skipping")
			return ctrl.Result{RequeueAfter: requeueHealthy}, nil
		}
	}

	// Get cluster
	cluster, err := GetCluster(ctx, r.Client, acl.Spec.Cluster, acl.Namespace)
	if err != nil {
		log.Error(err, "Failed to get cluster")
		r.setCondition(acl, typeDegraded, metav1.ConditionTrue, "ClusterError", fmt.Sprintf("Failed to get cluster: %v", err))
		r.setCondition(acl, typeReady, metav1.ConditionFalse, "ClusterError", "Cannot get cluster")
		if statusErr := r.Status().Update(ctx, acl); statusErr != nil {
			log.Error(statusErr, "Failed to update status")
		}
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}
	if !IsClusterReady(cluster) {
		log.Info("Cluster not ready, waiting", "cluster", acl.Spec.Cluster)
		return ctrl.Result{RequeueAfter: clusterNotReadyRequeue}, nil
	}

	// Set progressing
	r.setCondition(acl, typeProgressing, metav1.ConditionTrue, "Reconciling", "Reconciling ACL")

	// Get admin credentials
	saslConfig, err := GetAdminCredentials(ctx, r.Client, cluster)
	if err != nil {
		log.Error(err, "Failed to get admin credentials")
		r.setCondition(acl, typeDegraded, metav1.ConditionTrue, "CredentialsError", fmt.Sprintf("Failed to get credentials: %v", err))
		r.setCondition(acl, typeReady, metav1.ConditionFalse, "CredentialsError", "Cannot get admin credentials")
		if statusErr := r.Status().Update(ctx, acl); statusErr != nil {
			log.Error(statusErr, "Failed to update status")
		}
		r.Recorder.Event(acl, "Warning", "CredentialsError", fmt.Sprintf("Failed to get admin credentials: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}

	// Connect to Bufstream
	bsClient, err := bufstream.NewClient(ctx, cluster.Spec.BootstrapServers, saslConfig)
	if err != nil {
		log.Error(err, "Failed to connect to Bufstream")
		r.setCondition(acl, typeDegraded, metav1.ConditionTrue, "ConnectionFailed", fmt.Sprintf("Failed to connect: %v", err))
		r.setCondition(acl, typeReady, metav1.ConditionFalse, "ConnectionFailed", "Cannot connect to Bufstream")
		if statusErr := r.Status().Update(ctx, acl); statusErr != nil {
			log.Error(statusErr, "Failed to update status")
		}
		r.Recorder.Event(acl, "Warning", "ConnectionFailed", fmt.Sprintf("Failed to connect: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}
	defer bsClient.Close()

	// Create or verify ACL
	aclConfig := r.toACLConfig(acl)
	log.Info("Creating ACL", "principal", acl.Spec.Principal, "resource", acl.Spec.Resource.Type, "operation", acl.Spec.Operation)

	if err := bsClient.CreateACL(ctx, aclConfig); err != nil {
		log.Error(err, "Failed to create ACL")
		r.setCondition(acl, typeDegraded, metav1.ConditionTrue, "CreateFailed", fmt.Sprintf("Failed to create ACL: %v", err))
		r.setCondition(acl, typeReady, metav1.ConditionFalse, "CreateFailed", "Failed to create ACL")
		if statusErr := r.Status().Update(ctx, acl); statusErr != nil {
			log.Error(statusErr, "Failed to update status")
		}
		r.Recorder.Event(acl, "Warning", "CreateFailed", fmt.Sprintf("Failed to create ACL: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}

	log.Info("ACL created successfully")
	r.Recorder.Event(acl, "Normal", "ACLCreated", fmt.Sprintf("ACL for %s created", acl.Spec.Principal))

	// Update status
	acl.Status.ObservedGeneration = acl.Generation
	r.setCondition(acl, typeProgressing, metav1.ConditionFalse, "Reconciled", "ACL reconciled successfully")
	r.setCondition(acl, typeDegraded, metav1.ConditionFalse, "Healthy", "ACL is healthy")
	r.setCondition(acl, typeReady, metav1.ConditionTrue, "Ready", "ACL is ready")

	if err := r.Status().Update(ctx, acl); err != nil {
		log.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	log.Info("ACL reconciled successfully")
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

// toACLConfig converts the CRD spec to a bufstream.ACLConfig
func (r *ACLReconciler) toACLConfig(acl *bufstreamv1alpha1.ACL) bufstream.ACLConfig {
	config := bufstream.ACLConfig{
		Principal:    acl.Spec.Principal,
		Host:         acl.Spec.Host,
		ResourceName: acl.Spec.Resource.Name,
	}

	// Map resource type
	switch acl.Spec.Resource.Type {
	case bufstreamv1alpha1.ACLResourceTypeGroup:
		config.ResourceType = bufstream.ACLResourceTypeGroup
	case bufstreamv1alpha1.ACLResourceTypeCluster:
		config.ResourceType = bufstream.ACLResourceTypeCluster
	case bufstreamv1alpha1.ACLResourceTypeTransactionalId:
		config.ResourceType = bufstream.ACLResourceTypeTransactionalId
	default:
		config.ResourceType = bufstream.ACLResourceTypeTopic
	}

	// Map pattern type
	switch acl.Spec.Resource.PatternType {
	case bufstreamv1alpha1.ACLPatternTypePrefixed:
		config.ResourcePattern = bufstream.ACLPatternTypePrefixed
	default:
		config.ResourcePattern = bufstream.ACLPatternTypeLiteral
	}

	// Map permission
	switch acl.Spec.Permission {
	case bufstreamv1alpha1.ACLPermissionDeny:
		config.Permission = bufstream.ACLPermissionDeny
	default:
		config.Permission = bufstream.ACLPermissionAllow
	}

	// Map operation
	switch acl.Spec.Operation {
	case bufstreamv1alpha1.ACLOperationRead:
		config.Operation = bufstream.ACLOperationRead
	case bufstreamv1alpha1.ACLOperationWrite:
		config.Operation = bufstream.ACLOperationWrite
	case bufstreamv1alpha1.ACLOperationCreate:
		config.Operation = bufstream.ACLOperationCreate
	case bufstreamv1alpha1.ACLOperationDelete:
		config.Operation = bufstream.ACLOperationDelete
	case bufstreamv1alpha1.ACLOperationAlter:
		config.Operation = bufstream.ACLOperationAlter
	case bufstreamv1alpha1.ACLOperationDescribe:
		config.Operation = bufstream.ACLOperationDescribe
	case bufstreamv1alpha1.ACLOperationClusterAction:
		config.Operation = bufstream.ACLOperationClusterAction
	case bufstreamv1alpha1.ACLOperationDescribeConfigs:
		config.Operation = bufstream.ACLOperationDescribeConfigs
	case bufstreamv1alpha1.ACLOperationAlterConfigs:
		config.Operation = bufstream.ACLOperationAlterConfigs
	case bufstreamv1alpha1.ACLOperationIdempotentWrite:
		config.Operation = bufstream.ACLOperationIdempotentWrite
	case bufstreamv1alpha1.ACLOperationAll:
		config.Operation = bufstream.ACLOperationAll
	}

	return config
}

// setCondition sets a condition on the ACL status
func (r *ACLReconciler) setCondition(acl *bufstreamv1alpha1.ACL, conditionType string, status metav1.ConditionStatus, reason, message string) {
	condition := metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: acl.Generation,
		LastTransitionTime: metav1.Now(),
	}
	meta.SetStatusCondition(&acl.Status.Conditions, condition)
}

// SetupWithManager sets up the controller with the Manager.
func (r *ACLReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&bufstreamv1alpha1.ACL{}).
		Named("acl").
		Complete(r)
}
