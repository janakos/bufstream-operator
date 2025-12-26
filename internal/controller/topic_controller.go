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
	// topicFinalizer is added to Topic resources to ensure cleanup
	topicFinalizer = "bufstream.buf.build/topic-finalizer"

	// Condition types
	typeReady       = "Ready"
	typeDegraded    = "Degraded"
	typeProgressing = "Progressing"

	// Requeue intervals
	requeueHealthy  = 5 * time.Minute
	requeueDegraded = 30 * time.Second
)

// TopicReconciler reconciles a Topic object
type TopicReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=bufstream.buf.build,resources=topics,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=bufstream.buf.build,resources=topics/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=bufstream.buf.build,resources=topics/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *TopicReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Fetch the Topic resource
	topic := &bufstreamv1alpha1.Topic{}
	if err := r.Get(ctx, req.NamespacedName, topic); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Topic resource not found, ignoring")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get Topic")
		return ctrl.Result{}, err
	}

	// Check if the resource is being deleted
	if !topic.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, topic)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(topic, topicFinalizer) {
		log.Info("Adding finalizer")
		controllerutil.AddFinalizer(topic, topicFinalizer)
		if err := r.Update(ctx, topic); err != nil {
			log.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Reconcile the topic
	return r.reconcileTopic(ctx, topic)
}

// reconcileDelete handles deletion of the Topic resource
func (r *TopicReconciler) reconcileDelete(ctx context.Context, topic *bufstreamv1alpha1.Topic) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(topic, topicFinalizer) {
		return ctrl.Result{}, nil
	}

	log.Info("Deleting topic from Bufstream")

	// Connect to Bufstream
	bsClient, err := bufstream.NewClient(ctx, topic.Spec.BootstrapServers)
	if err != nil {
		log.Error(err, "Failed to connect to Bufstream during deletion, will retry")
		r.Recorder.Event(topic, "Warning", "DeletionFailed", fmt.Sprintf("Failed to connect to Bufstream: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}
	defer bsClient.Close()

	// Get topic name
	topicName := r.getTopicName(topic)

	// Delete the topic
	log.Info("Deleting topic", "topicName", topicName)
	if err := bsClient.DeleteTopic(ctx, topicName); err != nil {
		log.Error(err, "Failed to delete topic")
		r.Recorder.Event(topic, "Warning", "DeletionFailed", fmt.Sprintf("Failed to delete topic: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}

	log.Info("Topic deleted successfully")
	r.Recorder.Event(topic, "Normal", "TopicDeleted", fmt.Sprintf("Topic %s deleted from Bufstream", topicName))

	// Remove finalizer
	controllerutil.RemoveFinalizer(topic, topicFinalizer)
	if err := r.Update(ctx, topic); err != nil {
		log.Error(err, "Failed to remove finalizer")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// reconcileTopic handles creation and updates of the topic
func (r *TopicReconciler) reconcileTopic(ctx context.Context, topic *bufstreamv1alpha1.Topic) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Check if we've already reconciled this generation
	if topic.Status.ObservedGeneration == topic.Generation {
		// Check if topic is ready
		readyCondition := meta.FindStatusCondition(topic.Status.Conditions, typeReady)
		if readyCondition != nil && readyCondition.Status == metav1.ConditionTrue {
			log.V(1).Info("Topic already reconciled and ready, skipping")
			return ctrl.Result{RequeueAfter: requeueHealthy}, nil
		}
	}

	// Set progressing condition
	r.setCondition(topic, typeProgressing, metav1.ConditionTrue, "Reconciling", "Reconciling topic")

	// Connect to Bufstream
	bsClient, err := bufstream.NewClient(ctx, topic.Spec.BootstrapServers)
	if err != nil {
		log.Error(err, "Failed to connect to Bufstream")
		r.setCondition(topic, typeDegraded, metav1.ConditionTrue, "ConnectionFailed", fmt.Sprintf("Failed to connect: %v", err))
		r.setCondition(topic, typeReady, metav1.ConditionFalse, "ConnectionFailed", "Cannot connect to Bufstream")
		if statusErr := r.Status().Update(ctx, topic); statusErr != nil {
			log.Error(statusErr, "Failed to update status")
		}
		r.Recorder.Event(topic, "Warning", "ConnectionFailed", fmt.Sprintf("Failed to connect to Bufstream: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}
	defer bsClient.Close()

	// Check Bufstream health
	if err := bsClient.CheckHealth(ctx); err != nil {
		log.Error(err, "Bufstream health check failed")
		r.setCondition(topic, typeDegraded, metav1.ConditionTrue, "Unhealthy", fmt.Sprintf("Bufstream unhealthy: %v", err))
		r.setCondition(topic, typeReady, metav1.ConditionFalse, "Unhealthy", "Bufstream is unhealthy")
		if statusErr := r.Status().Update(ctx, topic); statusErr != nil {
			log.Error(statusErr, "Failed to update status")
		}
		r.Recorder.Event(topic, "Warning", "BufstreamUnhealthy", fmt.Sprintf("Bufstream health check failed: %v", err))
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}

	// Get topic name
	topicName := r.getTopicName(topic)

	// Check if topic exists
	exists, currentInfo, err := bsClient.TopicExists(ctx, topicName)
	if err != nil {
		log.Error(err, "Failed to check if topic exists")
		r.setCondition(topic, typeDegraded, metav1.ConditionTrue, "CheckFailed", fmt.Sprintf("Failed to check topic: %v", err))
		r.setCondition(topic, typeReady, metav1.ConditionFalse, "CheckFailed", "Failed to check topic existence")
		if statusErr := r.Status().Update(ctx, topic); statusErr != nil {
			log.Error(statusErr, "Failed to update status")
		}
		return ctrl.Result{RequeueAfter: requeueDegraded}, err
	}

	if !exists {
		// Create the topic
		log.Info("Creating topic", "topicName", topicName)
		config := bufstream.TopicConfig{
			Name:              topicName,
			Partitions:        topic.Spec.Partitions,
			ReplicationFactor: int16(topic.Spec.ReplicationFactor),
			Config:            topic.Spec.Config,
		}
		if err := bsClient.CreateTopic(ctx, config); err != nil {
			log.Error(err, "Failed to create topic")
			r.setCondition(topic, typeDegraded, metav1.ConditionTrue, "CreationFailed", fmt.Sprintf("Failed to create topic: %v", err))
			r.setCondition(topic, typeReady, metav1.ConditionFalse, "CreationFailed", "Topic creation failed")
			if statusErr := r.Status().Update(ctx, topic); statusErr != nil {
				log.Error(statusErr, "Failed to update status")
			}
			r.Recorder.Event(topic, "Warning", "CreationFailed", fmt.Sprintf("Failed to create topic: %v", err))
			return ctrl.Result{RequeueAfter: requeueDegraded}, err
		}
		log.Info("Topic created successfully")
		r.Recorder.Event(topic, "Normal", "TopicCreated", fmt.Sprintf("Topic %s created", topicName))
	} else {
		// Topic exists
		if !topic.Spec.AssumeOwnership {
			err := fmt.Errorf("topic %s already exists and assumeOwnership is false", topicName)
			log.Error(err, "Topic exists but assumeOwnership is false")
			r.setCondition(topic, typeDegraded, metav1.ConditionTrue, "TopicExists", err.Error())
			r.setCondition(topic, typeReady, metav1.ConditionFalse, "TopicExists", "Topic exists, cannot assume ownership")
			if statusErr := r.Status().Update(ctx, topic); statusErr != nil {
				log.Error(statusErr, "Failed to update status")
			}
			r.Recorder.Event(topic, "Warning", "TopicExists", err.Error())
			return ctrl.Result{}, err
		}

		log.Info("Topic exists, assuming ownership", "topicName", topicName)
		// Update topic if needed
		desiredReplicas := int16(topic.Spec.ReplicationFactor)
		if desiredReplicas == 0 {
			desiredReplicas = 1
		}
		if err := bsClient.UpdateTopic(ctx, topicName, topic.Spec.Partitions, desiredReplicas, currentInfo, topic.Spec.Config); err != nil {
			log.Error(err, "Failed to update topic")
			r.setCondition(topic, typeDegraded, metav1.ConditionTrue, "UpdateFailed", fmt.Sprintf("Failed to update topic: %v", err))
			r.setCondition(topic, typeReady, metav1.ConditionFalse, "UpdateFailed", "Topic update failed")
			if statusErr := r.Status().Update(ctx, topic); statusErr != nil {
				log.Error(statusErr, "Failed to update status")
			}
			r.Recorder.Event(topic, "Warning", "UpdateFailed", fmt.Sprintf("Failed to update topic: %v", err))
			return ctrl.Result{RequeueAfter: requeueDegraded}, err
		}
		r.Recorder.Event(topic, "Normal", "TopicUpdated", fmt.Sprintf("Topic %s updated", topicName))
	}

	// Update status
	topic.Status.ObservedGeneration = topic.Generation
	topic.Status.TopicName = topicName
	topic.Status.Partitions = topic.Spec.Partitions
	topic.Status.ReplicationFactor = topic.Spec.ReplicationFactor

	// Set ready condition
	r.setCondition(topic, typeDegraded, metav1.ConditionFalse, "Healthy", "Topic is healthy")
	r.setCondition(topic, typeProgressing, metav1.ConditionFalse, "Reconciled", "Topic reconciled successfully")
	r.setCondition(topic, typeReady, metav1.ConditionTrue, "Ready", "Topic is ready")

	if err := r.Status().Update(ctx, topic); err != nil {
		log.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	log.Info("Topic reconciled successfully")
	return ctrl.Result{RequeueAfter: requeueHealthy}, nil
}

// getTopicName returns the topic name from spec or defaults to resource name
func (r *TopicReconciler) getTopicName(topic *bufstreamv1alpha1.Topic) string {
	if topic.Spec.TopicName != "" {
		return topic.Spec.TopicName
	}
	return topic.Name
}

// setCondition sets a condition on the Topic status
func (r *TopicReconciler) setCondition(topic *bufstreamv1alpha1.Topic, conditionType string, status metav1.ConditionStatus, reason, message string) {
	condition := metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: topic.Generation,
		LastTransitionTime: metav1.Now(),
	}
	meta.SetStatusCondition(&topic.Status.Conditions, condition)
}

// SetupWithManager sets up the controller with the Manager.
func (r *TopicReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&bufstreamv1alpha1.Topic{}).
		Named("topic").
		Complete(r)
}
