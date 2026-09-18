// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package controllers

import (
	"context"
	stderrors "errors"
	"time"

	"github.com/aws/amazon-vpc-resource-controller-k8s/apis/vpcresources/v1alpha1"
	cninodepkg "github.com/aws/amazon-vpc-resource-controller-k8s/pkg/cninode"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/condition"
	rcHealthz "github.com/aws/amazon-vpc-resource-controller-k8s/pkg/healthz"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/k8s"
	nodepkg "github.com/aws/amazon-vpc-resource-controller-k8s/pkg/node"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/node/manager"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
)

// MaxNodeConcurrentReconciles is the number of go routines that can invoke
// Reconcile in parallel. Since Node Reconciler, performs local operation
// on cache only a single go routine should be sufficient. Using more than
// one routines to help high rate churn and larger nodes groups restarting
// when the controller has to be restarted for various reasons.
const (
	NodeTerminationFinalizer  = "networking.k8s.aws/resource-cleanup"
	computeTypeLabelKey       = "eks.amazonaws.com/compute-type"
	autoComputeTypeLabelValue = "auto"
)

// NodeReconciler reconciles a Node object
type NodeReconciler struct {
	client.Client
	APIReader  client.Reader
	K8sAPI     k8s.K8sWrapper
	Log        logr.Logger
	Scheme     *runtime.Scheme
	Manager    manager.Manager
	Conditions condition.Conditions
	Context    context.Context
}

// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=core,resources=nodes/status,verbs=get;patch

// Reconcile Adds a new node by calling the Node Manager. A node can be added as a
// Managed Node in which case the controller can provide resources for Pod's scheduled
// on the node or it can be added as a un managed Node in which case controller doesn't
// do any operations on the Node or any Pods scheduled on the Node. A node can be toggled
// from Un-Managed to Managed and vice-versa in which case the Node Manager updates it's
// status accordingly
func (r *NodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if !r.Conditions.GetPodDataStoreSyncStatus() {
		// if pod cache is not ready, let's exponentially requeue the requests instead of letting routines wait
		r.Log.Info("waiting for pod datastore to sync")
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	node := &corev1.Node{}

	logger := r.Log.WithValues("node", req.NamespacedName)

	if nodeErr := r.reader().Get(ctx, req.NamespacedName, node); nodeErr != nil {
		return r.reconcileMissingNode(req.Name, nodeErr, logger)
	}
	computeKey, ok := node.Labels[computeTypeLabelKey]
	if ok && computeKey == autoComputeTypeLabelValue {
		logger.Info("node is auto compute type, skipping")
		return ctrl.Result{}, nil
	}
	if !node.GetDeletionTimestamp().IsZero() {
		return r.reconcileTerminatingNode(req.Name, logger)
	}

	if result, handled, err := r.reconcileCNINodeGeneration(ctx, req, node, logger); handled {
		return result, err
	}

	return r.reconcileManagerGeneration(req.Name, node, logger)
}

func (r *NodeReconciler) reconcileMissingNode(nodeName string, nodeErr error,
	logger logr.Logger,
) (ctrl.Result, error) {
	if !k8serrors.IsNotFound(nodeErr) {
		return ctrl.Result{}, nodeErr
	}

	if _, found := r.Manager.GetNode(nodeName); !found {
		return ctrl.Result{}, nil
	}
	if err := r.Manager.DeleteNode(nodeName); err != nil {
		// The request is not retryable so not returning the error.
		logger.Error(err, "failed to delete node from manager")
		return ctrl.Result{}, nil
	}
	logger.V(1).Info("deleted the node from manager")
	return ctrl.Result{}, nil
}

func (r *NodeReconciler) reconcileCNINodeGeneration(ctx context.Context, req ctrl.Request,
	node *corev1.Node, logger logr.Logger,
) (ctrl.Result, bool, error) {
	checkpoint, checkpointFound, err := replacementCheckpointForNode(node)
	if err != nil {
		return ctrl.Result{}, true, err
	}

	cniNode := &v1alpha1.CNINode{}
	if getErr := r.reader().Get(ctx, req.NamespacedName, cniNode); getErr != nil {
		return r.reconcileMissingCNINode(ctx, node, getErr, logger)
	}
	if !cniNode.IsManagedByVPCResourceController() {
		if checkpointFound {
			logger.Info("waiting for replacement CNINode managed by this controller")
			return ctrl.Result{RequeueAfter: time.Second}, true, nil
		}
		return ctrl.Result{}, false, nil
	}
	if !cniNode.GetDeletionTimestamp().IsZero() {
		return r.waitForDeletingCNINode(ctx, node, cniNode, logger)
	}
	if !cninodepkg.OwnedByNode(cniNode, node) {
		return r.deleteStaleCNINode(ctx, node, cniNode, logger)
	}
	if checkpointFound {
		if checkpoint.SourceCNINodeUID != "" && checkpoint.SourceCNINodeUID == cniNode.UID {
			logger.Info("waiting for checkpointed CNINode deletion to become visible",
				"cniNodeUID", cniNode.UID)
			return ctrl.Result{RequeueAfter: time.Second}, true, nil
		}
		if err := r.clearCNINodeReplacementCheckpoint(ctx, node); err != nil {
			return ctrl.Result{}, true, err
		}
	}
	return ctrl.Result{}, false, nil
}

func replacementCheckpointForNode(node *corev1.Node) (*cninodepkg.ReplacementCheckpoint, bool, error) {
	return cninodepkg.Checkpoint(node)
}

func (r *NodeReconciler) reconcileMissingCNINode(ctx context.Context, node *corev1.Node, getErr error,
	logger logr.Logger,
) (ctrl.Result, bool, error) {
	if !k8serrors.IsNotFound(getErr) {
		return ctrl.Result{}, true, getErr
	}
	// Re-read the Node after observing CNINode NotFound. The CNINode controller
	// may have persisted the replacement checkpoint and deleted the old object
	// between the two cross-resource reads above.
	freshNode := &corev1.Node{}
	if err := r.reader().Get(ctx, types.NamespacedName{Name: node.Name}, freshNode); err != nil {
		if k8serrors.IsNotFound(err) {
			result, reconcileErr := r.reconcileMissingNode(node.Name, err, logger)
			return result, true, reconcileErr
		}
		return ctrl.Result{}, true, err
	}
	*node = *freshNode
	if !node.GetDeletionTimestamp().IsZero() {
		result, err := r.reconcileTerminatingNode(node.Name, logger)
		return result, true, err
	}
	checkpoint, checkpointFound, err := replacementCheckpointForNode(node)
	if err != nil {
		return ctrl.Result{}, true, err
	}
	if !checkpointFound {
		checkpoint, checkpointFound, err = cninodepkg.GetCheckpointRecord(ctx, r.reader(), node.Name)
		if err != nil {
			return ctrl.Result{}, true, err
		}
	}
	if !checkpointFound {
		return ctrl.Result{}, false, nil
	}
	if checkpoint.NodeUID != node.UID {
		if err := r.persistCNINodeReplacementCheckpoint(
			ctx, node, checkpoint.Spec, checkpoint.SourceCNINodeUID); err != nil {
			return ctrl.Result{}, true, err
		}
		logger.Info("moved CNINode replacement checkpoint to the current Node generation",
			"checkpointNodeUID", checkpoint.NodeUID, "currentNodeUID", node.UID)
		return ctrl.Result{RequeueAfter: time.Second}, true, nil
	}
	successor := cninodepkg.NewSuccessor(node, checkpoint.Spec)
	if err := r.Client.Create(ctx, successor); err != nil && !k8serrors.IsAlreadyExists(err) {
		return ctrl.Result{}, true, err
	}
	logger.Info("created replacement CNINode for the current Node generation",
		"currentNodeUID", node.UID)
	return ctrl.Result{RequeueAfter: time.Second}, true, nil
}

func (r *NodeReconciler) waitForDeletingCNINode(ctx context.Context, node *corev1.Node,
	cniNode *v1alpha1.CNINode, logger logr.Logger,
) (ctrl.Result, bool, error) {
	if err := r.persistCNINodeReplacementCheckpoint(ctx, node, cniNode.Spec, cniNode.UID); err != nil {
		return ctrl.Result{}, true, err
	}
	logger.Info("waiting for the existing CNINode to finish deleting")
	return ctrl.Result{RequeueAfter: time.Second}, true, nil
}

func (r *NodeReconciler) deleteStaleCNINode(ctx context.Context, node *corev1.Node,
	cniNode *v1alpha1.CNINode, logger logr.Logger,
) (ctrl.Result, bool, error) {
	if err := r.persistCNINodeReplacementCheckpoint(ctx, node, cniNode.Spec, cniNode.UID); err != nil {
		return ctrl.Result{}, true, err
	}
	uid := cniNode.UID
	resourceVersion := cniNode.ResourceVersion
	if err := r.Client.Delete(ctx, cniNode, &client.DeleteOptions{
		Preconditions: &metav1.Preconditions{
			UID:             &uid,
			ResourceVersion: &resourceVersion,
		},
	}); err != nil && !k8serrors.IsNotFound(err) {
		return ctrl.Result{}, true, err
	}
	logger.Info("deleting stale CNINode before initializing the current Node",
		"cniNodeOwnerReferences", cniNode.OwnerReferences, "currentNodeUID", node.UID)
	return ctrl.Result{RequeueAfter: time.Second}, true, nil
}

func (r *NodeReconciler) reconcileManagerGeneration(nodeName string, node *corev1.Node,
	logger logr.Logger,
) (ctrl.Result, error) {
	var err error
	cachedNode, found := r.Manager.GetNode(nodeName)
	if found {
		currentInstanceID := manager.GetNodeInstanceID(node)
		cachedInstanceID := cachedNode.GetNodeInstanceID()
		currentNodeUID := node.UID
		cachedNodeUID := nodepkg.GetNodeUID(cachedNode)
		instanceMismatch := currentInstanceID != "" && cachedInstanceID != "" &&
			currentInstanceID != cachedInstanceID
		uidMismatch := currentNodeUID != "" && cachedNodeUID != "" &&
			currentNodeUID != cachedNodeUID
		if instanceMismatch || uidMismatch {
			if instanceMismatch {
				nodepkg.RecordInstanceIDMismatch()
			}
			logger.Info("cached node identity does not match the current Kubernetes Node",
				"cachedNodeUID", cachedNodeUID, "currentNodeUID", currentNodeUID,
				"cachedInstanceID", cachedInstanceID, "currentInstanceID", currentInstanceID)
			if err := r.Manager.DeleteNode(nodeName); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		if !cachedNode.IsReady() {
			logger.Info("waiting for node resource initialization")
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}

		logger.V(1).Info("updating node")
		err = r.Manager.UpdateNode(nodeName)
		if stderrors.Is(err, manager.ErrNodeCleanupInProgress) {
			logger.Info("waiting for previous node generation cleanup")
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}

		// ReconcileNode actually run a branch ENI leaking check from an independent goroutine on added nodes.
		r.Manager.CheckNodeForLeakedENIs(nodeName)
	} else {
		logger.Info("adding node")
		err = r.Manager.AddNode(nodeName)
		if stderrors.Is(err, manager.ErrNodeCleanupInProgress) {
			logger.Info("waiting for previous node generation cleanup")
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
	}

	return ctrl.Result{}, err
}

func (r *NodeReconciler) persistCNINodeReplacementCheckpoint(ctx context.Context, node *corev1.Node,
	spec v1alpha1.CNINodeSpec, sourceCNINodeUID types.UID,
) error {
	if _, err := cninodepkg.EnsureCheckpointRecord(
		ctx, r.Client, r.reader(), node.Name, node.UID, sourceCNINodeUID, spec); err != nil {
		return err
	}
	updated := node.DeepCopy()
	if err := cninodepkg.SetCheckpoint(updated, spec, sourceCNINodeUID); err != nil {
		return err
	}
	if node.Annotations[cninodepkg.ReplacementCheckpointAnnotation] ==
		updated.Annotations[cninodepkg.ReplacementCheckpointAnnotation] {
		return nil
	}
	return r.Client.Patch(ctx, updated,
		client.MergeFromWithOptions(node, client.MergeFromWithOptimisticLock{}))
}

func (r *NodeReconciler) clearCNINodeReplacementCheckpoint(ctx context.Context,
	node *corev1.Node,
) error {
	checkpoint, found, err := cninodepkg.Checkpoint(node)
	if err != nil {
		return err
	}
	if found {
		if err := cninodepkg.DeleteCheckpointRecord(
			ctx, r.Client, r.reader(), node.Name, checkpoint.SourceCNINodeUID); err != nil {
			return err
		}
	}
	updated := node.DeepCopy()
	cninodepkg.ClearCheckpoint(updated)
	return r.Client.Patch(ctx, updated,
		client.MergeFromWithOptions(node, client.MergeFromWithOptimisticLock{}))
}

func (r *NodeReconciler) SetupWithManager(mgr ctrl.Manager, maxConcurrentReconciles int, healthzHandler *rcHealthz.HealthzHandler) error {
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	// add health check on subpath for node controller
	healthzHandler.AddControllersHealthCheckers(
		map[string]healthz.Checker{"health-node-controller": r.Check()},
	)

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxConcurrentReconciles}).
		Owns(&v1alpha1.CNINode{}).
		Complete(r)
}

func (r *NodeReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *NodeReconciler) reconcileTerminatingNode(nodeName string,
	logger logr.Logger,
) (ctrl.Result, error) {
	if _, found := r.Manager.GetNode(nodeName); !found {
		return ctrl.Result{}, nil
	}
	if err := r.Manager.DeleteNode(nodeName); err != nil {
		logger.Error(err, "failed to delete terminating node from manager")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *NodeReconciler) Check() healthz.Checker {
	r.Log.Info("Node controller's healthz subpath was added")
	return rcHealthz.SimplePing("node-reconciler", r.Log)
}
