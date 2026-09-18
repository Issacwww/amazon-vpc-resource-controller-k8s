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

package crds

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/aws/amazon-vpc-resource-controller-k8s/apis/vpcresources/v1alpha1"
	ec2API "github.com/aws/amazon-vpc-resource-controller-k8s/pkg/aws/ec2/api"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/aws/ec2/api/cleanup"
	cninodepkg "github.com/aws/amazon-vpc-resource-controller-k8s/pkg/cninode"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/config"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/k8s"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/utils"
	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	prometheusRegistered     = false
	recreateCNINodeCallCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "recreate_cniNode_call_count",
			Help: "The number of requests made by controller to recreate CNINode when node exists",
		},
	)
	recreateCNINodeErrCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "recreate_cniNode_err_count",
			Help: "The number of requests that failed when controller tried to recreate the CNINode",
		},
	)
)

func prometheusRegister() {
	prometheusRegistered = true

	metrics.Registry.MustRegister(
		recreateCNINodeCallCount,
		recreateCNINodeErrCount)

	prometheusRegistered = true
}

// CNINodeReconciler reconciles a CNINode object
type CNINodeReconciler struct {
	client.Client
	apiReader          client.Reader
	scheme             *runtime.Scheme
	context            context.Context
	log                logr.Logger
	eC2Wrapper         ec2API.EC2Wrapper
	k8sAPI             k8s.K8sWrapper
	clusterName        string
	vpcId              string
	finalizerManager   k8s.FinalizerManager
	newResourceCleaner func(nodeID string, eC2Wrapper ec2API.EC2Wrapper, vpcID string) cleanup.ResourceCleaner
}

func NewCNINodeReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	ctx context.Context,
	logger logr.Logger,
	ec2Wrapper ec2API.EC2Wrapper,
	k8sWrapper k8s.K8sWrapper,
	clusterName string,
	vpcId string,
	finalizerManager k8s.FinalizerManager,
	newResourceCleaner func(nodeID string, eC2Wrapper ec2API.EC2Wrapper, vpcID string) cleanup.ResourceCleaner,
) *CNINodeReconciler {
	return &CNINodeReconciler{
		Client:             client,
		apiReader:          client,
		scheme:             scheme,
		context:            ctx,
		log:                logger,
		eC2Wrapper:         ec2Wrapper,
		k8sAPI:             k8sWrapper,
		clusterName:        clusterName,
		vpcId:              vpcId,
		finalizerManager:   finalizerManager,
		newResourceCleaner: newResourceCleaner,
	}
}

//+kubebuilder:rbac:groups=vpcresources.k8s.aws,resources=cninodes,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=vpcresources.k8s.aws,resources=cninodes/status,verbs=get;update;patch

// Reconcile handles CNINode create/update/delete events
// Reconciler will add the finalizer and cluster name tag if it does not exist and finalize on CNINode on deletion to clean up leaked resource on node
func (r *CNINodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cniNode := &v1alpha1.CNINode{}
	if err := r.Client.Get(ctx, req.NamespacedName, cniNode); err != nil {
		// Ignore not found error
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if cninodepkg.IsCheckpointRecord(cniNode) {
		if slices.Contains(cniNode.Finalizers, config.NodeTerminationFinalizer) {
			return ctrl.Result{}, r.finalizerManager.RemoveFinalizers(
				ctx, cniNode, config.NodeTerminationFinalizer)
		}
		return ctrl.Result{}, nil
	}

	// Skip CNINodes owned by another controller (e.g. EKS Auto Mode). The owning
	// controller is responsible for the object's tags, finalizers, status, and
	// resource cleanup; adding our finalizer here would block deletion since our
	// finalizer routine never runs for these objects.
	if !cniNode.IsManagedByVPCResourceController() {
		r.log.V(1).Info("skipping CNINode managed by another controller", "cniNode", cniNode.Name, "managedBy", cniNode.Spec.ManagedBy)
		return ctrl.Result{}, nil
	}

	nodeFound := true
	node := &v1.Node{}
	if err := r.reader().Get(ctx, req.NamespacedName, node); err != nil {
		if errors.IsNotFound(err) {
			nodeFound = false
		} else {
			r.log.Error(err, "failed to get the node object in CNINode reconciliation, will retry")
			// Requeue request so it can be retried
			return ctrl.Result{}, err
		}
	}
	if nodeFound && !node.GetDeletionTimestamp().IsZero() {
		nodeFound = false
	}

	if cniNode.GetDeletionTimestamp().IsZero() {
		return r.reconcileActiveCNINode(ctx, cniNode, node, nodeFound)
	}
	if !nodeFound {
		return r.finalizeCNINodeWithoutNode(ctx, cniNode)
	}
	return r.replaceCNINodeForCurrentNode(ctx, req, cniNode, node)
}

func (r *CNINodeReconciler) reconcileActiveCNINode(ctx context.Context, cniNode *v1alpha1.CNINode,
	node *v1.Node, nodeFound bool,
) (ctrl.Result, error) {
	updated := cniNode.DeepCopy()
	shouldPatch := ensureMapValue(&updated.Spec.Tags, config.VPCCNIClusterNameKey, r.clusterName)
	if nodeFound {
		shouldPatch = ensureMapValue(&updated.Labels, config.NodeLabelOS,
			node.Labels[config.NodeLabelOS]) || shouldPatch
	}
	if shouldPatch {
		r.log.Info("patching CNINode to add required fields Tags and Labels", "cninode", cniNode.Name)
		return ctrl.Result{}, r.Client.Patch(ctx, updated,
			client.MergeFromWithOptions(cniNode, client.MergeFromWithOptimisticLock{}))
	}
	if err := r.finalizerManager.AddFinalizers(ctx, cniNode, config.NodeTerminationFinalizer); err != nil {
		r.log.Error(err, "failed to add finalizer on CNINode, will retry",
			"cniNode", cniNode.Name, "finalizer", config.NodeTerminationFinalizer)
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func ensureMapValue(values *map[string]string, key, value string) bool {
	if *values == nil {
		*values = make(map[string]string)
	}
	if current, ok := (*values)[key]; ok && current == value {
		return false
	}
	(*values)[key] = value
	return true
}

func (r *CNINodeReconciler) finalizeCNINodeWithoutNode(ctx context.Context,
	cniNode *v1alpha1.CNINode,
) (ctrl.Result, error) {
	if cniNode.Labels[config.NodeLabelOS] == config.OSLinux {
		r.log.Info("running the finalizer routine on cniNode", "cniNode", cniNode.Name)
		if nodeID := cniNode.Spec.Tags[config.NetworkInterfaceNodeIDKey]; nodeID != "" {
			if err := r.newResourceCleaner(nodeID, r.eC2Wrapper, r.vpcId).DeleteLeakedResources(); err != nil {
				r.log.Error(err, "failed to cleanup resources during node termination")
				ec2API.NodeTerminationENICleanupFailure.Inc()
			}
		}
	}
	if err := r.finalizerManager.RemoveFinalizers(ctx, cniNode, config.NodeTerminationFinalizer); err != nil {
		r.log.Error(err, "failed to remove finalizer on CNINode, will retry",
			"cniNode", cniNode.Name, "finalizer", config.NodeTerminationFinalizer)
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *CNINodeReconciler) replaceCNINodeForCurrentNode(ctx context.Context, req ctrl.Request,
	cniNode *v1alpha1.CNINode, node *v1.Node,
) (ctrl.Result, error) {
	// The current Node is present, so periodic cleanup handles any leaked ENIs.
	// Preserve desired configuration while dropping old instance-specific status.
	checkpointNode, checkpoint, err := r.ensureReplacementCheckpoint(
		ctx, node, cniNode.Spec, cniNode.UID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.finalizerManager.RemoveFinalizers(ctx, cniNode, config.NodeTerminationFinalizer); err != nil {
		r.log.Error(err, "failed to remove finalizer on CNINode, will retry")
		return ctrl.Result{}, err
	}
	if err := r.waitTillCNINodeDeleted(client.ObjectKeyFromObject(cniNode)); err != nil {
		r.k8sAPI.BroadcastEvent(cniNode, utils.CNINodeDeleteFailed,
			"CNINode delete failed, will be retried", v1.EventTypeWarning)
		return ctrl.Result{}, err
	}

	currentNode := &v1.Node{}
	if err := r.reader().Get(ctx, req.NamespacedName, currentNode); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !currentNode.GetDeletionTimestamp().IsZero() {
		return ctrl.Result{}, fmt.Errorf("current Node %s is terminating", currentNode.Name)
	}
	if currentNode.UID != checkpointNode.UID {
		currentNode, checkpoint, err = r.ensureReplacementCheckpoint(
			ctx, currentNode, checkpoint.Spec, checkpoint.SourceCNINodeUID)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	r.log.Info("creating CNINode after it has been deleted as node still exists", "cniNode", cniNode.Name)
	recreateCNINodeCallCount.Inc()
	if err := r.createOrPatchCNINodeSuccessor(ctx, currentNode, checkpoint.Spec); err != nil {
		recreateCNINodeErrCount.Inc()
		utils.SendNodeEventWithNodeName(r.k8sAPI, currentNode.Name, utils.CNINodeCreateFailed,
			"CNINode was deleted and failed to be recreated by the vpc-resource-controller",
			v1.EventTypeWarning, r.log)
		return ctrl.Result{}, err
	}
	if err := r.clearReplacementCheckpoint(
		ctx, currentNode.Name, currentNode.UID, checkpoint.SourceCNINodeUID); err != nil {
		return ctrl.Result{}, err
	}
	r.log.Info("successfully recreated CNINode", "cniNode", cniNode.Name)
	return ctrl.Result{}, nil
}

func (r *CNINodeReconciler) ensureReplacementCheckpoint(ctx context.Context, node *v1.Node,
	spec v1alpha1.CNINodeSpec, sourceCNINodeUID types.UID,
) (*v1.Node, *cninodepkg.ReplacementCheckpoint, error) {
	checkpoint, err := cninodepkg.EnsureCheckpointRecord(
		ctx, r.Client, r.reader(), node.Name, node.UID, sourceCNINodeUID, spec)
	if err != nil {
		return nil, nil, err
	}
	updated := node.DeepCopy()
	if err := cninodepkg.SetCheckpoint(updated, spec, sourceCNINodeUID); err != nil {
		return nil, nil, err
	}
	if node.Annotations[cninodepkg.ReplacementCheckpointAnnotation] ==
		updated.Annotations[cninodepkg.ReplacementCheckpointAnnotation] {
		return node, checkpoint, nil
	}
	if err := r.Client.Patch(ctx, updated,
		client.MergeFromWithOptions(node, client.MergeFromWithOptimisticLock{})); err != nil {
		return nil, nil, err
	}
	return updated, checkpoint, nil
}

func (r *CNINodeReconciler) clearReplacementCheckpoint(ctx context.Context,
	nodeName string, expectedUID, sourceCNINodeUID types.UID,
) error {
	node := &v1.Node{}
	if err := r.reader().Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		return client.IgnoreNotFound(err)
	}
	if node.UID != expectedUID {
		return fmt.Errorf("Node UID changed from %s to %s while creating replacement CNINode",
			expectedUID, node.UID)
	}
	checkpoint, found, err := cninodepkg.Checkpoint(node)
	if err != nil || !found {
		return err
	}
	if checkpoint.SourceCNINodeUID != sourceCNINodeUID {
		return nil
	}
	if err := cninodepkg.DeleteCheckpointRecord(
		ctx, r.Client, r.reader(), nodeName, sourceCNINodeUID); err != nil {
		return err
	}
	updated := node.DeepCopy()
	cninodepkg.ClearCheckpoint(updated)
	return r.Client.Patch(ctx, updated,
		client.MergeFromWithOptions(node, client.MergeFromWithOptimisticLock{}))
}

func (r *CNINodeReconciler) replacementCNINode(node *v1.Node,
	spec v1alpha1.CNINodeSpec,
) *v1alpha1.CNINode {
	replacement := cninodepkg.NewSuccessor(node, spec)
	if replacement.Spec.Tags == nil {
		replacement.Spec.Tags = make(map[string]string)
	}
	replacement.Spec.Tags[config.VPCCNIClusterNameKey] = r.clusterName
	return replacement
}

// SetupWithManager sets up the controller with the Manager.
func (r *CNINodeReconciler) SetupWithManager(mgr ctrl.Manager, maxNodeConcurrentReconciles int) error {
	r.apiReader = mgr.GetAPIReader()
	if !prometheusRegistered {
		prometheusRegister()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.CNINode{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxNodeConcurrentReconciles}).
		Complete(r)
}

// waitTillCNINodeDeleted waits for CNINode to be deleted with timeout and returns error
func (r *CNINodeReconciler) waitTillCNINodeDeleted(nameSpacedCNINode types.NamespacedName) error {
	oldCNINode := &v1alpha1.CNINode{}

	return wait.PollUntilContextTimeout(context.TODO(), 500*time.Millisecond, time.Second*3, true, func(ctx context.Context) (bool, error) {
		if err := r.Client.Get(ctx, nameSpacedCNINode, oldCNINode); err != nil && errors.IsNotFound(err) {
			return true, nil
		}
		return false, nil
	})
}

// createOrPatchCNINodeSuccessor creates the current-generation CNINode with
// backoff. A same-generation object created concurrently already satisfies the
// transition and is left intact so its current spec and status cannot be lost.
func (r *CNINodeReconciler) createOrPatchCNINodeSuccessor(ctx context.Context,
	expectedNode *v1.Node, spec v1alpha1.CNINodeSpec,
) error {
	return retry.OnError(retry.DefaultBackoff, func(error) bool { return true },
		func() error {
			currentNode := &v1.Node{}
			if err := r.reader().Get(ctx, types.NamespacedName{Name: expectedNode.Name}, currentNode); err != nil {
				return err
			}
			if !currentNode.GetDeletionTimestamp().IsZero() {
				return fmt.Errorf("current Node %s is terminating", currentNode.Name)
			}
			if currentNode.UID != expectedNode.UID {
				return fmt.Errorf("Node UID changed from %s to %s while creating replacement CNINode",
					expectedNode.UID, currentNode.UID)
			}
			newCNINode := r.replacementCNINode(currentNode, spec)
			if err := r.Client.Create(ctx, newCNINode); err == nil {
				return nil
			} else if !errors.IsAlreadyExists(err) {
				return err
			}

			existing := &v1alpha1.CNINode{}
			if err := r.Client.Get(ctx, client.ObjectKeyFromObject(newCNINode), existing); err != nil {
				return err
			}
			if !existing.IsManagedByVPCResourceController() {
				return fmt.Errorf("replacement CNINode %s is managed by %s", existing.Name, existing.Spec.ManagedBy)
			}
			if !cninodepkg.OwnedByNode(existing, currentNode) {
				return fmt.Errorf("replacement CNINode %s is not owned by current Node UID %s",
					existing.Name, currentNode.UID)
			}
			return nil
		})
}

func (r *CNINodeReconciler) reader() client.Reader {
	if r.apiReader != nil {
		return r.apiReader
	}
	return r.Client
}
