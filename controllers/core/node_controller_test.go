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
	"fmt"
	"testing"
	"time"

	"github.com/aws/amazon-vpc-resource-controller-k8s/apis/vpcresources/v1alpha1"
	mock_condition "github.com/aws/amazon-vpc-resource-controller-k8s/mocks/amazon-vcp-resource-controller-k8s/pkg/condition"
	mock_node "github.com/aws/amazon-vpc-resource-controller-k8s/mocks/amazon-vcp-resource-controller-k8s/pkg/node"
	mock_manager "github.com/aws/amazon-vpc-resource-controller-k8s/mocks/amazon-vcp-resource-controller-k8s/pkg/node/manager"
	cninodepkg "github.com/aws/amazon-vpc-resource-controller-k8s/pkg/cninode"
	nodepkg "github.com/aws/amazon-vpc-resource-controller-k8s/pkg/node"
	nodemanager "github.com/aws/amazon-vpc-resource-controller-k8s/pkg/node/manager"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeClient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	controllermetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var (
	reconcileRequest = reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name: mockNodeName,
		},
	}
	mockNodeObj = &corev1.Node{
		ObjectMeta: v1.ObjectMeta{
			Name: mockNodeName,
		},
	}
)

type NodeMock struct {
	Conditions *mock_condition.MockConditions
	Manager    *mock_manager.MockManager
	MockNode   *mock_node.MockNode
	Reconciler NodeReconciler
}

func NewNodeMock(ctrl *gomock.Controller, mockObjects ...client.Object) NodeMock {
	mockManager := mock_manager.NewMockManager(ctrl)
	mockConditions := mock_condition.NewMockConditions(ctrl)
	mockNode := mock_node.NewMockNode(ctrl)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
	client := fakeClient.NewClientBuilder().WithScheme(scheme).WithObjects(mockObjects...).Build()

	return NodeMock{
		Conditions: mockConditions,
		Manager:    mockManager,
		MockNode:   mockNode,
		Reconciler: NodeReconciler{
			Scheme:     scheme,
			Client:     client,
			Log:        zap.New(),
			Manager:    mockManager,
			Conditions: mockConditions,
		},
	}
}

func TestNodeReconciler_Reconcile_AddNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewNodeMock(ctrl, mockNodeObj)

	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	mock.Manager.EXPECT().GetNode(mockNodeName).Return(mock.MockNode, false).Times(1)
	mock.Manager.EXPECT().AddNode(mockNodeName).Return(nil)
	mock.Manager.EXPECT().CheckNodeForLeakedENIs(mockNodeName).Times(0)

	res, err := mock.Reconciler.Reconcile(context.TODO(), reconcileRequest)
	assert.NoError(t, err)
	assert.Equal(t, res, reconcile.Result{})
}

func TestNodeReconciler_Reconcile_UpdateNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currentNode := mockNodeObj.DeepCopy()
	currentNode.Spec.ProviderID = "aws:///us-west-2c/i-current"
	mock := NewNodeMock(ctrl, currentNode)

	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	mock.Manager.EXPECT().GetNode(mockNodeName).Return(mock.MockNode, true).Times(1)
	mock.MockNode.EXPECT().GetNodeInstanceID().Return("i-current")
	mock.MockNode.EXPECT().IsReady().Return(true)
	mock.Manager.EXPECT().UpdateNode(mockNodeName).Return(nil)
	mock.Manager.EXPECT().CheckNodeForLeakedENIs(mockNodeName).Times(1)

	mismatchBefore := prometheusCounterValue(t, "node_instance_id_mismatch_total")
	res, err := mock.Reconciler.Reconcile(context.TODO(), reconcileRequest)
	assert.NoError(t, err)
	assert.Equal(t, res, reconcile.Result{})
	assert.Equal(t, mismatchBefore, prometheusCounterValue(t, "node_instance_id_mismatch_total"))
}

func TestNodeReconciler_Reconcile_WaitsForNodeInitialization(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currentNode := mockNodeObj.DeepCopy()
	currentNode.Spec.ProviderID = "aws:///us-west-2c/i-current"
	mock := NewNodeMock(ctrl, currentNode)

	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	mock.Manager.EXPECT().GetNode(mockNodeName).Return(mock.MockNode, true)
	mock.MockNode.EXPECT().GetNodeInstanceID().Return("i-current")
	mock.MockNode.EXPECT().IsReady().Return(false)

	res, err := mock.Reconciler.Reconcile(context.TODO(), reconcileRequest)

	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, res)
}

func TestNodeReconciler_Reconcile_UpdateNode_InstanceIDMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currentNode := mockNodeObj.DeepCopy()
	currentNode.Spec.ProviderID = "aws:///us-west-2c/i-current"
	mock := NewNodeMock(ctrl, currentNode)

	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	mock.Manager.EXPECT().GetNode(mockNodeName).Return(mock.MockNode, true).Times(1)
	mock.MockNode.EXPECT().GetNodeInstanceID().Return("i-cached")
	mock.Manager.EXPECT().DeleteNode(mockNodeName).Return(nil)

	mismatchBefore := prometheusCounterValue(t, "node_instance_id_mismatch_total")
	res, err := mock.Reconciler.Reconcile(context.TODO(), reconcileRequest)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, res)
	assert.Equal(t, mismatchBefore+1, prometheusCounterValue(t, "node_instance_id_mismatch_total"))
}

func TestNodeReconciler_Reconcile_AddNodeWaitsForCleanup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewNodeMock(ctrl, mockNodeObj)

	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	mock.Manager.EXPECT().GetNode(mockNodeName).Return(nil, false)
	mock.Manager.EXPECT().AddNode(mockNodeName).Return(nodemanager.ErrNodeCleanupInProgress)

	res, err := mock.Reconciler.Reconcile(context.TODO(), reconcileRequest)

	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, res)
}

func TestNodeReconciler_Reconcile_DeletesStaleCNINodeOwner(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currentNode := mockNodeObj.DeepCopy()
	currentNode.UID = types.UID("current-node-uid")
	staleCNINode := &v1alpha1.CNINode{
		ObjectMeta: v1.ObjectMeta{
			Name: mockNodeName,
			UID:  types.UID("stale-cninode-uid"),
			OwnerReferences: []v1.OwnerReference{{
				APIVersion: "v1",
				Kind:       "Node",
				Name:       mockNodeName,
				UID:        types.UID("old-node-uid"),
			}},
		},
		Spec: v1alpha1.CNINodeSpec{
			Features: []v1alpha1.Feature{{Name: v1alpha1.SecurityGroupsForPods}},
		},
	}
	mock := NewNodeMock(ctrl, currentNode, staleCNINode)
	var deletePreconditionUID *types.UID
	var deletePreconditionResourceVersion *string
	mock.Reconciler.Client = interceptor.NewClient(
		mock.Reconciler.Client.(client.WithWatch),
		interceptor.Funcs{
			Delete: func(ctx context.Context, delegate client.WithWatch, obj client.Object,
				opts ...client.DeleteOption,
			) error {
				options := (&client.DeleteOptions{}).ApplyOptions(opts)
				if options.Preconditions != nil {
					deletePreconditionUID = options.Preconditions.UID
					deletePreconditionResourceVersion = options.Preconditions.ResourceVersion
				}
				return delegate.Delete(ctx, obj, opts...)
			},
		})

	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)

	res, err := mock.Reconciler.Reconcile(context.TODO(), reconcileRequest)

	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, res)
	assert.NotNil(t, deletePreconditionUID)
	assert.Equal(t, staleCNINode.UID, *deletePreconditionUID)
	assert.NotNil(t, deletePreconditionResourceVersion)
	assert.NotEmpty(t, *deletePreconditionResourceVersion)
	deleted := &v1alpha1.CNINode{}
	getErr := mock.Reconciler.Client.Get(context.TODO(), reconcileRequest.NamespacedName, deleted)
	assert.True(t, errors.IsNotFound(getErr))
	persistedNode := &corev1.Node{}
	assert.NoError(t, mock.Reconciler.Client.Get(context.TODO(), reconcileRequest.NamespacedName, persistedNode))
	checkpoint, found, err := cninodepkg.Checkpoint(persistedNode)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, staleCNINode.Spec, checkpoint.Spec)
	assert.Equal(t, staleCNINode.UID, checkpoint.SourceCNINodeUID)

	restartedReconciler := mock.Reconciler
	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	res, err = restartedReconciler.Reconcile(context.TODO(), reconcileRequest)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, res)
	successor := &v1alpha1.CNINode{}
	assert.NoError(t, mock.Reconciler.Client.Get(context.TODO(), reconcileRequest.NamespacedName, successor))
	assert.True(t, cninodepkg.OwnedByNode(successor, currentNode))
	assert.Equal(t, staleCNINode.Spec.Features, successor.Spec.Features)
	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	mock.Manager.EXPECT().GetNode(mockNodeName).Return(nil, false)
	mock.Manager.EXPECT().AddNode(mockNodeName).Return(nil)

	res, err = restartedReconciler.Reconcile(context.TODO(), reconcileRequest)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)
	assert.NoError(t, mock.Reconciler.Client.Get(context.TODO(), reconcileRequest.NamespacedName, persistedNode))
	_, found, err = cninodepkg.Checkpoint(persistedNode)
	assert.NoError(t, err)
	assert.False(t, found)
}

func TestNodeReconciler_Reconcile_WaitsForDeletingCNINode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currentNode := mockNodeObj.DeepCopy()
	currentNode.UID = types.UID("current-node-uid")
	deletingCNINode := &v1alpha1.CNINode{
		ObjectMeta: v1.ObjectMeta{
			Name:              mockNodeName,
			Finalizers:        []string{NodeTerminationFinalizer},
			DeletionTimestamp: &v1.Time{Time: time.Now()},
		},
	}
	mock := NewNodeMock(ctrl, currentNode, deletingCNINode)

	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)

	res, err := mock.Reconciler.Reconcile(context.TODO(), reconcileRequest)

	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, res)
	persistedNode := &corev1.Node{}
	assert.NoError(t, mock.Reconciler.Client.Get(context.TODO(), reconcileRequest.NamespacedName, persistedNode))
	checkpoint, found, checkpointErr := cninodepkg.Checkpoint(persistedNode)
	assert.NoError(t, checkpointErr)
	assert.True(t, found)
	assert.Equal(t, currentNode.UID, checkpoint.NodeUID)
}

func TestNodeReconciler_Reconcile_TreatsOwnerlessCNINodeAsStale(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currentNode := mockNodeObj.DeepCopy()
	currentNode.UID = types.UID("current-node-uid")
	ownerless := &v1alpha1.CNINode{
		ObjectMeta: v1.ObjectMeta{Name: mockNodeName, UID: types.UID("cninode-uid")},
	}
	mock := NewNodeMock(ctrl, currentNode, ownerless)
	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)

	res, err := mock.Reconciler.Reconcile(context.Background(), reconcileRequest)

	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, res)
	actual := &v1alpha1.CNINode{}
	assert.True(t, errors.IsNotFound(
		mock.Reconciler.Client.Get(context.Background(), reconcileRequest.NamespacedName, actual)))
}

func TestNodeReconciler_Reconcile_UsesFreshCheckpointWhenCachedNodeIsStale(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cachedNode := mockNodeObj.DeepCopy()
	cachedNode.UID = types.UID("current-node-uid")
	freshNode := cachedNode.DeepCopy()
	assert.NoError(t, cninodepkg.SetCheckpoint(freshNode, v1alpha1.CNINodeSpec{
		Features: []v1alpha1.Feature{{Name: v1alpha1.SecurityGroupsForPods}},
	}))
	mock := NewNodeMock(ctrl, cachedNode)
	freshReader := fakeClient.NewClientBuilder().
		WithScheme(mock.Reconciler.Scheme).
		WithObjects(freshNode).
		Build()
	mock.Reconciler.APIReader = freshReader
	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)

	res, err := mock.Reconciler.Reconcile(context.Background(), reconcileRequest)

	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, res)
	successor := &v1alpha1.CNINode{}
	assert.NoError(t, mock.Reconciler.Client.Get(
		context.Background(), reconcileRequest.NamespacedName, successor))
	assert.True(t, cninodepkg.OwnedByNode(successor, freshNode))
	assert.Equal(t, freshNode.UID, successor.OwnerReferences[0].UID)
	assert.Equal(t, []v1alpha1.Feature{{Name: v1alpha1.SecurityGroupsForPods}},
		successor.Spec.Features)
}

func TestNodeReconciler_Reconcile_MovesCheckpointAcrossAnotherNodeUIDRollover(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currentNode := mockNodeObj.DeepCopy()
	currentNode.UID = types.UID("old-node-uid")
	assert.NoError(t, cninodepkg.SetCheckpoint(
		currentNode,
		v1alpha1.CNINodeSpec{Features: []v1alpha1.Feature{{Name: v1alpha1.SecurityGroupsForPods}}},
		types.UID("source-cninode-uid")))
	currentNode.UID = types.UID("current-node-uid")
	mock := NewNodeMock(ctrl, currentNode)
	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)

	res, err := mock.Reconciler.Reconcile(context.Background(), reconcileRequest)

	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, res)
	storedNode := &corev1.Node{}
	assert.NoError(t, mock.Reconciler.Client.Get(
		context.Background(), reconcileRequest.NamespacedName, storedNode))
	checkpoint, found, err := cninodepkg.Checkpoint(storedNode)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, currentNode.UID, checkpoint.NodeUID)
	assert.Equal(t, types.UID("source-cninode-uid"), checkpoint.SourceCNINodeUID)

	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	res, err = mock.Reconciler.Reconcile(context.Background(), reconcileRequest)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, res)
	successor := &v1alpha1.CNINode{}
	assert.NoError(t, mock.Reconciler.Client.Get(
		context.Background(), reconcileRequest.NamespacedName, successor))
	assert.True(t, cninodepkg.OwnedByNode(successor, currentNode))
}

func TestNodeReconciler_Reconcile_RecoversCheckpointAfterRealNodeRollover(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currentNode := mockNodeObj.DeepCopy()
	currentNode.UID = types.UID("node-c")
	mock := NewNodeMock(ctrl, currentNode)
	spec := v1alpha1.CNINodeSpec{
		Features: []v1alpha1.Feature{{Name: v1alpha1.SecurityGroupsForPods}},
	}
	_, err := cninodepkg.EnsureCheckpointRecord(
		context.Background(),
		mock.Reconciler.Client,
		mock.Reconciler.Client,
		currentNode.Name,
		types.UID("node-b"),
		types.UID("source-cninode"),
		spec)
	assert.NoError(t, err)

	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	res, err := mock.Reconciler.Reconcile(context.Background(), reconcileRequest)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, res)

	storedNode := &corev1.Node{}
	assert.NoError(t, mock.Reconciler.Client.Get(
		context.Background(), reconcileRequest.NamespacedName, storedNode))
	checkpoint, found, err := cninodepkg.Checkpoint(storedNode)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, currentNode.UID, checkpoint.NodeUID)
	assert.Equal(t, spec, checkpoint.Spec)

	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	res, err = mock.Reconciler.Reconcile(context.Background(), reconcileRequest)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, res)
	successor := &v1alpha1.CNINode{}
	assert.NoError(t, mock.Reconciler.Client.Get(
		context.Background(), reconcileRequest.NamespacedName, successor))
	assert.True(t, cninodepkg.OwnedByNode(successor, currentNode))
	assert.Equal(t, spec.Features, successor.Spec.Features)
}

func TestNodeReconciler_Reconcile_ReplacesCachedSameInstanceOldNodeUID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currentNode := mockNodeObj.DeepCopy()
	currentNode.UID = types.UID("node-c")
	currentNode.Spec.ProviderID = "aws:///us-west-2a/i-reused"
	currentCNINode := cninodepkg.NewSuccessor(currentNode, v1alpha1.CNINodeSpec{})
	cachedNode := nodepkg.NewManagedNode(
		zap.New(), currentNode.Name, "i-reused", "linux", nil, nil, types.UID("node-b"))
	mock := NewNodeMock(ctrl, currentNode, currentCNINode)
	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	mock.Manager.EXPECT().GetNode(currentNode.Name).Return(cachedNode, true)
	mock.Manager.EXPECT().DeleteNode(currentNode.Name).Return(nil)

	res, err := mock.Reconciler.Reconcile(context.Background(), reconcileRequest)

	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, res)
}

func TestNodeReconciler_Reconcile_ReReadsCheckpointAfterCNINodeDisappears(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	firstReadNode := mockNodeObj.DeepCopy()
	firstReadNode.UID = types.UID("current-node-uid")
	freshNode := firstReadNode.DeepCopy()
	assert.NoError(t, cninodepkg.SetCheckpoint(freshNode, v1alpha1.CNINodeSpec{
		Features: []v1alpha1.Feature{{Name: v1alpha1.SecurityGroupsForPods}},
	}))
	mock := NewNodeMock(ctrl, firstReadNode)
	freshClient := fakeClient.NewClientBuilder().
		WithScheme(mock.Reconciler.Scheme).
		WithObjects(freshNode).
		Build()
	nodeReads := 0
	mock.Reconciler.APIReader = interceptor.NewClient(freshClient, interceptor.Funcs{
		Get: func(ctx context.Context, delegate client.WithWatch, key client.ObjectKey,
			obj client.Object, opts ...client.GetOption,
		) error {
			if node, ok := obj.(*corev1.Node); ok {
				nodeReads++
				if nodeReads == 1 {
					*node = *firstReadNode.DeepCopy()
					return nil
				}
			}
			return delegate.Get(ctx, key, obj, opts...)
		},
	})
	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)

	res, err := mock.Reconciler.Reconcile(context.Background(), reconcileRequest)

	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, res)
	assert.Equal(t, 2, nodeReads)
	successor := &v1alpha1.CNINode{}
	assert.NoError(t, mock.Reconciler.Client.Get(
		context.Background(), reconcileRequest.NamespacedName, successor))
	assert.True(t, cninodepkg.OwnedByNode(successor, freshNode))
	assert.Equal(t, freshNode.UID, successor.OwnerReferences[0].UID)
	assert.Equal(t, []v1alpha1.Feature{{Name: v1alpha1.SecurityGroupsForPods}},
		successor.Spec.Features)
}

func TestNodeReconciler_Reconcile_RefreshesCheckpointBeforeDeletingStaleCNINode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currentNode := mockNodeObj.DeepCopy()
	currentNode.UID = types.UID("current-node-uid")
	assert.NoError(t, cninodepkg.SetCheckpoint(currentNode, v1alpha1.CNINodeSpec{
		Features: []v1alpha1.Feature{{Name: v1alpha1.SecurityGroupsForPods}},
	}))
	staleCNINode := &v1alpha1.CNINode{
		ObjectMeta: v1.ObjectMeta{
			Name: mockNodeName,
			UID:  types.UID("stale-cninode-uid"),
			OwnerReferences: []v1.OwnerReference{{
				APIVersion: "v1",
				Kind:       "Node",
				Name:       mockNodeName,
				UID:        types.UID("old-node-uid"),
			}},
		},
		Spec: v1alpha1.CNINodeSpec{
			Features: []v1alpha1.Feature{{
				Name:  v1alpha1.CustomNetworking,
				Value: "latest-eni-config",
			}},
		},
	}
	mock := NewNodeMock(ctrl, currentNode, staleCNINode)
	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)

	_, err := mock.Reconciler.Reconcile(context.Background(), reconcileRequest)

	assert.NoError(t, err)
	persistedNode := &corev1.Node{}
	assert.NoError(t, mock.Reconciler.Client.Get(
		context.Background(), reconcileRequest.NamespacedName, persistedNode))
	checkpoint, found, checkpointErr := cninodepkg.Checkpoint(persistedNode)
	assert.NoError(t, checkpointErr)
	assert.True(t, found)
	assert.Equal(t, staleCNINode.Spec, checkpoint.Spec)
}

func TestNodeReconciler_Reconcile_DoesNotInitializeTerminatingNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	terminatingNode := mockNodeObj.DeepCopy()
	terminatingNode.Finalizers = []string{"test"}
	terminatingNode.DeletionTimestamp = &v1.Time{Time: time.Now()}
	mock := NewNodeMock(ctrl, terminatingNode)
	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	mock.Manager.EXPECT().GetNode(mockNodeName).Return(mock.MockNode, true)
	mock.Manager.EXPECT().DeleteNode(mockNodeName).Return(nil)

	res, err := mock.Reconciler.Reconcile(context.Background(), reconcileRequest)

	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)
}

func TestNodeReconciler_Reconcile_DoesNotGateOnStaleStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currentNode := mockNodeObj.DeepCopy()
	currentNode.UID = types.UID("current-node-uid")
	currentNode.Spec.ProviderID = "aws:///us-west-2c/i-current"
	controller := true
	currentCNINode := &v1alpha1.CNINode{
		ObjectMeta: v1.ObjectMeta{
			Name: mockNodeName,
			OwnerReferences: []v1.OwnerReference{{
				APIVersion: "v1",
				Kind:       "Node",
				Name:       mockNodeName,
				UID:        currentNode.UID,
				Controller: &controller,
			}},
		},
		Status: v1alpha1.CNINodeStatus{
			NodeNetworkState: &v1alpha1.NodeNetworkState{InstanceID: "i-old"},
		},
	}
	mock := NewNodeMock(ctrl, currentNode, currentCNINode)

	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	mock.Manager.EXPECT().GetNode(mockNodeName).Return(nil, false)
	mock.Manager.EXPECT().AddNode(mockNodeName).Return(nil)

	res, err := mock.Reconciler.Reconcile(context.TODO(), reconcileRequest)

	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)
}

func TestNodeReconciler_Reconcile_DeleteNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewNodeMock(ctrl)

	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	mock.Manager.EXPECT().GetNode(mockNodeName).Return(mock.MockNode, true)
	mock.Manager.EXPECT().DeleteNode(mockNodeName).Return(nil)

	res, err := mock.Reconciler.Reconcile(context.TODO(), reconcileRequest)
	assert.NoError(t, err)
	assert.Equal(t, res, reconcile.Result{})
}

func TestNodeReconciler_Reconcile_DeleteNonExistentNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewNodeMock(ctrl)

	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	mock.Manager.EXPECT().GetNode(mockNodeName).Return(mock.MockNode, false)

	res, err := mock.Reconciler.Reconcile(context.TODO(), reconcileRequest)
	assert.NoError(t, err)
	assert.Equal(t, res, reconcile.Result{})
}

func TestNodeReconciler_Reconcile_DeleteNonExistentUnmanagedNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewNodeMock(ctrl)

	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	mock.Manager.EXPECT().GetNode(mockNodeName).Return(mock.MockNode, false)

	res, err := mock.Reconciler.Reconcile(context.TODO(), reconcileRequest)
	assert.NoError(t, err)
	assert.Equal(t, res, reconcile.Result{})
}

func TestNodeReconciler_Reconcile_DeleteNonExistentUnmanagedWithoutInstanceNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewNodeMock(ctrl)

	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	mock.Manager.EXPECT().GetNode(mockNodeName).Return(mock.MockNode, false)

	res, err := mock.Reconciler.Reconcile(context.TODO(), reconcileRequest)
	assert.NoError(t, err)
	assert.Equal(t, res, reconcile.Result{})
}

func TestNodeReconciler_Reconcile_AddNode_Internal_Server_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	internalServerError := errors.NewInternalError(fmt.Errorf("internal server error"))
	mock := NewNodeMock(ctrl, mockNodeObj)

	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	mock.Manager.EXPECT().GetNode(mockNodeName).Return(nil, false).Times(1)
	mock.Manager.EXPECT().AddNode(mockNodeName).Return(internalServerError)
	mock.Manager.EXPECT().CheckNodeForLeakedENIs(mockNodeName).Times(0)
	res, err := mock.Reconciler.Reconcile(context.TODO(), reconcileRequest)

	assert.Error(t, err, "We return error on internal server error to make sure it gets requeued by controller-runtime.")
	assert.False(t, res.Requeue)
}

func TestNodeReconciler_Reconcile_SkipAutoComputeType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	autoComputeNode := &corev1.Node{
		ObjectMeta: v1.ObjectMeta{
			Name: mockNodeName,
			Labels: map[string]string{
				computeTypeLabelKey: autoComputeTypeLabelValue,
			},
		},
	}

	mock := NewNodeMock(ctrl, autoComputeNode)

	mock.Conditions.EXPECT().GetPodDataStoreSyncStatus().Return(true)
	mock.Manager.EXPECT().AddNode(gomock.Any()).Times(0)
	mock.Manager.EXPECT().UpdateNode(gomock.Any()).Times(0)
	mock.Manager.EXPECT().GetNode(gomock.Any()).Times(0)
	mock.Manager.EXPECT().CheckNodeForLeakedENIs(gomock.Any()).Times(0)

	res, err := mock.Reconciler.Reconcile(context.TODO(), reconcileRequest)
	assert.NoError(t, err)
	assert.Equal(t, res, reconcile.Result{})
}

func prometheusCounterValue(t *testing.T, name string) float64 {
	t.Helper()
	metricFamilies, err := controllermetrics.Registry.Gather()
	assert.NoError(t, err)
	for _, metricFamily := range metricFamilies {
		if metricFamily.GetName() == name && len(metricFamily.Metric) == 1 {
			return metricFamily.Metric[0].GetCounter().GetValue()
		}
	}
	return 0
}
