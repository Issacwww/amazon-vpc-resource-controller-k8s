package crds

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/amazon-vpc-resource-controller-k8s/apis/vpcresources/v1alpha1"
	mock_api "github.com/aws/amazon-vpc-resource-controller-k8s/mocks/amazon-vcp-resource-controller-k8s/pkg/aws/ec2/api"
	mock_cleanup "github.com/aws/amazon-vpc-resource-controller-k8s/mocks/amazon-vcp-resource-controller-k8s/pkg/aws/ec2/api/cleanup"
	mock_k8s "github.com/aws/amazon-vpc-resource-controller-k8s/mocks/amazon-vcp-resource-controller-k8s/pkg/k8s"
	ec2API "github.com/aws/amazon-vpc-resource-controller-k8s/pkg/aws/ec2/api"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/aws/ec2/api/cleanup"
	cninodepkg "github.com/aws/amazon-vpc-resource-controller-k8s/pkg/cninode"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/config"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeClient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type CNINodeMock struct {
	Reconciler CNINodeReconciler
}

var (
	mockName          = "node-name"
	mockClusterName   = "test-cluster"
	mockNodeWithLabel = &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: mockName,
			UID:  types.UID("current-node-uid"),
			Labels: map[string]string{
				config.NodeLabelOS: "linux",
			},
		},
	}
	reconcileRequest = reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name: mockName,
		},
	}
)

func NewCNINodeMock(ctrl *gomock.Controller, mockObjects ...client.Object) *CNINodeMock {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
	client := fakeClient.NewClientBuilder().WithScheme(scheme).WithObjects(mockObjects...).Build()
	return &CNINodeMock{
		Reconciler: CNINodeReconciler{
			Client:      client,
			scheme:      scheme,
			log:         zap.New(),
			clusterName: mockClusterName,
			vpcId:       "vpc-000000000000",
		},
	}
}

func TestCNINodeReconcile(t *testing.T) {
	type args struct {
		mockNode    *corev1.Node
		mockCNINode *v1alpha1.CNINode
	}
	type fields struct {
		mockResourceCleaner  *mock_cleanup.MockResourceCleaner
		mockK8sApi           *mock_k8s.MockK8sWrapper
		mockFinalizerManager *mock_k8s.MockFinalizerManager
		mockEC2API           *mock_api.MockEC2APIHelper
		mockCNINode          *CNINodeMock
	}
	tests := []struct {
		name    string
		args    args
		prepare func(f *fields)
		asserts func(reconcile.Result, error, *v1alpha1.CNINode)
	}{
		{
			name: "verify clusterName tag and labels are added if missing",
			args: args{
				mockNode: mockNodeWithLabel,
				mockCNINode: &v1alpha1.CNINode{
					ObjectMeta: metav1.ObjectMeta{
						Name: mockName,
					},
				},
			},
			prepare: nil,
			asserts: func(res reconcile.Result, err error, cniNode *v1alpha1.CNINode) {
				assert.NoError(t, err)
				assert.Equal(t, res, reconcile.Result{})
				assert.Equal(t, cniNode.Labels, map[string]string{config.NodeLabelOS: "linux"})
				assert.Equal(t, cniNode.Spec.Tags, map[string]string{config.VPCCNIClusterNameKey: mockClusterName})
			},
		},
		{
			name: "verify cleaner was not called if node id is not present given that node is being finalized",
			args: args{
				mockNode: nil,
				mockCNINode: &v1alpha1.CNINode{
					ObjectMeta: metav1.ObjectMeta{
						Name: mockName,
						Labels: map[string]string{
							config.NodeLabelOS: config.OSLinux,
						},
						Finalizers:        []string{config.NodeTerminationFinalizer},
						DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
					},
				},
			},
			prepare: func(f *fields) {
				f.mockCNINode.Reconciler.newResourceCleaner = func(nodeID string, eC2Wrapper ec2API.EC2Wrapper, vpcID string) cleanup.ResourceCleaner {
					return f.mockResourceCleaner
				}
				f.mockResourceCleaner.EXPECT().DeleteLeakedResources().Times(0)

				f.mockFinalizerManager.EXPECT().
					RemoveFinalizers(gomock.Any(), gomock.Any(), config.NodeTerminationFinalizer).
					Return(nil)
			},
			asserts: func(res reconcile.Result, err error, cniNode *v1alpha1.CNINode) {
				assert.NoError(t, err)
				assert.Equal(t, res, reconcile.Result{})
			},
		},
		{
			name: "verify cleaner was called if node id is not empty when node is being finalized",
			args: args{
				mockNode: nil,
				mockCNINode: &v1alpha1.CNINode{
					ObjectMeta: metav1.ObjectMeta{
						Name: mockName,
						Labels: map[string]string{
							config.NodeLabelOS: config.OSLinux,
						},
						Finalizers:        []string{config.NodeTerminationFinalizer},
						DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
					},
					Spec: v1alpha1.CNINodeSpec{
						Tags: map[string]string{
							config.NetworkInterfaceNodeIDKey: "i-1234567890",
						},
					},
				},
			},
			prepare: func(f *fields) {
				f.mockCNINode.Reconciler.newResourceCleaner = func(nodeID string, eC2Wrapper ec2API.EC2Wrapper, vpcID string) cleanup.ResourceCleaner {
					assert.Equal(t, "i-1234567890", nodeID)
					return f.mockResourceCleaner
				}
				f.mockResourceCleaner.EXPECT().DeleteLeakedResources().Times(1).Return(nil)
				f.mockFinalizerManager.EXPECT().
					RemoveFinalizers(gomock.Any(), gomock.Any(), config.NodeTerminationFinalizer).
					Return(nil)

			},
			asserts: func(res reconcile.Result, err error, cniNode *v1alpha1.CNINode) {
				assert.NoError(t, err)
				assert.Equal(t, res, reconcile.Result{})
			},
		},
		{
			name: "terminating Node does not receive a replacement CNINode",
			args: args{
				mockNode: &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:              mockName,
						UID:               types.UID("terminating-node-uid"),
						Finalizers:        []string{"test"},
						DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
					},
				},
				mockCNINode: &v1alpha1.CNINode{
					ObjectMeta: metav1.ObjectMeta{
						Name:              mockName,
						Finalizers:        []string{config.NodeTerminationFinalizer},
						DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
					},
					Spec: v1alpha1.CNINodeSpec{
						Features: []v1alpha1.Feature{{Name: v1alpha1.SecurityGroupsForPods}},
					},
				},
			},
			prepare: func(f *fields) {
				f.mockFinalizerManager.EXPECT().
					RemoveFinalizers(gomock.Any(), gomock.Any(), config.NodeTerminationFinalizer).
					Return(nil)
			},
			asserts: func(res reconcile.Result, err error, cniNode *v1alpha1.CNINode) {
				assert.NoError(t, err)
				assert.Equal(t, reconcile.Result{}, res)
				assert.Empty(t, cniNode.OwnerReferences)
			},
		},
		{
			name: "replace deleting CNINode for the current Node generation",
			args: args{
				mockNode: mockNodeWithLabel,
				mockCNINode: &v1alpha1.CNINode{
					ObjectMeta: metav1.ObjectMeta{
						Name: mockName,
						UID:  types.UID("old-cninode-uid"),
						OwnerReferences: []metav1.OwnerReference{{
							APIVersion: "v1",
							Kind:       "Node",
							Name:       mockName,
							UID:        types.UID("old-node-uid"),
						}},
						Labels: map[string]string{
							config.NodeLabelOS: config.OSLinux,
						},
						Finalizers:        []string{config.NodeTerminationFinalizer},
						DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
					},
					Spec: v1alpha1.CNINodeSpec{
						Features: []v1alpha1.Feature{
							{Name: v1alpha1.SecurityGroupsForPods},
							{Name: v1alpha1.CustomNetworking, Value: "eni-config"},
						},
						Tags: map[string]string{
							config.NetworkInterfaceNodeIDKey: "i-0123456789abcdef0",
							"preserved":                      "value",
						},
					},
					Status: v1alpha1.CNINodeStatus{
						NodeNetworkState: &v1alpha1.NodeNetworkState{
							InstanceID: "i-0123456789abcdef0",
						},
						TrunkInterface: &v1alpha1.TrunkInterface{ID: "eni-old"},
					},
				},
			},
			prepare: func(f *fields) {
				f.mockFinalizerManager.EXPECT().
					RemoveFinalizers(gomock.Any(), gomock.Any(), config.NodeTerminationFinalizer).
					DoAndReturn(func(ctx context.Context, object client.Object, _ ...string) error {
						node := &corev1.Node{}
						assert.NoError(t, f.mockCNINode.Reconciler.Client.Get(ctx,
							types.NamespacedName{Name: mockName}, node))
						checkpoint, found, checkpointErr := cninodepkg.Checkpoint(node)
						assert.NoError(t, checkpointErr)
						assert.True(t, found)
						assert.Equal(t, object.(*v1alpha1.CNINode).Spec, checkpoint.Spec)
						object.SetFinalizers(nil)
						if err := f.mockCNINode.Reconciler.Client.Update(ctx, object); err != nil &&
							!k8serrors.IsNotFound(err) {
							return err
						}
						err := f.mockCNINode.Reconciler.Client.Delete(ctx, object)
						return client.IgnoreNotFound(err)
					})
			},
			asserts: func(res reconcile.Result, err error, cniNode *v1alpha1.CNINode) {
				assert.NoError(t, err)
				assert.Equal(t, reconcile.Result{}, res)
				assert.Len(t, cniNode.OwnerReferences, 1)
				assert.Equal(t, mockNodeWithLabel.UID, cniNode.OwnerReferences[0].UID)
				assert.Equal(t, []v1alpha1.Feature{
					{Name: v1alpha1.SecurityGroupsForPods},
					{Name: v1alpha1.CustomNetworking, Value: "eni-config"},
				}, cniNode.Spec.Features)
				assert.Equal(t, "value", cniNode.Spec.Tags["preserved"])
				assert.Equal(t, mockClusterName, cniNode.Spec.Tags[config.VPCCNIClusterNameKey])
				assert.NotContains(t, cniNode.Spec.Tags, config.NetworkInterfaceNodeIDKey)
				assert.Nil(t, cniNode.Status.NodeNetworkState)
				assert.Nil(t, cniNode.Status.TrunkInterface)
			},
		},
		{
			name: "verify CNINode managed by another controller is skipped entirely",
			args: args{
				mockNode: mockNodeWithLabel,
				mockCNINode: &v1alpha1.CNINode{
					ObjectMeta: metav1.ObjectMeta{
						Name: mockName,
					},
					Spec: v1alpha1.CNINodeSpec{
						ManagedBy: v1alpha1.ManagedByEKSAutoMode,
					},
				},
			},
			prepare: nil,
			asserts: func(res reconcile.Result, err error, cniNode *v1alpha1.CNINode) {
				assert.NoError(t, err)
				assert.Equal(t, res, reconcile.Result{})
				// no tags, labels, or finalizer added by this controller
				assert.Empty(t, cniNode.Labels)
				assert.Empty(t, cniNode.Spec.Tags)
				assert.NotContains(t, cniNode.Finalizers, config.NodeTerminationFinalizer)
			},
		},
		{
			name: "verify empty managedBy is treated as vpc-resource-controller (backward compatibility)",
			args: args{
				mockNode: mockNodeWithLabel,
				mockCNINode: &v1alpha1.CNINode{
					ObjectMeta: metav1.ObjectMeta{
						Name: mockName,
						Labels: map[string]string{
							config.NodeLabelOS: "linux",
						},
					},
					Spec: v1alpha1.CNINodeSpec{
						Tags: map[string]string{
							config.VPCCNIClusterNameKey: mockClusterName,
						},
					},
				},
			},
			// This branch adds finalizers through the finalizer manager (master uses
			// controllerutil.AddFinalizer + patch); expecting the call proves the object
			// was reconciled as self-owned rather than skipped.
			prepare: func(f *fields) {
				f.mockFinalizerManager.EXPECT().
					AddFinalizers(gomock.Any(), gomock.Any(), config.NodeTerminationFinalizer).
					Return(nil)
			},
			asserts: func(res reconcile.Result, err error, cniNode *v1alpha1.CNINode) {
				assert.NoError(t, err)
				assert.Equal(t, res, reconcile.Result{})
			},
		},
		{
			name: "verify finalizer is added when labels and tags are present",
			args: args{
				mockNode: mockNodeWithLabel,
				mockCNINode: &v1alpha1.CNINode{
					ObjectMeta: metav1.ObjectMeta{
						Name: mockName,
						Labels: map[string]string{
							config.NodeLabelOS: "linux",
						},
					},
					Spec: v1alpha1.CNINodeSpec{
						Tags: map[string]string{
							config.VPCCNIClusterNameKey: mockClusterName,
						},
					},
				},
			},
			prepare: func(f *fields) {
				f.mockFinalizerManager.EXPECT().
					AddFinalizers(gomock.Any(), gomock.Any(), config.NodeTerminationFinalizer).
					Return(nil)
			},
			asserts: func(res reconcile.Result, err error, cniNode *v1alpha1.CNINode) {
				assert.NoError(t, err)
				assert.Equal(t, res, reconcile.Result{})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			objs := []client.Object{tt.args.mockCNINode}
			if tt.args.mockNode != nil {
				objs = append(objs, tt.args.mockNode)
			}
			mock := NewCNINodeMock(ctrl, objs...)
			f := fields{
				mockResourceCleaner:  mock_cleanup.NewMockResourceCleaner(ctrl),
				mockK8sApi:           mock_k8s.NewMockK8sWrapper(ctrl),
				mockFinalizerManager: mock_k8s.NewMockFinalizerManager(ctrl),
				mockEC2API:           mock_api.NewMockEC2APIHelper(ctrl),
				mockCNINode:          mock,
			}
			mock.Reconciler.finalizerManager = f.mockFinalizerManager
			mock.Reconciler.k8sAPI = f.mockK8sApi
			if tt.prepare != nil {
				tt.prepare(&f)
			}
			res, err := mock.Reconciler.Reconcile(context.Background(), reconcileRequest)

			cniNode := &v1alpha1.CNINode{}
			getErr := mock.Reconciler.Client.Get(context.Background(), reconcileRequest.NamespacedName, cniNode)
			assert.NoError(t, getErr)

			if tt.asserts != nil {
				tt.asserts(res, err, cniNode)
			}
		})
	}
}

func TestCreateOrPatchCNINodeSuccessorPreservesExistingCurrentGeneration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currentNode := mockNodeWithLabel.DeepCopy()
	existing := &v1alpha1.CNINode{
		ObjectMeta: metav1.ObjectMeta{
			Name: mockName,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(
				currentNode, corev1.SchemeGroupVersion.WithKind("Node"))},
		},
		Spec: v1alpha1.CNINodeSpec{
			Features: []v1alpha1.Feature{{Name: v1alpha1.CustomNetworking, Value: "new-config"}},
		},
		Status: v1alpha1.CNINodeStatus{
			NodeNetworkState: &v1alpha1.NodeNetworkState{InstanceID: "i-current"},
		},
	}
	mock := NewCNINodeMock(ctrl, currentNode, existing)
	checkpointSpec := v1alpha1.CNINodeSpec{
		Features: []v1alpha1.Feature{{Name: v1alpha1.SecurityGroupsForPods}},
	}

	err := mock.Reconciler.createOrPatchCNINodeSuccessor(
		context.Background(), currentNode, checkpointSpec)

	assert.NoError(t, err)
	actual := &v1alpha1.CNINode{}
	assert.NoError(t, mock.Reconciler.Client.Get(context.Background(), reconcileRequest.NamespacedName, actual))
	assert.Len(t, actual.OwnerReferences, 1)
	assert.Equal(t, currentNode.UID, actual.OwnerReferences[0].UID)
	assert.Equal(t, existing.Spec, actual.Spec)
	assert.Equal(t, existing.Status, actual.Status)
}

func TestEnsureReplacementCheckpointRefreshesDesiredSpec(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currentNode := mockNodeWithLabel.DeepCopy()
	assert.NoError(t, cninodepkg.SetCheckpoint(currentNode, v1alpha1.CNINodeSpec{
		Features: []v1alpha1.Feature{{Name: v1alpha1.SecurityGroupsForPods}},
	}))
	mock := NewCNINodeMock(ctrl, currentNode)
	latestSpec := v1alpha1.CNINodeSpec{
		Features: []v1alpha1.Feature{{Name: v1alpha1.CustomNetworking, Value: "latest-config"}},
	}

	updatedNode, checkpoint, err := mock.Reconciler.ensureReplacementCheckpoint(
		context.Background(), currentNode, latestSpec, types.UID("source-cninode"))

	assert.NoError(t, err)
	assert.Equal(t, currentNode.UID, checkpoint.NodeUID)
	assert.Equal(t, types.UID("source-cninode"), checkpoint.SourceCNINodeUID)
	assert.Equal(t, latestSpec, checkpoint.Spec)
	storedNode := &corev1.Node{}
	assert.NoError(t, mock.Reconciler.Client.Get(
		context.Background(), client.ObjectKeyFromObject(currentNode), storedNode))
	storedCheckpoint, found, err := cninodepkg.Checkpoint(storedNode)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, latestSpec, storedCheckpoint.Spec)
	assert.Equal(t, updatedNode.Annotations, storedNode.Annotations)
}

func TestClearReplacementCheckpointDoesNotClearNewerTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currentNode := mockNodeWithLabel.DeepCopy()
	assert.NoError(t, cninodepkg.SetCheckpoint(
		currentNode, v1alpha1.CNINodeSpec{}, types.UID("new-source-cninode")))
	mock := NewCNINodeMock(ctrl, currentNode)

	err := mock.Reconciler.clearReplacementCheckpoint(
		context.Background(), currentNode.Name, currentNode.UID, types.UID("old-source-cninode"))

	assert.NoError(t, err)
	storedNode := &corev1.Node{}
	assert.NoError(t, mock.Reconciler.Client.Get(
		context.Background(), client.ObjectKeyFromObject(currentNode), storedNode))
	checkpoint, found, err := cninodepkg.Checkpoint(storedNode)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, types.UID("new-source-cninode"), checkpoint.SourceCNINodeUID)
}

func TestCreateOrPatchCNINodeSuccessorRejectsDifferentGeneration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currentNode := mockNodeWithLabel.DeepCopy()
	existing := &v1alpha1.CNINode{
		ObjectMeta: metav1.ObjectMeta{
			Name: mockName,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1",
				Kind:       "Node",
				Name:       mockName,
				UID:        types.UID("different-node-uid"),
			}},
		},
	}
	mock := NewCNINodeMock(ctrl, currentNode, existing)

	err := mock.Reconciler.createOrPatchCNINodeSuccessor(
		context.Background(), currentNode, v1alpha1.CNINodeSpec{})

	assert.Error(t, err)
	actual := &v1alpha1.CNINode{}
	assert.NoError(t, mock.Reconciler.Client.Get(context.Background(), reconcileRequest.NamespacedName, actual))
	assert.Equal(t, existing.OwnerReferences, actual.OwnerReferences)
}

func TestCreateOrPatchCNINodeSuccessorReturnsCreateFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currentNode := mockNodeWithLabel.DeepCopy()
	mock := NewCNINodeMock(ctrl, currentNode)
	mock.Reconciler.Client = interceptor.NewClient(
		mock.Reconciler.Client.(client.WithWatch),
		interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, obj client.Object,
				_ ...client.CreateOption,
			) error {
				if _, ok := obj.(*v1alpha1.CNINode); ok {
					return fmt.Errorf("create failed")
				}
				return nil
			},
		})

	err := mock.Reconciler.createOrPatchCNINodeSuccessor(
		context.Background(), currentNode, v1alpha1.CNINodeSpec{})

	assert.EqualError(t, err, "create failed")
}
