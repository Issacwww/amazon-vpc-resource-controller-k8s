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

package cninode

import (
	"context"
	"testing"

	"github.com/aws/amazon-vpc-resource-controller-k8s/apis/vpcresources/v1alpha1"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/config"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReplacementCheckpointAndSuccessor(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node",
			UID:    types.UID("node-uid"),
			Labels: map[string]string{config.NodeLabelOS: config.OSLinux},
		},
	}
	spec := v1alpha1.CNINodeSpec{
		Features: []v1alpha1.Feature{{Name: v1alpha1.SecurityGroupsForPods}},
		Tags: map[string]string{
			config.NetworkInterfaceNodeIDKey: "i-old",
			"preserved":                      "value",
		},
	}

	sourceCNINodeUID := types.UID("source-cninode-uid")
	assert.NoError(t, SetCheckpoint(node, spec, sourceCNINodeUID))
	checkpoint, found, err := Checkpoint(node)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, node.UID, checkpoint.NodeUID)
	assert.Equal(t, sourceCNINodeUID, checkpoint.SourceCNINodeUID)
	assert.Equal(t, spec, checkpoint.Spec)

	successor := NewSuccessor(node, checkpoint.Spec)
	assert.True(t, OwnedByNode(successor, node))
	assert.Equal(t, spec.Features, successor.Spec.Features)
	assert.Equal(t, "value", successor.Spec.Tags["preserved"])
	assert.NotContains(t, successor.Spec.Tags, config.NetworkInterfaceNodeIDKey)
	assert.Empty(t, successor.Status)

	ClearCheckpoint(node)
	_, found, err = Checkpoint(node)
	assert.NoError(t, err)
	assert.False(t, found)
}

func TestCheckpointRejectsMalformedValue(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Annotations: map[string]string{ReplacementCheckpointAnnotation: "{"},
	}}

	_, found, err := Checkpoint(node)

	assert.True(t, found)
	assert.Error(t, err)
}

func TestOwnedByNodeRequiresControllingOwner(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "node",
		UID:  types.UID("node-uid"),
	}}
	cniNode := &v1alpha1.CNINode{ObjectMeta: metav1.ObjectMeta{
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       node.Name,
			UID:        node.UID,
		}},
	}}

	assert.False(t, OwnedByNode(cniNode, node))
}

func TestCheckpointRecordSurvivesNodeGenerationRollover(t *testing.T) {
	scheme := runtime.NewScheme()
	assert.NoError(t, v1alpha1.AddToScheme(scheme))
	k8sClient := fakeclient.NewClientBuilder().WithScheme(scheme).Build()
	spec := v1alpha1.CNINodeSpec{
		Features: []v1alpha1.Feature{{Name: v1alpha1.SecurityGroupsForPods}},
	}

	checkpoint, err := EnsureCheckpointRecord(
		context.Background(), k8sClient, k8sClient, "node", "node-b", "source-cni", spec)
	assert.NoError(t, err)
	assert.Equal(t, types.UID("node-b"), checkpoint.NodeUID)

	checkpoint, found, err := GetCheckpointRecord(context.Background(), k8sClient, "node")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, spec, checkpoint.Spec)

	checkpoint, err = EnsureCheckpointRecord(
		context.Background(), k8sClient, k8sClient, "node", "node-c", "source-cni", spec)
	assert.NoError(t, err)
	assert.Equal(t, types.UID("node-c"), checkpoint.NodeUID)

	assert.NoError(t, DeleteCheckpointRecord(
		context.Background(), k8sClient, k8sClient, "node", "older-source"))
	_, found, err = GetCheckpointRecord(context.Background(), k8sClient, "node")
	assert.NoError(t, err)
	assert.True(t, found)

	assert.NoError(t, DeleteCheckpointRecord(
		context.Background(), k8sClient, k8sClient, "node", "source-cni"))
	_, found, err = GetCheckpointRecord(context.Background(), k8sClient, "node")
	assert.NoError(t, err)
	assert.False(t, found)
}
