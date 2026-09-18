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
	"encoding/json"
	"fmt"

	"github.com/aws/amazon-vpc-resource-controller-k8s/apis/vpcresources/v1alpha1"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/config"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const ReplacementCheckpointAnnotation = "vpcresources.k8s.aws/cninode-replacement"

type ReplacementCheckpoint struct {
	NodeUID          types.UID            `json:"nodeUID"`
	SourceCNINodeUID types.UID            `json:"sourceCNINodeUID,omitempty"`
	Spec             v1alpha1.CNINodeSpec `json:"spec"`
}

func Checkpoint(node *corev1.Node) (*ReplacementCheckpoint, bool, error) {
	value, found := node.Annotations[ReplacementCheckpointAnnotation]
	if !found {
		return nil, false, nil
	}
	checkpoint, err := decodeCheckpoint(value)
	return checkpoint, true, err
}

func decodeCheckpoint(value string) (*ReplacementCheckpoint, error) {
	checkpoint := &ReplacementCheckpoint{}
	if err := json.Unmarshal([]byte(value), checkpoint); err != nil {
		return nil, fmt.Errorf("decode CNINode replacement checkpoint: %w", err)
	}
	if checkpoint.NodeUID == "" {
		return nil, fmt.Errorf("CNINode replacement checkpoint has no Node UID")
	}
	return checkpoint, nil
}

func encodeCheckpoint(checkpoint *ReplacementCheckpoint) (string, error) {
	value, err := json.Marshal(checkpoint)
	if err != nil {
		return "", fmt.Errorf("encode CNINode replacement checkpoint: %w", err)
	}
	return string(value), nil
}

func SetCheckpoint(node *corev1.Node, spec v1alpha1.CNINodeSpec, sourceCNINodeUID ...types.UID) error {
	var sourceUID types.UID
	if len(sourceCNINodeUID) > 0 {
		sourceUID = sourceCNINodeUID[0]
	}
	checkpoint := ReplacementCheckpoint{
		NodeUID:          node.UID,
		SourceCNINodeUID: sourceUID,
		Spec:             *spec.DeepCopy(),
	}
	value, err := encodeCheckpoint(&checkpoint)
	if err != nil {
		return err
	}
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	node.Annotations[ReplacementCheckpointAnnotation] = value
	return nil
}

func ClearCheckpoint(node *corev1.Node) {
	delete(node.Annotations, ReplacementCheckpointAnnotation)
}

func OwnedByNode(cniNode *v1alpha1.CNINode, node *corev1.Node) bool {
	owner := metav1.GetControllerOf(cniNode)
	return owner != nil &&
		owner.APIVersion == corev1.SchemeGroupVersion.String() &&
		owner.Kind == "Node" && owner.Name == node.Name &&
		owner.UID != "" && owner.UID == node.UID
}

func NewSuccessor(node *corev1.Node, spec v1alpha1.CNINodeSpec) *v1alpha1.CNINode {
	successorSpec := *spec.DeepCopy()
	if successorSpec.Tags == nil {
		successorSpec.Tags = make(map[string]string)
	}
	delete(successorSpec.Tags, config.NetworkInterfaceNodeIDKey)

	return &v1alpha1.CNINode{
		ObjectMeta: metav1.ObjectMeta{
			Name:            node.Name,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(node, corev1.SchemeGroupVersion.WithKind("Node"))},
			Labels: map[string]string{
				config.NodeLabelOS: node.Labels[config.NodeLabelOS],
			},
			Finalizers: []string{config.NodeTerminationFinalizer},
		},
		Spec: successorSpec,
	}
}
