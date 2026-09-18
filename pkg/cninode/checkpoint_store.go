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
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/aws/amazon-vpc-resource-controller-k8s/apis/vpcresources/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ReplacementCheckpointRecordLabel = "vpcresources.k8s.aws/cninode-replacement-checkpoint"
	ReplacementNodeNameAnnotation    = "vpcresources.k8s.aws/replacement-node-name"
)

func CheckpointRecordName(nodeName string) string {
	sum := sha256.Sum256([]byte(nodeName))
	return "cninode-replacement-" + hex.EncodeToString(sum[:12])
}

func IsCheckpointRecord(cniNode *v1alpha1.CNINode) bool {
	return cniNode.Labels[ReplacementCheckpointRecordLabel] == "true"
}

func EnsureCheckpointRecord(ctx context.Context, writer client.Client, reader client.Reader, nodeName string,
	nodeUID, sourceCNINodeUID types.UID, spec v1alpha1.CNINodeSpec,
) (*ReplacementCheckpoint, error) {
	checkpoint := &ReplacementCheckpoint{
		NodeUID:          nodeUID,
		SourceCNINodeUID: sourceCNINodeUID,
		Spec:             *spec.DeepCopy(),
	}
	value, err := encodeCheckpoint(checkpoint)
	if err != nil {
		return nil, err
	}

	key := types.NamespacedName{Name: CheckpointRecordName(nodeName)}
	record := &v1alpha1.CNINode{}
	if err := reader.Get(ctx, key, record); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, err
		}
		record = &v1alpha1.CNINode{
			ObjectMeta: metav1.ObjectMeta{
				Name: key.Name,
				Labels: map[string]string{
					ReplacementCheckpointRecordLabel: "true",
				},
				Annotations: map[string]string{
					ReplacementNodeNameAnnotation:   nodeName,
					ReplacementCheckpointAnnotation: value,
				},
			},
			Spec: *spec.DeepCopy(),
		}
		if err := writer.Create(ctx, record); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return EnsureCheckpointRecord(
					ctx, writer, reader, nodeName, nodeUID, sourceCNINodeUID, spec)
			}
			return nil, err
		}
		return checkpoint, nil
	}
	if !IsCheckpointRecord(record) ||
		record.Annotations[ReplacementNodeNameAnnotation] != nodeName {
		return nil, fmt.Errorf("CNINode %s collides with replacement checkpoint for Node %s",
			record.Name, nodeName)
	}
	if !record.GetDeletionTimestamp().IsZero() {
		return nil, fmt.Errorf("CNINode replacement checkpoint %s is terminating", record.Name)
	}
	updated := record.DeepCopy()
	if updated.Annotations == nil {
		updated.Annotations = make(map[string]string)
	}
	updated.Annotations[ReplacementCheckpointAnnotation] = value
	updated.Spec = *spec.DeepCopy()
	if record.Annotations[ReplacementCheckpointAnnotation] == value {
		return checkpoint, nil
	}
	if err := writer.Patch(ctx, updated,
		client.MergeFromWithOptions(record, client.MergeFromWithOptimisticLock{})); err != nil {
		return nil, err
	}
	return checkpoint, nil
}

func GetCheckpointRecord(ctx context.Context, reader client.Reader,
	nodeName string,
) (*ReplacementCheckpoint, bool, error) {
	record := &v1alpha1.CNINode{}
	if err := reader.Get(ctx, types.NamespacedName{Name: CheckpointRecordName(nodeName)}, record); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if !IsCheckpointRecord(record) ||
		record.Annotations[ReplacementNodeNameAnnotation] != nodeName {
		return nil, true, fmt.Errorf("CNINode %s is not a replacement checkpoint for Node %s",
			record.Name, nodeName)
	}
	checkpoint, err := decodeCheckpoint(record.Annotations[ReplacementCheckpointAnnotation])
	return checkpoint, true, err
}

func DeleteCheckpointRecord(ctx context.Context, writer client.Client, reader client.Reader,
	nodeName string, sourceCNINodeUID types.UID,
) error {
	record := &v1alpha1.CNINode{}
	if err := reader.Get(ctx, types.NamespacedName{Name: CheckpointRecordName(nodeName)}, record); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !IsCheckpointRecord(record) ||
		record.Annotations[ReplacementNodeNameAnnotation] != nodeName {
		return fmt.Errorf("CNINode %s is not a replacement checkpoint for Node %s",
			record.Name, nodeName)
	}
	checkpoint, err := decodeCheckpoint(record.Annotations[ReplacementCheckpointAnnotation])
	if err != nil {
		return err
	}
	if checkpoint.SourceCNINodeUID != sourceCNINodeUID {
		return nil
	}
	uid := record.UID
	resourceVersion := record.ResourceVersion
	return client.IgnoreNotFound(writer.Delete(ctx, record, &client.DeleteOptions{
		Preconditions: &metav1.Preconditions{
			UID:             &uid,
			ResourceVersion: &resourceVersion,
		},
	}))
}
