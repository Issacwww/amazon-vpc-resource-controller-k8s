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

package worker

import (
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/identity"
	"k8s.io/apimachinery/pkg/types"
)

// Operations are the supported operations for on demand resource handler
type Operations string

const (
	OperationProcessDeleteQueue Operations = "ProcessDeleteQueue"
	// OperationReconcileNode represents a reconcile operation that reclaims dangling network interfaces using local cache
	OperationReconcileNode Operations = "ReconcileNode"
	// OperationCreate represents pods that are in created state
	OperationCreate Operations = "Create"
	// OperationDelete represents pods that have been deleted
	OperationDeleted Operations = "Deleted"
	// OperationDeleting represents pods that are being deleted
	OperationDeleting Operations = "Deleting"
	// OperationReconcileNotRequired represents job that don't need execution
	OperationReconcileNotRequired Operations = "NoReconcile"
	// OperationReSyncPool represents a job to re-sync a dirty pool
	OperationReSyncPool Operations = "ReSyncPool"
	// OperationDeleteNode represents the job to delete the node
	OperationDeleteNode Operations = "NodeDelete"
)

// OnDemandJob represents the job that will be executed by the respective worker
type OnDemandJob struct {
	// Operation is the type of operation to perform on the given pod
	Operation Operations
	// UID is the unique ID of the pod
	UID string
	// PodName is the name of the pod
	PodName string
	// PodNamespace is the pod's namespace
	PodNamespace string
	// RequestCount is the number of resources to create for operations of type Create. Optional for other operations
	RequestCount int
	// NodeName is the k8s node name
	NodeName string
	// NodeUID identifies the Kubernetes Node generation validated for a Pod
	// allocation.
	NodeUID types.UID
	// InstanceID identifies the node generation for operations that can run
	// after a same-name Node has been created.
	InstanceID string
	// Generation identifies the exact provider-cache installation. Instance ID
	// alone cannot distinguish two lifecycles of the same EC2 instance.
	Generation string
}

// NewOnDemandDeleteJob returns an on demand job for operation Create or Update
func NewOnDemandCreateJob(podNamespace, podName string, requestCount int,
	allocations ...identity.Allocation,
) OnDemandJob {
	var allocation identity.Allocation
	if len(allocations) > 0 {
		allocation = allocations[0]
	}
	return OnDemandJob{
		Operation:    OperationCreate,
		UID:          string(allocation.PodUID),
		PodNamespace: podNamespace,
		PodName:      podName,
		RequestCount: requestCount,
		NodeName:     allocation.Name,
		NodeUID:      allocation.UID,
		InstanceID:   allocation.InstanceID,
	}
}

// NewOnDemandDeleteJob returns an on demand job for operation Deleted
func NewOnDemandDeletedJob(nodeName string, uid types.UID) OnDemandJob {
	return OnDemandJob{
		Operation: OperationDeleted,
		NodeName:  nodeName,
		UID:       string(uid),
	}
}

// NewOnDemandReconcileJob returns a reconcile job
func NewOnDemandReconcileNodeJob(nodeName string) OnDemandJob {
	return OnDemandJob{
		Operation: OperationReconcileNode,
		NodeName:  nodeName,
	}
}

// NewOnDemandProcessDeleteQueueJob returns a process delete queue job
func NewOnDemandProcessDeleteQueueJob(nodeName string) OnDemandJob {
	return OnDemandJob{
		Operation: OperationProcessDeleteQueue,
		NodeName:  nodeName,
	}
}

// NewOnDemandDeleteNodeJob returns a delete node job
func NewOnDemandDeleteNodeJob(nodeName, instanceID, generation string) OnDemandJob {
	return OnDemandJob{
		Operation:  OperationDeleteNode,
		NodeName:   nodeName,
		InstanceID: instanceID,
		Generation: generation,
	}
}

// WarmPoolJob represents the job for a resource handler for warm pool resources
type WarmPoolJob struct {
	// Operation is the type of operation on warm pool
	Operations Operations
	// Resources can hold the resource to delete or the created resources
	Resources []string
	// ResourceCount is the number of resource to be created
	ResourceCount int
	// NodeName is the name of the node
	NodeName string
	// Generation identifies the exact provider cache and pool that produced the
	// job. It prevents delayed jobs from an old same-name Node lifecycle from
	// mutating a replacement pool.
	Generation string
}

// NewWarmPoolCreateJob returns a job on warm pool of resource
func NewWarmPoolCreateJob(nodeName string, count int, generation ...string) *WarmPoolJob {
	return &WarmPoolJob{
		Operations:    OperationCreate,
		NodeName:      nodeName,
		ResourceCount: count,
		Generation:    warmPoolGeneration(generation),
	}
}

// NewWarmPoolReSyncJob returns a job to re-sync the warm pool with upstream
func NewWarmPoolReSyncJob(nodeName string, generation ...string) *WarmPoolJob {
	return &WarmPoolJob{
		Operations: OperationReSyncPool,
		NodeName:   nodeName,
		Generation: warmPoolGeneration(generation),
	}
}

func NewWarmPoolDeleteJob(nodeName string, resourcesToDelete []string, generation ...string) *WarmPoolJob {
	return &WarmPoolJob{
		Operations:    OperationDeleted,
		NodeName:      nodeName,
		Resources:     resourcesToDelete,
		ResourceCount: len(resourcesToDelete),
		Generation:    warmPoolGeneration(generation),
	}
}

func NewWarmProcessDeleteQueueJob(nodeName string, generation ...string) *WarmPoolJob {
	return &WarmPoolJob{
		Operations: OperationProcessDeleteQueue,
		NodeName:   nodeName,
		Generation: warmPoolGeneration(generation),
	}
}

func warmPoolGeneration(generation []string) string {
	if len(generation) == 0 {
		return ""
	}
	return generation[0]
}
