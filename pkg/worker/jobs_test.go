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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"
)

var (
	podName      = "pod-name"
	podNamespace = "pod-namespace"
	podUid       = "pod-uid"
	UID          = types.UID(podUid)
	reqCount     = 2
	nodeName     = "node-name"
	instanceID   = "i-0123456789abcdef0"
)

// TestNewOnDemandCreateJob tests the fields of Create Job
func TestNewOnDemandCreateJob(t *testing.T) {
	onDemandJob := NewOnDemandCreateJob(podNamespace, podName, reqCount)

	assert.Equal(t, OperationCreate, onDemandJob.Operation)
	assert.Equal(t, podName, onDemandJob.PodName)
	assert.Equal(t, podNamespace, onDemandJob.PodNamespace)
	assert.Equal(t, reqCount, onDemandJob.RequestCount)
}

// TestNewOnDemandDeleteJob tests the fields of Deleted Job
func TestNewOnDemandDeletedJob(t *testing.T) {
	onDemandJob := NewOnDemandDeletedJob(nodeName, UID)

	assert.Equal(t, OperationDeleted, onDemandJob.Operation)
	assert.Equal(t, podUid, onDemandJob.UID)
}

func TestNewOnDemandReconcileJob(t *testing.T) {
	onDemandJob := NewOnDemandReconcileNodeJob(nodeName)

	assert.Equal(t, OperationReconcileNode, onDemandJob.Operation)
	assert.Equal(t, nodeName, onDemandJob.NodeName)
}

func TestNewOnDemandProcessDeleteQueueJob(t *testing.T) {
	onDemandJob := NewOnDemandProcessDeleteQueueJob(nodeName)

	assert.Equal(t, OperationProcessDeleteQueue, onDemandJob.Operation)
	assert.Equal(t, nodeName, onDemandJob.NodeName)
}

func TestNewOnDemandDeleteNodeJob(t *testing.T) {
	generation := "cache-generation"
	onDemandJob := NewOnDemandDeleteNodeJob(nodeName, instanceID, generation)

	assert.Equal(t, OperationDeleteNode, onDemandJob.Operation)
	assert.Equal(t, nodeName, onDemandJob.NodeName)
	assert.Equal(t, instanceID, onDemandJob.InstanceID)
	assert.Equal(t, generation, onDemandJob.Generation)
}

func TestNewWarmPoolCreateJob(t *testing.T) {
	generation := "provider-generation"
	warmPoolJob := NewWarmPoolCreateJob(nodeName, 2, generation)

	assert.Equal(t, OperationCreate, warmPoolJob.Operations)
	assert.Equal(t, 2, warmPoolJob.ResourceCount)
	assert.Equal(t, nodeName, warmPoolJob.NodeName)
	assert.Equal(t, generation, warmPoolJob.Generation)
}

func TestNewWarmPoolDeleteJob(t *testing.T) {
	resources := []string{"res-1", "res-2"}
	generation := "provider-generation"
	WarmPoolJob := NewWarmPoolDeleteJob(nodeName, resources, generation)

	assert.Equal(t, OperationDeleted, WarmPoolJob.Operations)
	assert.Equal(t, nodeName, WarmPoolJob.NodeName)
	assert.Equal(t, resources, WarmPoolJob.Resources)
	assert.Equal(t, len(resources), WarmPoolJob.ResourceCount)
	assert.Equal(t, generation, WarmPoolJob.Generation)
}

func TestNewWarmPoolReSyncJob(t *testing.T) {
	generation := "provider-generation"
	WarmPoolJob := NewWarmPoolReSyncJob(nodeName, generation)

	assert.Equal(t, OperationReSyncPool, WarmPoolJob.Operations)
	assert.Equal(t, nodeName, WarmPoolJob.NodeName)
	assert.Equal(t, generation, WarmPoolJob.Generation)
}

func TestNewWarmProcessDeleteQueueJob(t *testing.T) {
	generation := "provider-generation"
	job := NewWarmProcessDeleteQueueJob(nodeName, generation)

	assert.Equal(t, OperationProcessDeleteQueue, job.Operations)
	assert.Equal(t, nodeName, job.NodeName)
	assert.Equal(t, generation, job.Generation)
}
