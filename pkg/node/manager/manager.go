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

package manager

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aws/amazon-vpc-resource-controller-k8s/apis/vpcresources/v1alpha1"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/api"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/condition"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/config"
	rcHealthz "github.com/aws/amazon-vpc-resource-controller-k8s/pkg/healthz"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/node"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/provider"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/resource"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/utils"
	asyncWorker "github.com/aws/amazon-vpc-resource-controller-k8s/pkg/worker"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
)

type manager struct {
	// Log is the logger for node manager
	Log logr.Logger
	// lock to prevent multiple routines to write/update to data store concurrently
	lock sync.RWMutex
	// dataStore is the in memory data store of all the managed/un-managed nodes in the cluster
	dataStore map[string]node.Node
	// nodeGenerations fences asynchronous jobs belonging to an older lifecycle
	// of a same-name Kubernetes Node.
	nodeGenerations map[string]string
	// activeNodeInits keeps a previous generation's cleanup barrier in place
	// until every Init that was already running has finished and cleaned up.
	activeNodeInits map[nodeGeneration]int
	// deletingNodes contains the instance generation whose asynchronous
	// provider cleanup must finish before a same-name Node can be added.
	deletingNodes map[string]nodeDeletion
	// resourceManager provides the resource provider for all supported resources
	resourceManager resource.ResourceManager
	// wrapper around the clients for all APIs used by controller
	wrapper api.Wrapper
	// worker for performing async operation on node APIs
	worker            asyncWorker.Worker
	conditions        condition.Conditions
	controllerVersion string
	stopHealthCheckAt time.Time
	clusterName       string
}

// Manager to perform operation on list of managed/un-managed node
type Manager interface {
	GetNode(nodeName string) (node node.Node, found bool)
	AddNode(nodeName string) error
	UpdateNode(nodeName string) error
	DeleteNode(nodeName string) error
	CheckNodeForLeakedENIs(nodeName string)
	SkipHealthCheck() bool
}

var ErrNodeCleanupInProgress = errors.New("cleanup for the previous node generation is still in progress")

// AsyncOperation is operation on a node after the lock has been released.
// All AsyncOperation are done without lock as it involves API calls that
// will temporarily block the access to Manager in Pod Watcher. This is done
// to prevent Pod startup latency for already processed nodes.
type AsyncOperation string

const (
	Init   = AsyncOperation("Init")
	Update = AsyncOperation("Update")
	Delete = AsyncOperation("Delete")
)

// NodeUpdateStatus represents the status of the Node on Update operation.
type NodeUpdateStatus string

const (
	ManagedToUnManaged = NodeUpdateStatus("managedToUnManaged")
	UnManagedToManaged = NodeUpdateStatus("UnManagedToManaged")
	StillManaged       = NodeUpdateStatus("Managed")
	StillUnManaged     = NodeUpdateStatus("UnManaged")
)

type AsyncOperationJob struct {
	op         AsyncOperation
	node       node.Node
	nodeName   string
	generation string
}

const pausingHealthCheckDuration = 10 * time.Minute

const nodeDeleteRetryDelay = 30 * time.Second

const nodeInitRetryDelay = time.Second

type nodeDeletion struct {
	instanceID       string
	generation       string
	activeCleanups   int
	cleanupSucceeded bool
}

type nodeGeneration struct {
	nodeName   string
	generation string
}

var (
	nodeGenerationCleanupRetryCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "node_generation_cleanup_retry_total",
			Help: "The number of failed node-generation cleanup attempts that were requeued",
		},
	)
	nodeGenerationCleanupPending = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "node_generation_cleanup_pending",
			Help: "The number of node generations waiting for provider cleanup",
		},
	)
	nodeGenerationMetricsOnce sync.Once
)

// NewNodeManager returns a new node manager
func NewNodeManager(logger logr.Logger, resourceManager resource.ResourceManager,
	wrapper api.Wrapper, worker asyncWorker.Worker, conditions condition.Conditions, clusterName string, controllerVersion string, healthzHandler *rcHealthz.HealthzHandler) (Manager, error) {

	manager := &manager{
		resourceManager:   resourceManager,
		Log:               logger,
		dataStore:         make(map[string]node.Node),
		nodeGenerations:   make(map[string]string),
		activeNodeInits:   make(map[nodeGeneration]int),
		deletingNodes:     make(map[string]nodeDeletion),
		wrapper:           wrapper,
		worker:            worker,
		conditions:        conditions,
		controllerVersion: controllerVersion,
		clusterName:       clusterName,
	}
	nodeGenerationMetricsOnce.Do(func() {
		metrics.Registry.MustRegister(nodeGenerationCleanupRetryCount, nodeGenerationCleanupPending)
	})

	// add health check on subpath for node manager
	healthzHandler.AddControllersHealthCheckers(
		map[string]healthz.Checker{"health-node-manager": manager.check()},
	)

	return manager, worker.StartWorkerPool(manager.performAsyncOperation)
}

func (m *manager) CheckNodeForLeakedENIs(nodeName string) {
	cachedNode, found := m.GetNode(nodeName)
	if !found || !cachedNode.IsManaged() {
		m.Log.V(1).Info("node not found or not managed by controller, skip reconciliation", "nodeName", nodeName)
		return
	}

	// Only start a goroutine when need to
	if time.Now().After(cachedNode.GetNextReconciliationTime()) {
		go func() {
			if resourceProvider, found := m.resourceManager.GetResourceProvider(config.ResourceNamePodENI); found {
				foundLeakedENI := resourceProvider.ReconcileNode(nodeName)
				if foundLeakedENI {
					cachedNode.SetReconciliationInterval(node.NodeInitialCleanupInterval)
				} else {
					interval := wait.Jitter(cachedNode.GetReconciliationInterval(), 5)
					if interval > node.MaxNodeReconciliationInterval {
						interval = node.MaxNodeReconciliationInterval
					}
					cachedNode.SetReconciliationInterval(interval)
				}
				cachedNode.SetNextReconciliationTime(time.Now().Add(cachedNode.GetReconciliationInterval()))
				m.Log.Info("reconciled node to cleanup leaked branch ENIs", "NodeName", nodeName, "NextInterval", cachedNode.GetReconciliationInterval(), "NextReconciliationTime", cachedNode.GetNextReconciliationTime())
			} else {
				// no SGP provider enabled
				return
			}
		}()
	}
}

// GetNode returns the node from in memory data store
func (m *manager) GetNode(nodeName string) (node node.Node, found bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	node, found = m.dataStore[nodeName]
	return
}

// AddNode adds the managed and un-managed nodes to the in memory data store, the
// user of node can verify if the node is managed before performing any operations
func (m *manager) AddNode(nodeName string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if previous, pending := m.deletingNodes[nodeName]; pending {
		m.Log.Info("waiting for previous node generation cleanup before adding node",
			"nodeName", nodeName, "previousInstanceID", previous.instanceID)
		return fmt.Errorf("%w: node %s instance %s", ErrNodeCleanupInProgress, nodeName, previous.instanceID)
	}

	k8sNode, err := m.wrapper.K8sAPI.GetNode(nodeName)
	if err != nil {
		return fmt.Errorf("failed to add node %s, doesn't exist in cache anymore", nodeName)
	}

	log := m.Log.WithValues("node name", k8sNode.Name, "request", "add")

	var newNode node.Node
	var nodeFound bool

	_, nodeFound = m.dataStore[k8sNode.Name]
	if nodeFound {
		log.Info("node is already processed, not processing add event again")
		return nil
	}

	if err = m.CreateCNINodeIfNotExisting(k8sNode); err != nil {
		m.Log.Error(err, "Failed to create CNINode for k8sNode", "NodeName", k8sNode.Name)
		return err
	}

	shouldManage, err := m.isSelectedForManagement(k8sNode)
	if err != nil {
		return err
	}

	var op AsyncOperation
	generation := uuid.NewString()

	if shouldManage {
		newNode = node.NewManagedNode(m.Log, k8sNode.Name, GetNodeInstanceID(k8sNode),
			GetNodeOS(k8sNode), m.wrapper.K8sAPI, m.wrapper.EC2API, k8sNode.UID)
		err := m.updateSubnetIfUsingENIConfig(newNode, k8sNode)
		if err != nil {
			return err
		}
		m.dataStore[k8sNode.Name] = newNode
		m.setNodeGenerationLocked(k8sNode.Name, generation)
		log.Info("node added as a managed node")
		op = Init
	} else {
		newNode = node.NewUnManagedNode(m.Log, k8sNode.Name, GetNodeInstanceID(k8sNode),
			GetNodeOS(k8sNode), k8sNode.UID)
		m.dataStore[k8sNode.Name] = newNode
		m.setNodeGenerationLocked(k8sNode.Name, generation)
		log.V(1).Info("node added as an un-managed node")
		return nil
	}

	m.worker.SubmitJob(AsyncOperationJob{
		op:         op,
		node:       newNode,
		nodeName:   nodeName,
		generation: generation,
	})
	return nil
}

func (m *manager) CreateCNINodeIfNotExisting(node *v1.Node) error {
	if cniNode, err := m.wrapper.K8sAPI.GetCNINode(
		types.NamespacedName{Name: node.Name},
	); err != nil {
		if apierrors.IsNotFound(err) {
			m.Log.Info("Will create a new CNINode", "CNINodeName", node.Name)
			return m.wrapper.K8sAPI.CreateCNINode(node, m.clusterName)
		}
		return err
	} else {
		m.Log.Info("The CNINode is already existing", "cninode", cniNode.Name, "features", cniNode.Spec.Features)
		return nil
	}
}

// UpdateNode updates the node object and, if the node is previously un-managed and now
// is selected for management, node resources are initialized, if the node is managed
// and now is not required to be managed, it's resources are de-initialized. Finally,
// if there is no toggling, the resources are updated
func (m *manager) UpdateNode(nodeName string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	k8sNode, err := m.wrapper.K8sAPI.GetNode(nodeName)
	if err != nil {
		return fmt.Errorf("failed to update node %s, doesn't exist in cache anymore", nodeName)
	}

	log := m.Log.WithValues("node name", nodeName, "request", "update")

	cachedNode, found := m.dataStore[nodeName]
	if !found {
		m.Log.Info("the node doesn't exist in cache anymore, it might have been deleted")
		return nil
	}

	var op AsyncOperation
	status, err := m.GetNodeUpdateStatus(k8sNode, cachedNode)

	if err != nil {
		return err
	}

	generation := m.nodeGenerationLocked(nodeName)
	switch status {
	case UnManagedToManaged:
		if previous, pending := m.deletingNodes[nodeName]; pending {
			return fmt.Errorf("%w: node %s instance %s", ErrNodeCleanupInProgress,
				nodeName, previous.instanceID)
		}
		log.Info("node was previously un-managed, will be added as managed node now")
		cachedNode = node.NewManagedNode(m.Log, k8sNode.Name,
			GetNodeInstanceID(k8sNode), GetNodeOS(k8sNode),
			m.wrapper.K8sAPI, m.wrapper.EC2API, k8sNode.UID)
		// Update the Subnet if the node has custom networking configured
		err = m.updateSubnetIfUsingENIConfig(cachedNode, k8sNode)
		if err != nil {
			return err
		}
		m.dataStore[nodeName] = cachedNode
		generation = uuid.NewString()
		m.setNodeGenerationLocked(nodeName, generation)
		op = Init
	case ManagedToUnManaged:
		log.Info("node was being managed earlier, will be added as un-managed node now")
		// Change the node in cache, but for de initializing all resource providers
		// pass the async job the older cached value instead
		previousGeneration := generation
		m.dataStore[nodeName] = node.NewUnManagedNode(m.Log, k8sNode.Name,
			GetNodeInstanceID(k8sNode), GetNodeOS(k8sNode), k8sNode.UID)
		generation = uuid.NewString()
		m.setNodeGenerationLocked(nodeName, generation)
		m.startNodeDeletionLocked(nodeName, cachedNode.GetNodeInstanceID(), previousGeneration)
		op = Delete
		generation = previousGeneration
	case StillManaged:
		// We only need to update the Subnet for Managed Node. This subnet is required for creating
		// Branch ENIs when user is using Custom Networking. In future, we should move this to
		// UpdateResources for Trunk ENI Provider as this is resource specific
		err = m.updateSubnetIfUsingENIConfig(cachedNode, k8sNode)
		if err != nil {
			return err
		}
		op = Update
	case StillUnManaged:
		log.V(1).Info("node not managed, no operation required")
		// No async operation required for un-managed nodes
		return nil
	}

	m.worker.SubmitJob(AsyncOperationJob{
		op:         op,
		node:       cachedNode,
		nodeName:   nodeName,
		generation: generation,
	})
	return nil
}

func (m *manager) GetNodeUpdateStatus(k8sNode *v1.Node, cachedNode node.Node) (NodeUpdateStatus, error) {
	isSelectedForManagement, err := m.isSelectedForManagement(k8sNode)

	if err != nil {
		return "", err
	}

	if isSelectedForManagement && !cachedNode.IsManaged() {
		return UnManagedToManaged, err
	} else if !isSelectedForManagement && cachedNode.IsManaged() {
		return ManagedToUnManaged, err
	} else if isSelectedForManagement {
		return StillManaged, err
	} else {
		return StillUnManaged, err
	}
}

// DeleteNode deletes the nodes from the cache and cleans up the resources used by all the resource providers
func (m *manager) DeleteNode(nodeName string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	log := m.Log.WithValues("node name", nodeName, "request", "delete")

	cachedNode, nodeFound := m.dataStore[nodeName]
	if !nodeFound {
		log.Info("node not found in the data store, ignoring the event")
		return nil
	}

	delete(m.dataStore, nodeName)
	generation := m.nodeGenerationLocked(nodeName)
	delete(m.nodeGenerations, nodeName)

	if !cachedNode.IsManaged() {
		log.V(1).Info("un managed node removed from data store")
		return nil
	}

	m.startNodeDeletionLocked(nodeName, cachedNode.GetNodeInstanceID(), generation)
	m.worker.SubmitJob(AsyncOperationJob{
		op:         Delete,
		node:       cachedNode,
		nodeName:   nodeName,
		generation: generation,
	})

	log.Info("node removed from data store")

	return nil
}

// updateSubnetIfUsingENIConfig updates the subnet id for the node to the subnet specified in ENIConfig if the node is
// using custom networking
func (m *manager) updateSubnetIfUsingENIConfig(cachedNode node.Node, k8sNode *v1.Node) error {
	eniConfigName, isPresent := k8sNode.Labels[config.CustomNetworkingLabel]
	var cniNodeEnabled bool
	var err error
	if !isPresent {
		if cniNodeEnabled, err = m.customNetworkEnabledInCNINode(k8sNode); err != nil {
			return err
		}
	}

	if isPresent || cniNodeEnabled {
		if !isPresent {
			var err error
			eniConfigName, err = m.GetEniConfigName(k8sNode)
			// if we couldn't find the name from CNINode, this should be a misconfiguration
			// as long as the feature is registered in CNINode, we couldn't easily use "" as eniconfig name
			// when not able to find the name from CNINode.
			if err != nil {
				if errors.Is(err, utils.ErrNotFound) {
					utils.SendNodeEventWithNodeObject(
						m.wrapper.K8sAPI, k8sNode, utils.EniConfigNameNotFoundReason, err.Error(), v1.EventTypeWarning, m.Log)
				}
				return err
			}
		}
		eniConfig, err := m.wrapper.K8sAPI.GetENIConfig(eniConfigName)
		if err != nil {
			return fmt.Errorf("failed to find the ENIConfig %s: %v", eniConfigName, err)
		}
		if eniConfig.Spec.Subnet != "" {
			m.Log.V(1).Info("node is using custom networking, updating the subnet", "node", k8sNode.Name,
				"subnet", eniConfig.Spec.Subnet)
			cachedNode.UpdateCustomNetworkingSpecs(eniConfig.Spec.Subnet, eniConfig.Spec.SecurityGroups)
			return nil
		}
	} else {
		cachedNode.UpdateCustomNetworkingSpecs("", nil)
	}
	return nil
}

func (m *manager) GetEniConfigName(node *v1.Node) (string, error) {
	cniNode, err := m.wrapper.K8sAPI.GetCNINode(types.NamespacedName{Name: node.Name})
	if err != nil {
		return "", err
	}
	for _, feature := range cniNode.Spec.Features {
		if feature.Name == v1alpha1.CustomNetworking && feature.Value != "" {
			return feature.Value, nil
		}
	}

	return "", fmt.Errorf("couldn't find custom networking eniconfig name for node %s, error: %w", node.Name, utils.ErrNotFound)
}

// performAsyncOperation performs the operation on a node without taking the node manager lock
func (m *manager) performAsyncOperation(job interface{}) (ctrl.Result, error) {
	asyncJob, ok := job.(AsyncOperationJob)
	if !ok {
		m.Log.Error(fmt.Errorf("wrong job type submitted"), "not re-queuing")
		return ctrl.Result{}, nil
	}

	log := m.Log.WithValues("node", asyncJob.nodeName, "operation", asyncJob.op)

	switch asyncJob.op {
	case Init:
		if !m.beginNodeInit(asyncJob.nodeName, asyncJob.generation) {
			log.Info("skipping stale node operation", "generation", asyncJob.generation)
			return ctrl.Result{}, nil
		}
		defer m.endNodeInit(asyncJob.nodeName, asyncJob.generation)
		return m.performNodeInit(asyncJob, log)
	case Update:
		if !m.isCurrentNodeGeneration(asyncJob.nodeName, asyncJob.generation) {
			log.Info("skipping stale node operation", "generation", asyncJob.generation)
			return ctrl.Result{}, nil
		}
		return m.finishNodeOperation(asyncJob,
			asyncJob.node.UpdateResources(m.resourceManager), log)
	case Delete:
		return m.performNodeDelete(asyncJob, log)
	default:
		m.Log.V(1).Info("no operation operation requested",
			"node", asyncJob.nodeName)
		return ctrl.Result{}, nil
	}
}

func (m *manager) performNodeInit(asyncJob AsyncOperationJob,
	log logr.Logger,
) (ctrl.Result, error) {
	utils.SendNodeEventWithNodeName(m.wrapper.K8sAPI, asyncJob.nodeName, utils.VersionNotice,
		fmt.Sprintf("The node is managed by VPC resource controller version %s", m.controllerVersion),
		v1.EventTypeNormal, m.Log)
	if err := asyncJob.node.InitResources(m.resourceManager); err != nil {
		return m.handleNodeInitError(asyncJob, err, log)
	}
	if !m.isCurrentNodeGeneration(asyncJob.nodeName, asyncJob.generation) {
		return m.cleanupStaleNodeGeneration(asyncJob, log)
	}
	asyncJob.op = Update
	return m.performAsyncOperation(asyncJob)
}

func (m *manager) handleNodeInitError(asyncJob AsyncOperationJob, err error,
	log logr.Logger,
) (ctrl.Result, error) {
	if !m.isCurrentNodeGeneration(asyncJob.nodeName, asyncJob.generation) {
		return m.cleanupStaleNodeGeneration(asyncJob, log)
	}
	if errors.Is(err, provider.ErrNodeGenerationCleanupInProgress) {
		nodeGenerationCleanupRetryCount.Inc()
		log.Info("waiting for previous provider generation cleanup")
		return ctrl.Result{Requeue: true, RequeueAfter: nodeInitRetryDelay}, nil
	}
	if pauseHealthCheckOnError(err) && !m.SkipHealthCheck() {
		m.setStopHealthCheck()
		log.Info("node manager sets a pause on health check due to observing a EC2 error",
			"error", err.Error())
	}
	log.Error(err, "removing the node from cache as it failed to initialize")
	m.removeNodeSafe(asyncJob.nodeName, asyncJob.generation)
	// Node will be retried for init on the next event.
	return ctrl.Result{}, nil
}

func (m *manager) cleanupStaleNodeGeneration(asyncJob AsyncOperationJob,
	log logr.Logger,
) (ctrl.Result, error) {
	log.Info("node generation changed while resources were initializing; cleaning up stale resources",
		"generation", asyncJob.generation)
	asyncJob.op = Delete
	m.requireNodeCleanup(asyncJob.nodeName, asyncJob.node.GetNodeInstanceID(),
		asyncJob.generation)
	result, err := m.performNodeDelete(asyncJob, log)
	if err == nil && result.Requeue {
		// The worker is currently processing the original Init value. Submit the
		// converted Delete explicitly so the timed retry cannot be requeued as a
		// stale Init and silently abandon cleanup.
		m.worker.SubmitJobAfter(asyncJob, result.RequeueAfter)
		return ctrl.Result{}, nil
	}
	return result, err
}

func (m *manager) performNodeDelete(asyncJob AsyncOperationJob,
	log logr.Logger,
) (ctrl.Result, error) {
	if !m.beginNodeCleanup(asyncJob.nodeName, asyncJob.generation) {
		log.Info("skipping cleanup for a completed node generation",
			"generation", asyncJob.generation)
		return ctrl.Result{}, nil
	}
	err := asyncJob.node.DeleteResources(m.resourceManager)
	m.finishNodeCleanup(asyncJob.nodeName, asyncJob.generation, err)
	if err == nil {
		log.V(1).Info("successfully performed node operation")
		return ctrl.Result{}, nil
	}
	log.Error(err, "failed to perform node operation")
	nodeGenerationCleanupRetryCount.Inc()
	return ctrl.Result{Requeue: true, RequeueAfter: nodeDeleteRetryDelay}, nil
}

func (m *manager) finishNodeOperation(asyncJob AsyncOperationJob, err error,
	log logr.Logger,
) (ctrl.Result, error) {
	if err == nil {
		log.V(1).Info("successfully performed node operation")
		return ctrl.Result{}, nil
	}
	log.Error(err, "failed to perform node operation")

	return ctrl.Result{}, nil
}

func (m *manager) beginNodeInit(nodeName, generation string) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	if generation == "" || m.nodeGenerations[nodeName] != generation {
		return false
	}
	if m.activeNodeInits == nil {
		m.activeNodeInits = make(map[nodeGeneration]int)
	}
	m.activeNodeInits[nodeGeneration{nodeName: nodeName, generation: generation}]++
	return true
}

func (m *manager) endNodeInit(nodeName, generation string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	key := nodeGeneration{nodeName: nodeName, generation: generation}
	if m.activeNodeInits[key] <= 1 {
		delete(m.activeNodeInits, key)
	} else {
		m.activeNodeInits[key]--
	}
	m.completeNodeDeletionLocked(nodeName, generation)
}

// isSelectedForManagement returns true if the node should be managed by the controller
func (m *manager) isSelectedForManagement(v1node *v1.Node) (bool, error) {
	os := GetNodeOS(v1node)
	instanceID := GetNodeInstanceID(v1node)

	if os == "" || instanceID == "" {
		m.Log.V(1).Info("node doesn't have os/instance id", v1node.Name,
			"os", os, "instance ID", instanceID)
		return false, nil
	}

	if isWindowsNode(v1node) && m.conditions.IsWindowsIPAMEnabled() {
		return true, nil
	} else {
		return m.canAttachTrunk(v1node)
	}
}

// GetNodeInstanceID returns the EC2 instance ID of a node
func GetNodeInstanceID(node *v1.Node) string {
	var instanceID string

	if node.Spec.ProviderID != "" {
		// ProviderID is preferred when available.
		// aws:///us-west-2c/i-01234567890abcdef
		id := strings.Split(node.Spec.ProviderID, "/")
		instanceID = id[len(id)-1]
	}

	return instanceID
}

// GetNodeOS returns the operating system of a node.
func GetNodeOS(node *v1.Node) string {
	labels := node.GetLabels()
	os := labels[config.NodeLabelOS]
	if os == "" {
		// For older k8s version.
		os = labels[config.NodeLabelOSBeta]
	}
	return os
}

// isWindowsNode returns true if the "kubernetes.io/os" or "beta.kubernetes.io/os" is set to windows
func isWindowsNode(node *v1.Node) bool {
	labels := node.GetLabels()

	nodeOS, ok := labels[config.NodeLabelOS]
	if !ok {
		nodeOS, ok = labels[config.NodeLabelOSBeta]
		if !ok {
			return false
		}
	}

	return nodeOS == config.OSWindows
}

// canAttachTrunk returns true if the node has capability to attach a Trunk ENI
func (m *manager) canAttachTrunk(node *v1.Node) (bool, error) {
	if _, ok := node.Labels[config.HasTrunkAttachedLabel]; ok == true {
		return true, nil
	}
	enabled, err := m.trunkEnabledInCNINode(node)
	return enabled, err
}

func (m *manager) trunkEnabledInCNINode(node *v1.Node) (bool, error) {
	var err error
	if cniNode, err := m.wrapper.K8sAPI.GetCNINode(
		types.NamespacedName{Name: node.Name},
	); err == nil {
		if lo.ContainsBy(cniNode.Spec.Features, func(addedFeature v1alpha1.Feature) bool {
			return addedFeature.Name == v1alpha1.SecurityGroupsForPods
		}) {
			return true, nil
		}
	}
	return false, err
}

func (m *manager) customNetworkEnabledInCNINode(node *v1.Node) (bool, error) {
	var err error
	if cniNode, err := m.wrapper.K8sAPI.GetCNINode(
		types.NamespacedName{Name: node.Name},
	); err == nil {
		if lo.ContainsBy(cniNode.Spec.Features, func(addedFeature v1alpha1.Feature) bool {
			return addedFeature.Name == v1alpha1.CustomNetworking
		}) {
			return true, err
		}
	}
	return false, err
}

func (m *manager) removeNodeSafe(nodeName, generation string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.nodeGenerations[nodeName] != generation {
		return
	}
	delete(m.dataStore, nodeName)
	delete(m.nodeGenerations, nodeName)
}

func (m *manager) setNodeGenerationLocked(nodeName, generation string) {
	if m.nodeGenerations == nil {
		m.nodeGenerations = make(map[string]string)
	}
	m.nodeGenerations[nodeName] = generation
}

func (m *manager) nodeGenerationLocked(nodeName string) string {
	if generation := m.nodeGenerations[nodeName]; generation != "" {
		return generation
	}
	generation := uuid.NewString()
	m.setNodeGenerationLocked(nodeName, generation)
	return generation
}

func (m *manager) isCurrentNodeGeneration(nodeName, generation string) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return generation != "" && m.nodeGenerations[nodeName] == generation
}

func (m *manager) startNodeDeletionLocked(nodeName, instanceID, generation string) {
	if m.deletingNodes == nil {
		m.deletingNodes = make(map[string]nodeDeletion)
	}
	if deleting, pending := m.deletingNodes[nodeName]; pending &&
		deleting.generation == generation {
		deleting.cleanupSucceeded = false
		m.deletingNodes[nodeName] = deleting
		return
	}
	if _, pending := m.deletingNodes[nodeName]; !pending {
		nodeGenerationCleanupPending.Inc()
	}
	m.deletingNodes[nodeName] = nodeDeletion{
		instanceID:       instanceID,
		generation:       generation,
		cleanupSucceeded: false,
	}
}

func (m *manager) requireNodeCleanup(nodeName, instanceID, generation string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.startNodeDeletionLocked(nodeName, instanceID, generation)
}

func (m *manager) beginNodeCleanup(nodeName, generation string) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	deleting, pending := m.deletingNodes[nodeName]
	if !pending || deleting.generation != generation {
		return false
	}
	deleting.activeCleanups++
	m.deletingNodes[nodeName] = deleting
	return true
}

func (m *manager) finishNodeCleanup(nodeName, generation string, cleanupErr error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	deleting, pending := m.deletingNodes[nodeName]
	if !pending || deleting.generation != generation {
		return
	}
	if deleting.activeCleanups > 0 {
		deleting.activeCleanups--
	}
	// When cleanup attempts overlap, the result of the last attempt to finish
	// controls the barrier. A success must not remain sticky across a later
	// failure.
	deleting.cleanupSucceeded = cleanupErr == nil
	m.deletingNodes[nodeName] = deleting
	m.completeNodeDeletionLocked(nodeName, generation)
}

func (m *manager) completeNodeDeletionLocked(nodeName, generation string) {
	deleting, pending := m.deletingNodes[nodeName]
	if !pending || deleting.generation != generation ||
		deleting.activeCleanups != 0 || !deleting.cleanupSucceeded {
		return
	}
	key := nodeGeneration{nodeName: nodeName, generation: generation}
	if m.activeNodeInits[key] != 0 {
		return
	}
	delete(m.deletingNodes, nodeName)
	nodeGenerationCleanupPending.Dec()
}

func (m *manager) check() healthz.Checker {
	// instead of using SimplePing, testing the node cache from manager makes the test more accurate
	return func(req *http.Request) error {
		err := rcHealthz.PingWithTimeout(func(c chan<- error) {
			randomName := uuid.New().String()
			m.Log.V(1).Info("starting health check call to acquire read lock of node manager through get node function")
			_, found := m.GetNode(randomName)
			m.Log.V(1).Info("health check tested ping GetNode to check on datastore cache in node manager successfully", "TesedNodeName", randomName, "NodeFound", found)
			if m.SkipHealthCheck() {
				m.Log.Info("due to EC2 error, node manager skips node worker queue health check for now")
			} else {
				var ping interface{}
				m.Log.V(1).Info("starting health check to acquire lock on work queue of node manager")
				m.worker.SubmitJob(ping)
				m.Log.V(1).Info("health check tested ping SubmitJob with a nil job to check on worker queue in node manager successfully")
			}
			c <- nil
		}, m.Log)

		return err
	}
}

func (m *manager) SkipHealthCheck() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return time.Since(m.stopHealthCheckAt) < pausingHealthCheckDuration
}

func (m *manager) setStopHealthCheck() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.stopHealthCheckAt = time.Now()
}

func pauseHealthCheckOnError(err error) bool {
	return lo.ContainsBy(utils.PauseHealthCheckErrors, func(e string) bool {
		return strings.Contains(err.Error(), e)
	})
}
