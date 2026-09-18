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

package prefix

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/api"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/aws/ec2"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/aws/vpc"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/condition"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/config"
	rcHealthz "github.com/aws/amazon-vpc-resource-controller-k8s/pkg/healthz"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/identity"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/pool"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/provider"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/provider/ip"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/provider/ip/eni"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/utils"
	"github.com/aws/amazon-vpc-resource-controller-k8s/pkg/worker"
	"github.com/google/uuid"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
)

type ipv4PrefixProvider struct {
	// log is the logger initialized with prefix provider details
	log logr.Logger
	// apiWrapper wraps all clients used by the controller
	apiWrapper api.Wrapper
	// workerPool with worker routine to execute asynchronous job on the prefix provider
	workerPool worker.Worker
	// config is the warm pool configuration for the resource prefix
	config *config.WarmPoolConfig
	// lock to allow multiple routines to access the cache concurrently
	lock sync.RWMutex // guards the following
	// instanceResources stores the ENIManager and the resource pool per instance
	instanceProviderAndPool map[string]*ResourceProviderAndPool
	// conditions is used to check which IP allocation mode is enabled
	conditions condition.Conditions
	// healthz check subpath
	checker healthz.Checker
}

// ResourceProviderAndPool contains the instance's ENI manager and the resource pool
type ResourceProviderAndPool struct {
	// lock prevents replacement or deletion while an asynchronous job is using
	// this provider generation.
	lock         sync.RWMutex
	eniManager   eni.ENIManager
	resourcePool pool.Pool
	generation   string
	identity     identity.Node
	// capacity is stored so that it can be advertised when node is updated
	capacity int
	// isPrevPDEnabled stores whether PD was enabled previously
	isPrevPDEnabled bool
}

type resourceGeneration struct {
	generation string
	identity   identity.Node
}

func NewIPv4PrefixProvider(log logr.Logger, apiWrapper api.Wrapper, workerPool worker.Worker,
	resourceConfig config.ResourceConfig, conditions condition.Conditions) provider.ResourceProvider {
	provider := &ipv4PrefixProvider{
		instanceProviderAndPool: make(map[string]*ResourceProviderAndPool),
		config:                  resourceConfig.WarmPoolConfig,
		log:                     log,
		apiWrapper:              apiWrapper,
		workerPool:              workerPool,
		conditions:              conditions,
	}
	provider.checker = provider.check()
	return provider
}

func (p *ipv4PrefixProvider) InitResource(instance ec2.EC2Instance) error {
	return p.InitResourceForNode(instance, identity.Node{})
}

func (p *ipv4PrefixProvider) InitResourceForNode(instance ec2.EC2Instance, nodeIdentity identity.Node) error {
	nodeName := instance.Name()

	eniManager := eni.NewENIManager(instance)
	ipV4Resources, err := eniManager.InitResources(p.apiWrapper.EC2API)
	if err != nil || ipV4Resources == nil {
		if errors.Is(err, utils.ErrNotFound) {
			msg := fmt.Sprintf("The instance type %s is not supported for Windows", instance.Type())
			utils.SendNodeEventWithNodeName(p.apiWrapper.K8sAPI, instance.Name(), utils.UnsupportedInstanceTypeReason, msg, v1.EventTypeWarning, p.log)
		}

		return err
	}

	presentPrefixes := ipV4Resources.IPv4Prefixes

	presentSecondaryIPSet := make(map[string]struct{})
	for _, ip := range ipV4Resources.PrivateIPv4Addresses {
		presentSecondaryIPSet[ip] = struct{}{}
	}

	pods, err := p.apiWrapper.PodAPI.GetRunningPodsOnNode(nodeName)
	if err != nil {
		return err
	}

	// Construct map of all possible IPs to prefix for each assigned prefix
	warmResourceIDToGroup := map[string]string{}
	for _, prefix := range presentPrefixes {
		ips, err := utils.DeconstructIPsFromPrefix(prefix)
		if err != nil {
			return err
		}
		for _, ip := range ips {
			warmResourceIDToGroup[ip] = prefix
		}
	}

	podToResourceMap := make(map[string]pool.Resource)
	numberUsedSecondaryIP := 0

	for _, pod := range pods {
		annotation, present := pod.Annotations[config.ResourceNameIPAddress]
		if !present {
			continue
		}

		prefix, found := warmResourceIDToGroup[annotation]
		if found {
			// store running pod into map of used resources
			podToResourceMap[string(pod.UID)] = pool.Resource{GroupID: prefix, ResourceID: annotation}
			// remove running pod's IP from warm resources
			delete(warmResourceIDToGroup, annotation)
		} else {
			// If running pod's IP is not deconstructed from an assigned prefix on the instance, ignore it
			p.log.Info("ignoring non-prefix deconstructed IP", "IPv4 address ", annotation)
			if _, exist := presentSecondaryIPSet[annotation]; exist {
				numberUsedSecondaryIP++
			}
		}
	}

	// Construct map of warm Resources, key is prefix, value is list of Resources belonged to that prefix
	warmResources := make(map[string][]pool.Resource)
	for ip, prefix := range warmResourceIDToGroup {
		warmResources[prefix] = append(warmResources[prefix], pool.Resource{GroupID: prefix, ResourceID: ip})
	}

	// Expected node capacity based on instance type in PD mode
	nodeCapacity := getCapacity(instance.Type(), instance.Os()) * pool.NumIPv4AddrPerPrefix

	isPDEnabled := p.conditions.IsWindowsPrefixDelegationEnabled()
	p.config = pool.GetWinWarmPoolConfig(p.log, p.apiWrapper, isPDEnabled)

	// Set warm pool config to empty if PD is not enabled
	prefixIPWPConfig := p.config
	if !isPDEnabled {
		prefixIPWPConfig = &config.WarmPoolConfig{}
	} else {
		// Log the discrepancy between the advertised and the actual node capacity when it is in PD mode
		if numberUsedSecondaryIP > 0 {
			actualCapacity := (getCapacity(instance.Type(), instance.Os()) - numberUsedSecondaryIP) * pool.NumIPv4AddrPerPrefix
			p.log.Info("there could be discrepancy between advertised and actual node capacity due to existing pods from "+
				"secondary IP mode", "node name", instance.Name(), "advertised capacity", nodeCapacity,
				"actual capacity", actualCapacity)
		}
	}

	generation := uuid.NewString()
	resourcePool := pool.NewResourcePool(p.log.WithName("prefix ipv4 address resource pool").
		WithValues("node name", instance.Name()), prefixIPWPConfig, podToResourceMap,
		warmResources, instance.Name(), nodeCapacity, true, generation)

	p.putInstanceProviderAndPoolForGeneration(nodeName, resourcePool, eniManager, nodeCapacity, isPDEnabled,
		resourceGeneration{generation: generation, identity: nodeIdentity})

	p.log.Info("initialized the resource provider for ipv4 prefix",
		"capacity", nodeCapacity, "node name", nodeName, "instance type",
		instance.Type(), "instance ID", instance.InstanceID(), "warmPoolConfig", p.config)

	job := resourcePool.ReconcilePool()
	if job.Operations != worker.OperationReconcileNotRequired {
		p.SubmitAsyncJob(job)
	}

	// Submit the async job to periodically process the delete queue
	p.SubmitAsyncJob(worker.NewWarmProcessDeleteQueueJob(nodeName, generation))
	return nil
}

func (p *ipv4PrefixProvider) DeInitResource(instance ec2.EC2Instance) error {
	nodeName := instance.Name()
	p.deleteInstanceProviderAndPool(nodeName)

	return nil
}

func (p *ipv4PrefixProvider) UpdateResourceCapacity(instance ec2.EC2Instance) error {
	resourceProviderAndPool, isPresent := p.getInstanceProviderAndPool(instance.Name())
	if !isPresent {
		p.log.Error(utils.ErrNotFound, utils.ErrMsgProviderAndPoolNotFound, "node name", instance.Name())
		return nil
	}

	resourceProviderAndPool.lock.Lock()
	defer resourceProviderAndPool.lock.Unlock()

	// Check if PD is enabled
	isCurrPDEnabled := p.conditions.IsWindowsPrefixDelegationEnabled()

	// Previous state and current state are both PD disabled, which means secondary IP provider has been active without toggling, hence
	// no need to update the warm pool config or node capacity as prefix provider
	if !resourceProviderAndPool.isPrevPDEnabled && !isCurrPDEnabled {
		p.log.V(1).Info("secondary IP provider has been active without toggling, no update from prefix provider",
			"isPrevPDEnabled", resourceProviderAndPool.isPrevPDEnabled, "isCurrPDEnabled", isCurrPDEnabled)
		return nil
	}

	// If toggling from PD to secondary IP mode, then set the prefix IP pool state to draining and
	// do not update the capacity as that would be done by secondary IP provider
	if resourceProviderAndPool.isPrevPDEnabled && !isCurrPDEnabled {
		resourceProviderAndPool.isPrevPDEnabled = false
		p.log.Info("Secondary IP provider should be active")
		job := resourceProviderAndPool.resourcePool.SetToDraining()
		if job.Operations != worker.OperationReconcileNotRequired {
			p.SubmitAsyncJob(job)
		}
		return nil
	}

	resourceProviderAndPool.isPrevPDEnabled = true

	warmPoolConfig := pool.GetWinWarmPoolConfig(p.log, p.apiWrapper, isCurrPDEnabled)

	// Set the secondary IP provider pool state to active
	job := resourceProviderAndPool.resourcePool.SetToActive(warmPoolConfig)
	if job.Operations != worker.OperationReconcileNotRequired {
		p.SubmitAsyncJob(job)
	}

	instanceType := instance.Type()
	instanceName := instance.Name()
	os := instance.Os()

	capacity := resourceProviderAndPool.capacity

	// Advertise capacity of private IPv4 addresses deconstructed from prefixes
	err := p.apiWrapper.K8sAPI.AdvertiseCapacityIfNotSet(instanceName, config.ResourceNameIPAddress, capacity)
	if err != nil {
		return err
	}
	p.log.V(1).Info("advertised capacity",
		"instance", instanceName, "instance type", instanceType, "os", os, "capacity", capacity)

	return nil
}

func (p *ipv4PrefixProvider) SubmitAsyncJob(job interface{}) {
	p.workerPool.SubmitJob(job)
}

func (p *ipv4PrefixProvider) ProcessAsyncJob(job interface{}) (ctrl.Result, error) {
	warmPoolJob, isValid := job.(*worker.WarmPoolJob)
	if !isValid {
		return ctrl.Result{}, fmt.Errorf("invalid job type")
	}

	switch warmPoolJob.Operations {
	case worker.OperationCreate:
		p.CreateIPv4PrefixAndUpdatePool(warmPoolJob)
	case worker.OperationDeleted:
		p.DeleteIPv4PrefixAndUpdatePool(warmPoolJob)
	case worker.OperationReSyncPool:
		p.ReSyncPool(warmPoolJob)
	case worker.OperationProcessDeleteQueue:
		return p.ProcessDeleteQueue(warmPoolJob)
	}

	return ctrl.Result{}, nil
}

// CreateIPv4PrefixAndUpdatePool executes the Create IPv4 Prefix workflow by assigning enough prefixes to satisfy
// the desired number of prefixes required by the warm pool job
func (p *ipv4PrefixProvider) CreateIPv4PrefixAndUpdatePool(job *worker.WarmPoolJob) {
	instanceResource, release, found := p.lockResourceForJob(job)
	if !found {
		p.log.Error(utils.ErrNotFound, utils.ErrMsgProviderAndPoolNotFound, "node name", job.NodeName)
		return
	}
	defer release()

	// For successful jobs or non-retryable errors, do not re-sync or reconcile the pool.
	notRetry := true
	// If subnet has sufficient cidr blocks, prefixAvailable is true, otherwise false.
	prefixAvailable := true

	resources, err := instanceResource.eniManager.CreateIPV4Resource(job.ResourceCount, config.ResourceTypeIPv4Prefix, p.apiWrapper.EC2API,
		p.log)

	if err != nil {
		p.log.Error(err, "failed to create all/some of the IPv4 prefixes", "created resources", resources)

		// For retryable errors, set notRetry as false to re-sync and reconcile the pool.
		if utils.ShouldRetryOnError(err) {
			notRetry = false
		}

		//TODO: This adds a dependency on EC2 API error. Refactor later.
		if strings.HasPrefix(err.Error(), utils.InsufficientCidrBlocksReason) {
			// Prefix not available in the subnet, set status and pass it to pool. Note this is a non-retryable error.
			prefixAvailable = false
			// Send node event to inform user of insufficient CIDR blocks error
			utils.SendNodeEventWithNodeName(p.apiWrapper.K8sAPI, job.NodeName, utils.InsufficientCidrBlocksReason,
				utils.ErrInsufficientCidrBlocks.Error(), v1.EventTypeWarning, p.log)
		}
	}
	job.Resources = resources
	p.updatePoolAndReconcileIfRequired(instanceResource.resourcePool, job, notRetry, prefixAvailable)
}

// DeleteIPv4PrefixAndUpdatePool executes the Delete IPv4 Prefix workflow for the list of prefixes provided in the warm pool job
func (p *ipv4PrefixProvider) DeleteIPv4PrefixAndUpdatePool(job *worker.WarmPoolJob) {
	instanceResource, release, found := p.lockResourceForJob(job)
	if !found {
		p.log.Error(utils.ErrNotFound, utils.ErrMsgProviderAndPoolNotFound, "node name", job.NodeName)
		return
	}
	defer release()

	didSucceed := true
	failedResources, err := instanceResource.eniManager.DeleteIPV4Resource(job.Resources, config.ResourceTypeIPv4Prefix,
		p.apiWrapper.EC2API, p.log)

	if err != nil {
		p.log.Error(err, "failed to delete all/some of the IPv4 prefixes", "failed resources", failedResources)
		didSucceed = false
	}
	job.Resources = failedResources
	p.updatePoolAndReconcileIfRequired(instanceResource.resourcePool, job, didSucceed, true)
}

func (p *ipv4PrefixProvider) ReSyncPool(job *worker.WarmPoolJob) {
	providerAndPool, release, found := p.lockResourceForJob(job)
	if !found {
		p.log.Error(utils.ErrNotFound, "node is not initialized", "node name", job.NodeName)
		return
	}
	defer release()

	ipV4Resources, err := providerAndPool.eniManager.InitResources(p.apiWrapper.EC2API)
	if err != nil || ipV4Resources == nil {
		p.log.Error(err, "failed to get init resources for the node", "node name", job.NodeName)
		return
	}

	providerAndPool.resourcePool.ReSync(ipV4Resources.IPv4Prefixes)
}

func (p *ipv4PrefixProvider) ProcessDeleteQueue(job *worker.WarmPoolJob) (ctrl.Result, error) {
	resourceProviderAndPool, release, isPresent := p.lockResourceForJob(job)
	if !isPresent {
		p.log.Info("forgetting the delete queue processing job", "node name", job.NodeName)
		return ctrl.Result{}, nil
	}
	defer release()
	// TODO: For efficiency run only when required in next release
	resourceProviderAndPool.resourcePool.ProcessCoolDownQueue()

	// After the cool down queue is processed check if we need to do reconciliation
	job = resourceProviderAndPool.resourcePool.ReconcilePool()
	if job.Operations != worker.OperationReconcileNotRequired {
		p.SubmitAsyncJob(job)
	}

	// Re-submit the job to execute after cool down period has ended
	return ctrl.Result{Requeue: true, RequeueAfter: config.CoolDownPeriod}, nil
}

// updatePoolAndReconcileIfRequired updates the resource pool and reconcile again and submit a new job if required
func (p *ipv4PrefixProvider) updatePoolAndReconcileIfRequired(resourcePool pool.Pool, job *worker.WarmPoolJob, didSucceed bool,
	prefixAvailable bool) {
	// Update the pool to add the created/failed resource to the warm pool and decrement the pending count
	shouldReconcile := resourcePool.UpdatePool(job, didSucceed, prefixAvailable)

	if shouldReconcile {
		job := resourcePool.ReconcilePool()
		if job.Operations != worker.OperationReconcileNotRequired {
			p.SubmitAsyncJob(job)
		}
	}
}

func (p *ipv4PrefixProvider) GetPool(nodeName string) (pool.Pool, bool) {
	providerAndPool, exists := p.getInstanceProviderAndPool(nodeName)
	if !exists {
		return nil, false
	}
	return providerAndPool.resourcePool, true
}

// AcquirePool holds the current provider generation stable while a synchronous
// Pod operation mutates the pool and commits its annotation.
func (p *ipv4PrefixProvider) AcquirePool(nodeName string, expected identity.Node) (pool.Pool, func(), bool) {
	resource, found := p.getInstanceProviderAndPool(nodeName)
	if !found {
		return nil, nil, false
	}
	resource.lock.RLock()
	current, stillCurrent := p.getInstanceProviderAndPool(nodeName)
	if !stillCurrent || current != resource || !identity.Matches(expected, resource.identity) {
		resource.lock.RUnlock()
		return nil, nil, false
	}
	return resource.resourcePool, resource.lock.RUnlock, true
}

func (p *ipv4PrefixProvider) IsInstanceSupported(instance ec2.EC2Instance) bool {
	// if instance is non-Windows, don't send node event, simply return false
	if instance.Os() != config.OSWindows {
		return false
	}

	instanceName := instance.Name()
	instanceType := instance.Type()
	isNitroInstance, err := utils.IsNitroInstance(instanceType)

	// if instance type is not found in vpc limits, send node event for instance type not supported for Windows
	if errors.Is(err, utils.ErrNotFound) {
		msg := fmt.Sprintf("The instance type %s is not supported for Windows", instanceType)
		utils.SendNodeEventWithNodeName(p.apiWrapper.K8sAPI, instanceName, utils.UnsupportedInstanceTypeReason, msg, v1.EventTypeWarning, p.log)
		return false
	}

	if err == nil && isNitroInstance {
		return true
	}

	// if instance type is non-nitro, PD is not supported
	msg := fmt.Sprintf("The instance type %s is not supported for Windows prefix delegation", instanceType)
	utils.SendNodeEventWithNodeName(p.apiWrapper.K8sAPI, instanceName, utils.UnsupportedInstanceTypeReason, msg, v1.EventTypeWarning, p.log)
	return false
}

func (p *ipv4PrefixProvider) Introspect() interface{} {
	p.lock.RLock()
	defer p.lock.RUnlock()

	response := make(map[string]pool.IntrospectResponse)
	for nodeName, resource := range p.instanceProviderAndPool {
		response[nodeName] = resource.resourcePool.Introspect()
	}
	return response
}

func (p *ipv4PrefixProvider) IntrospectSummary() interface{} {
	p.lock.RLock()
	defer p.lock.RUnlock()

	response := make(map[string]pool.IntrospectSummaryResponse)
	for nodeName, resource := range p.instanceProviderAndPool {
		response[nodeName] = ip.ChangeToIntrospectSummary(resource.resourcePool.Introspect())
	}
	return response
}

func (p *ipv4PrefixProvider) IntrospectNode(node string) interface{} {
	p.lock.RLock()
	defer p.lock.RUnlock()

	resource, found := p.instanceProviderAndPool[node]
	if !found {
		return struct{}{}
	}
	return resource.resourcePool.Introspect()
}

// putInstanceProviderAndPool stores the node's instance provider and pool to the cache
func (p *ipv4PrefixProvider) putInstanceProviderAndPool(nodeName string, resourcePool pool.Pool, manager eni.ENIManager, capacity int,
	isPrevPDEnabled bool, generations ...string) {
	generation := ""
	if len(generations) > 0 {
		generation = generations[0]
	}
	p.putInstanceProviderAndPoolForGeneration(nodeName, resourcePool, manager, capacity, isPrevPDEnabled,
		resourceGeneration{generation: generation})
}

func (p *ipv4PrefixProvider) putInstanceProviderAndPoolForGeneration(nodeName string, resourcePool pool.Pool,
	manager eni.ENIManager, capacity int, isPrevPDEnabled bool, generations ...resourceGeneration,
) {
	var resourceIdentity resourceGeneration
	if len(generations) > 0 {
		resourceIdentity = generations[0]
	}
	resource := &ResourceProviderAndPool{
		eniManager:      manager,
		resourcePool:    resourcePool,
		generation:      resourceIdentity.generation,
		identity:        resourceIdentity.identity,
		capacity:        capacity,
		isPrevPDEnabled: isPrevPDEnabled,
	}
	for {
		previous, found := p.getInstanceProviderAndPool(nodeName)
		if found {
			previous.lock.Lock()
		}
		p.lock.Lock()
		current, stillPresent := p.instanceProviderAndPool[nodeName]
		if (found && current == previous) || (!found && !stillPresent) {
			p.instanceProviderAndPool[nodeName] = resource
			p.lock.Unlock()
			if found {
				previous.lock.Unlock()
			}
			return
		}
		p.lock.Unlock()
		if found {
			previous.lock.Unlock()
		}
	}
}

// getInstanceProviderAndPool returns the node's instance provider and pool from the cache
func (p *ipv4PrefixProvider) getInstanceProviderAndPool(nodeName string) (*ResourceProviderAndPool, bool) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	resource, found := p.instanceProviderAndPool[nodeName]
	return resource, found
}

func (p *ipv4PrefixProvider) lockResourceForJob(job *worker.WarmPoolJob) (*ResourceProviderAndPool, func(), bool) {
	resource, found := p.getInstanceProviderAndPool(job.NodeName)
	if !found {
		return nil, nil, false
	}
	resource.lock.RLock()
	current, stillCurrent := p.getInstanceProviderAndPool(job.NodeName)
	if !stillCurrent || current != resource || resource.generation != job.Generation {
		resource.lock.RUnlock()
		currentGeneration := ""
		if stillCurrent {
			currentGeneration = current.generation
		}
		p.log.Info("ignoring warm pool job from a previous node generation",
			"node", job.NodeName, "jobGeneration", job.Generation,
			"currentGeneration", currentGeneration)
		return nil, nil, false
	}
	return resource, resource.lock.RUnlock, true
}

// deleteInstanceProviderAndPool deletes the node's instance provider and pool from the cache
func (p *ipv4PrefixProvider) deleteInstanceProviderAndPool(nodeName string) {
	resource, found := p.getInstanceProviderAndPool(nodeName)
	if !found {
		return
	}
	resource.lock.Lock()
	defer resource.lock.Unlock()

	p.lock.Lock()
	defer p.lock.Unlock()

	if p.instanceProviderAndPool[nodeName] == resource {
		delete(p.instanceProviderAndPool, nodeName)
	}
}

// getCapacity returns the capacity for IPv4 addresses deconstructed from IPv4 prefixes based on the instance type and the instance os;
func getCapacity(instanceType string, instanceOs string) int {
	// Assign only 1st ENIs non-primary IP
	limits, found := vpc.Limits[instanceType]
	if !found {
		return 0
	}
	var capacity int
	if instanceOs == config.OSWindows {
		capacity = limits.IPv4PerInterface - 1
	} else {
		capacity = (limits.IPv4PerInterface - 1) * limits.Interface
	}

	return capacity
}

func (p *ipv4PrefixProvider) check() healthz.Checker {
	p.log.Info("IPv4 prefix provider's healthz subpath was added")
	return func(req *http.Request) error {
		err := rcHealthz.PingWithTimeout(func(c chan<- error) {
			var ping interface{}
			p.SubmitAsyncJob(ping)
			p.log.V(1).Info("***** health check on IPv4 prefix provider tested SubmitAsyncJob *****")
			c <- nil
		}, p.log)

		return err
	}
}

func (p *ipv4PrefixProvider) GetHealthChecker() healthz.Checker {
	return p.checker
}

// ReconcileNode implements provider.ResourceProvider.
func (*ipv4PrefixProvider) ReconcileNode(nodeName string) bool {
	return false
}
