# VPC-RC Hydration Instrumentation Plan (Issue #634)

## Why
Issue #634: on large clusters (~1700 nodes), after the VPC Resource Controller restarts it
re-hydrates every node's trunk cache, and `NodeTrunkInitiated` p50/p99 reaches minutes.
The original hypothesis was that `NodeManager.AddNode`'s lock scope serialized node init.

Testing the shrunk-lock change showed **no measurable improvement** in async-queue drain
time or end-to-end node-init latency, while EC2 API throttling was observed. So instead of
more speculative optimization, this patch **measures the hydration pipeline stage-by-stage**
to find the real bottleneck before touching it.

The strategy is a funnel:
1. **Coarse / flow level** — find *which* stage dominates (and definitively answer "is the
   lock the bottleneck?").
2. **Drill-down** — once the hot stage is known, break it into sub-stages.
3. **Decide** — e.g. if EC2 throttling is ~90% of the time, recommend the customer raise EC2
   API limits; if it's only ~50%, find where the other 50% goes and fix that.

## Data flow (what we are measuring)

```
API server informer (restart => LIST all ~1700 nodes)
        │ 1 Request per node
        ▼
[QUEUE 1] node reconcile workqueue  name="node"
        │  --max-node-reconcile=10 goroutines
        │  (controller-runtime already vends workqueue_{queue,work}_duration_seconds)
        ▼
   Reconcile -> AddNode / UpdateNode
        │  cache reads (µs); CreateCNINode (K8s write, ONLY on brand-new nodes);
        │  m.lock = found-check + map write (µs); SubmitJob(Init)
        ▼  (enqueue, µs)  ── the reconcile returns almost immediately
[QUEUE 2] "node async workers"  (pkg/worker, was UNNAMED => no metrics)
        │  --node-mgr-workers=10 goroutines
        ▼
   performAsyncOperation -> InitResources -> InitTrunk
        │  GetRunningPodsOnNode (pod cache)
        │  EC2: Describe / Create+Attach trunk / Describe branch ...  (QPS=12 burst=18)
        ▼  ← RequestLimitExceeded happens here
   NodeTrunkInitiated event  (the latency #634 measures)
```

**Key insight the metrics must confirm:** AddNode itself is cheap (cache reads + enqueue);
the expensive work is in Queue 2's worker pool (EC2). The lock can only matter on a true cold
scale-out where `CreateCNINode` runs inside the critical section — not on restart re-hydration
where CNINodes already exist.

## What already exists (reuse, do NOT re-add)
| Need | Already in repo |
|---|---|
| Queue 1 wait / work duration | `workqueue_queue_duration_seconds{name="node"}`, `workqueue_work_duration_seconds{name="node"}` (controller-runtime) |
| EC2 per-call latency | `aws_nw_call_latency{api=...}` summary (includes retry/backoff wait) |
| EC2 error count | `ec2_api_err_count` + per-API `*_err_count` |
| init_trunk coarse latency | `branch_provider_operation_latency{branch_provider_operation="init_trunk"}` |
| worker job counts | `jobs_{submitted,completed,failed}_count{resource}` |

## What we add

### Stage 1 — coarse (answers "is the lock the bottleneck?")
1. **Name Queue 2.** `pkg/worker/worker.go`: `NewRateLimitingQueue` -> `NewNamedRateLimitingQueue(rl, resourceName)`.
   Free `workqueue_*{name="node async workers"}` (depth, adds, queue/work duration). This is the
   biggest blind spot today (Queue 2 had only counters).
2. **`node_manager_async_queue_wait_seconds{operation}`** (histogram) — `pkg/node/manager/manager.go`.
   Time from `SubmitJob` (enqueue) to worker pickup. operation ∈ {Init, Update, Delete}.
   - High wait + low op duration => worker pool can't keep up (too few workers / backlog).
3. **`node_manager_async_operation_duration_seconds{operation}`** (histogram) — same file.
   Time the worker spends actually running the job. Init (InitResources) and Update
   (UpdateResources) and Delete measured separately.

If Queue 1 wait/work stays low and the time shows up in Queue 2 wait + op duration, the lock
is confirmed NOT the bottleneck and we drill into op duration.

### Stage 2 — drill-down into InitResources / InitTrunk
4. **`branch_provider_init_stage_duration_seconds{stage}`** (histogram) — `pkg/provider/branch/provider.go::InitResource`.
   stages: `total`, `get_running_pods_on_node`, `init_trunk`, `add_trunk_to_cache`,
   `submit_delete_queue_jobs`, `send_node_event`.
5. **`trunk_init_stage_duration_seconds{stage}`** (histogram) — `pkg/provider/branch/trunk/trunk.go::InitTrunk`.
   stages: `total`, `get_instance_network_interfaces`, `wait_trunk_attached`,
   `create_attach_trunk`, `get_branch_network_interfaces`, `rebuild_cache_from_pods`,
   `enqueue_orphan_branch_enis`.

### Stage 3 — EC2 throttle attribution
6. **`ec2_api_throttle_count`** (counter) — `pkg/aws/ec2/api/wrapper.go`.
   Incremented by an observer-only smithy Deserialize middleware (single chokepoint, applies to
   every EC2 call, never alters the request/response). `ec2_api_error_count` reuses the existing
   `ec2_api_err_count`. Combined with `aws_nw_call_latency`, this tells us what fraction of
   InitTrunk time is EC2 throttling/backoff.

### Slow-path logs (not bulk logging)
Each stage above logs a single line **only when the stage exceeds a threshold (10s)**:
`{node, operation/stage, duration}`. Keeps logs quiet on the happy path, loud on the slow path.

## How to read the result (decision tree)
- Queue 1 wait/work high → reconcile path / lock. (We expect this to be LOW.)
- Queue 2 wait high, op duration low → worker pool starved (raise `--node-mgr-workers`).
- op duration high → look at `branch_provider_init_stage_duration_seconds`.
  - `init_trunk` high → `trunk_init_stage_duration_seconds`.
    - EC2 stages high + `ec2_api_throttle_count` high → **EC2 throttling is the bottleneck → tell customer to raise EC2 API limits.**
    - `rebuild_cache_from_pods` high → local cache rebuild (CPU), not EC2.
  - `get_running_pods_on_node` high → pod informer/indexer.
  - `add_trunk_to_cache` high → branch provider `trunkENICache` lock.

## Part 6 — sync to CloudWatch (AWSWesleyK8SMetrics)
The EKS metrics agent scrapes vpc-rc `/metrics` and vends a curated subset. The new histograms
are added to `interestingVPCRCPerformanceMetrics` in
`metrics/networking_controllers_metrics.go` as `bucket: &bucketPoints{}` actions (same pattern
as `workqueue_work_duration_seconds`), p99 per stage/operation.

## Test (matches #634, not 1->1700 scale-out)
Stable ~1000-1700 node cluster → deploy this build with `--enable-profiling` → restart the
vpc-rc Deployment (empty cache, full re-hydration) → collect CloudWatch + Prometheus + pprof
(CPU/goroutine/mutex at T+30s, T+3min, T+8min) + slow logs in one run.
