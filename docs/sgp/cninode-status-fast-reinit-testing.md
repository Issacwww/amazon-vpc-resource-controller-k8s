# CNINode Status Fast Reinit Test Handoff

## Branch

- Base branch: `release-1.7.16-onwards`
- Test branch: `codex/cninode-trunk-fast-reinit`
- Commit with code change: `72f8172 Persist trunk init state in CNINode status`

## Problem Scope

This change targets VPCRC restart or leader transition when nodes and trunk ENIs already exist.

The original slow path rebuilds node/trunk state by synchronously calling EC2 for each node during initialization. At high node counts this can be throttled by EC2 API rate limits, so `NodeTrunkInitiated` events are delayed after controller restart.

This is not primarily a new-node scale-up optimization. New nodes do not have a persisted CNINode status snapshot yet, so they still follow the existing EC2 initialization path. New-node scale-up should still be measured, but it is a secondary regression check rather than the main expected win.

## Core Change

The controller now persists enough EC2/trunk state in `CNINode.status` after a successful slow EC2 initialization:

- Instance ID, instance type, subnet IDs/CIDRs, masks, primary ENI ID, security groups.
- Primary ENI connection tracking settings.
- Trunk ENI ID, subnet, and security groups.
- `snapshotVersion` so future schema changes can safely fall back.

On restart, node initialization first tries to hydrate the in-memory EC2 instance object from `CNINode.status`.

If that succeeds, the branch provider initializes the trunk cache from:

- `CNINode.status.trunkENI.id`
- Running pods on the node
- Existing pod ENI annotations

That path avoids synchronous EC2 trunk/branch discovery before declaring the node trunk initialized.

If the status is missing, stale, invalid, or mismatched with current custom networking settings, the code falls back to the original EC2 path.

After a successful status fast path, the controller submits a background worker job that calls EC2 once to discover branch ENIs not owned by any pod annotation and pushes those orphan branch ENIs to the delete queue. This keeps restart fast while preserving cleanup behavior.

## Expected Behavior

Expected improvement:

- Existing nodes with valid CNINode status and already attached trunk ENIs should reach `NodeTrunkInitiated` much faster after VPCRC restart.
- The improvement should be most visible at high node counts, where the old path was EC2 API throttled.

No guaranteed improvement:

- New node scale-up from 0 nodes, because there is no CNINode status snapshot before first initialization.
- Nodes whose CNINode status is absent or invalid.
- Nodes whose custom networking subnet/security groups changed after the snapshot.
- First rollout of this branch, until each node has completed one slow-path initialization and written status.

Correctness guarantees to check:

- Existing SGP pods keep their pod ENI annotations usable after controller restart.
- New SGP pod creation still succeeds after fast-path reinit.
- Deleted pods still release branch ENIs through the normal delete queue.
- Orphan branch ENIs without pod annotations are discovered by the background reconcile.

## Metrics

For dataplane VPCRC, metrics are exposed from the controller pod on `:8443/metrics`.

Example access methods:

```bash
kubectl -n kube-system get pods -l app=vpc-resource-controller -o wide
kubectl -n kube-system port-forward deploy/vpc-resource-controller 8443:8443
curl -s http://127.0.0.1:8443/metrics
```

If the deployment label differs, inspect the dataplane manifest and port-forward the controller pod directly:

```bash
kubectl -n kube-system port-forward pod/<controller-pod-name> 8443:8443
curl -s http://127.0.0.1:8443/metrics
```

New metrics added by this branch:

- `cninode_status_fast_path_total{result,reason}`
  - Shows whether the branch provider used the CNINode status fast path.
  - Expected good signal: `result="hit",reason="status_valid"` increases on restart.
  - Miss reasons include `trunk_status_invalid` and `get_cninode_error`.

- `trunk_cache_rebuild_latency{path,result}`
  - Summary metric for trunk cache rebuild time.
  - Compare `path="cninode_status",result="success"` vs `path="ec2",result="success"`.
  - This is the most direct white-box latency metric for the optimization.

- `cninode_status_background_reconcile_total{result}`
  - Counts background EC2 orphan-branch reconcile jobs after a status fast-path init.
  - Expected good signal: `result="success"` increases after restart.

Existing metrics still useful:

- `branch_provider_operation_latency{branch_provider_operation="init_trunk",resource_count="1"}`
  - Existing end-to-end init trunk latency observed by the branch provider.

- `branch_provider_operations_err_count`
  - Watch for increases in `init`, `load_instance_details_fallback`, `update_cninode_status`, or `reconcile_unassigned_branch_enis`.

Also inspect status directly:

```bash
kubectl get cninode <node-name> -o jsonpath='{.status.snapshotVersion}{"\n"}{.status.trunkENI.id}{"\n"}'
kubectl get cninode <node-name> -o yaml
```

Expected after one successful slow-path init:

- `.status.snapshotVersion` is `v1`
- `.status.instance.instanceID` is set
- `.status.instance.instanceType` is set
- `.status.trunkENI.id` is set

## Baseline Plan

Run baseline with the current release image before applying this branch.

Baseline does not have the new CNINode status metrics, so use event timestamps and existing metrics.

Main test: existing-node VPCRC restart.

1. Deploy dataplane VPCRC from `release-1.7.16-onwards`.
2. Scale or create the target node count, for example 0 -> 1500.
3. Run enough SGP pods to create representative branch ENI state.
4. Wait until all nodes have trunk initialized.
5. Restart only the dataplane VPCRC deployment.
6. Record controller ready or leader acquisition timestamp as T0.
7. Collect all `NodeTrunkInitiated` events after T0.
8. Compute p50, p90, p95, p99, max, and total time from T0 to `NodeTrunkInitiated`.
9. Also collect pod creation -> pod ENI annotation latency if pods are created during/after restart.

Secondary test: new-node scale-up.

1. Start with controller ready.
2. Scale 0 -> N or N -> N+M nodes.
3. Record node creation timestamp -> `NodeTrunkInitiated`.
4. Record pod creation timestamp -> pod ENI annotation.
5. Treat this as regression coverage; this branch is not expected to significantly improve this path.

## Fixed-Branch Plan

For this branch, do one warm-up pass before measuring restart improvement.

1. Deploy dataplane VPCRC with the fixed image.
2. Let all existing nodes initialize once.
3. Confirm CNINode status exists for sampled nodes.
4. Restart dataplane VPCRC.
5. Record the same event-based timings as baseline.
6. Scrape metrics during and after restart.
7. Confirm `cninode_status_fast_path_total{result="hit",reason="status_valid"}` increases.
8. Confirm `trunk_cache_rebuild_latency{path="cninode_status",result="success"}` has lower latency than the EC2 path.
9. Confirm no unexpected increase in `branch_provider_operations_err_count`.

Use the same cluster shape, node count, SGP pod shape, instance families, and controller worker/QPS settings for baseline and fixed runs.

## Pass Criteria

Primary success criteria:

- Existing-node restart p95/p99 and total time to all `NodeTrunkInitiated` events improves materially versus baseline.
- Fast-path hit count is close to the number of nodes with valid trunk status.
- SGP pod annotation and deletion behavior remains correct after restart.

Secondary success criteria:

- New-node scale-up is not slower than baseline beyond expected run-to-run noise.
- Background reconcile succeeds and does not produce a sustained error counter increase.

## Notes For The Tester

The first deployment of this branch may not show the full benefit immediately. The benefit appears after the branch has written CNINode status snapshots, then the controller is restarted.

If `cninode_status_fast_path_total{result="hit"}` is low, inspect sampled CNINodes first. Missing or empty `.status.trunkENI.id` means those nodes will intentionally fall back to EC2.

If custom networking is enabled, status hydration requires the persisted current subnet/security groups to match the current ENIConfig-derived settings. Mismatches intentionally fall back to EC2 for safety.

For managed EKS control-plane VPCRC, these controller metrics are not directly accessible externally. Dataplane deployment is the recommended white-box validation path. For black-box validation in managed control plane, rely on Kubernetes events, pod annotation timestamps, and CloudTrail/EC2 API volume.
