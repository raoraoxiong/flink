---
title: "Regional Checkpoints"
weight: 10
type: docs
aliases:
  - /ops/state/regional_checkpoints.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Regional Checkpoints

## Overview

Regional Checkpoint is an experimental feature that improves checkpoint availability for high-parallelism jobs, particularly those running on expensive GPU resources. When a partial region failure occurs during a checkpoint, the framework generates a logically complete Completed Checkpoint by combining the historical state of the failed regions with the current state of the healthy regions, rather than aborting the entire checkpoint.

This is especially useful for AI data processing workloads (e.g., model inference, multimodal processing) where unstable checkpoints can cause full-graph rollbacks that waste significant computational resources.

## How It Works

Flink's existing checkpoint mechanism requires all tasks to successfully acknowledge before a checkpoint can be completed. With Regional Checkpoint enabled:

1. When a task declines a checkpoint, the CheckpointCoordinator buffers the decline instead of immediately aborting.
2. After all tasks have responded, the coordinator determines which Pipeline Regions have failed.
3. If the failure ratio is within the configured threshold, the coordinator assembles a combined checkpoint:
   - **Healthy regions**: use the current checkpoint state
   - **Failed regions**: use state from the last successful checkpoint
4. OperatorCoordinators that support Regional Checkpoint perform state correction to maintain consistency.
5. Only healthy region tasks receive `notifyCheckpointComplete`.

## Prerequisites

Regional Checkpoint only works with **POINTWISE** topologies (forward, rescale connections). Topologies containing `keyBy`, `rebalance`, `hash`, or `shuffle` edges merge the entire graph into a single Pipeline Region, causing Regional Checkpoint to automatically fall back to standard checkpoint behavior.

## Configuration

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `execution.checkpointing.region.enabled` | Boolean | `false` | Global switch for Regional Checkpoint. |
| `execution.checkpointing.region.max-failure-ratio` | Double | `0.3` | Maximum ratio of failed regions allowed in a single checkpoint. If exceeded, the checkpoint aborts. |
| `execution.checkpointing.region.max-consecutive-failures` | Integer | `2` | Maximum number of consecutive checkpoints that may reference historical state. When exceeded, the next checkpoint is forced to be a global checkpoint. |

### Example

```yaml
execution.checkpointing.region.enabled: true
execution.checkpointing.region.max-failure-ratio: 0.5
execution.checkpointing.region.max-consecutive-failures: 3
```

## Monitoring

### Metrics

| Metric | Description |
|--------|-------------|
| `regionalCheckpointCount` | Cumulative count of completed Regional Checkpoints. |

### REST API

The `/jobs/{jobid}/checkpoints/config` endpoint includes:
- `regional_checkpoint_enabled`
- `regional_max_failure_ratio`
- `regional_max_consecutive_failures`

The `/jobs/{jobid}/checkpoints` endpoint includes:
- `ref_checkpoint_id` per subtask — which historical checkpoint the subtask's state references
- `oldest_ref_checkpoint_id` per task/checkpoint — the oldest referenced checkpoint

### Web UI

The Checkpoint configuration page displays Regional Checkpoint settings. The Checkpoint detail page shows which subtasks reference historical checkpoints.

## Limitations

1. **POINTWISE topologies only**: Jobs with ALL_TO_ALL edges (keyBy, rebalance) form a single region and cannot benefit from this feature.
2. **OperatorCoordinator support**: If an OperatorCoordinator in a failed region does not declare `supportsRegionCheckpoint() = true`, the checkpoint will abort. Flink's built-in `SourceCoordinator` supports Regional Checkpoint.
3. **Consecutive limit**: To prevent unbounded historical checkpoint accumulation, the system forces a global checkpoint after `max-consecutive-failures` consecutive regional checkpoints.
4. **Savepoints**: Savepoint semantics are unchanged — they always require a full-graph snapshot regardless of this setting.

## Checkpoint Cleanup

When Regional Checkpoint is enabled, the checkpoint cleaner ensures that historical checkpoints referenced via `ref_checkpoint_id` are not deleted. The effective retention set includes the most recent N checkpoints plus all transitively referenced historical checkpoints. Once a global checkpoint succeeds (no references), previously referenced checkpoints become eligible for cleanup.

## Troubleshooting

| Symptom | Possible Cause | Resolution |
|---------|---------------|------------|
| Regional Checkpoint never triggers | ALL_TO_ALL topology (single region) | Change to POINTWISE connections or accept standard behavior |
| Checkpoint aborts despite regional enabled | Failure ratio exceeds `max-failure-ratio` | Increase ratio threshold or investigate widespread failures |
| Checkpoint aborts after working initially | Consecutive limit reached | Increase `max-consecutive-failures` or investigate persistent region failures |
| OperatorCoordinator causes abort | Custom coordinator doesn't support regional | Implement `supportsRegionCheckpoint()` in your coordinator |
| Checkpoint storage growing | Referenced historical checkpoints kept | Expected behavior; tune `max-consecutive-failures` to limit depth |
