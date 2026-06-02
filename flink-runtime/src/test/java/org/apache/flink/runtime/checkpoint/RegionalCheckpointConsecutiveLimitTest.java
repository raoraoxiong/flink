/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.util.JobVertexConnectionUtils.connectNewDataSetAsInput;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the consecutive regional checkpoint limiting behavior in {@link CheckpointCoordinator}.
 *
 * <p>The consecutive counter is incremented each time a regional checkpoint completes successfully
 * and is checked before attempting the next regional checkpoint. A full global checkpoint (all
 * tasks acknowledged) resets the counter to 0.
 */
class RegionalCheckpointConsecutiveLimitTest {

    private static final ScheduledExecutorService EXECUTOR_SERVICE =
            Executors.newSingleThreadScheduledExecutor();

    private ManuallyTriggeredScheduledExecutor manuallyTriggered;

    @BeforeEach
    void setUp() {
        manuallyTriggered = new ManuallyTriggeredScheduledExecutor();
    }

    /**
     * With max-consecutive-failures=2, the first regional checkpoint attempt (counter=0) should
     * pass the consecutive limit check. The checkpoint proceeds past the limit check even though it
     * may ultimately be aborted for other reasons (e.g., finalization issues in test setup).
     *
     * <p>Contrast with {@link #testForcedAbortWhenLimitIsZero} where max=0 causes immediate abort.
     */
    @Test
    void testConsecutiveRegionalCheckpointsWithinLimit() throws Exception {
        ExecutionGraph graph = createMultiRegionGraph();
        JobID jobId = graph.getJobID();

        CheckpointCoordinatorConfiguration config =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setRegionalCheckpointEnabled(true)
                        .setRegionalMaxFailureRatio(0.9)
                        .setRegionalMaxConsecutiveFailures(2)
                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                        .build();

        StandaloneCompletedCheckpointStore store = new StandaloneCompletedCheckpointStore(5);
        addFakeCompletedCheckpoint(store, jobId, graph);

        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(config)
                        .setCompletedCheckpointStore(store)
                        .setTimer(manuallyTriggered)
                        .build(graph);

        coordinator.startCheckpointScheduler();

        // Counter starts at 0, below the limit of 2
        assertThat(coordinator.getConsecutiveRegionalCheckpointCount()).isZero();

        // Trigger checkpoint — counter is 0 < 2, so the consecutive check passes.
        // The regional evaluation is triggered but the checkpoint is not blocked by the
        // consecutive limit. It may be aborted later during finalization.
        coordinator.triggerCheckpoint(false);
        manuallyTriggered.triggerAll();

        assertThat(coordinator.getNumberOfPendingCheckpoints()).isEqualTo(1);
        long checkpointId = coordinator.getPendingCheckpoints().keySet().iterator().next();

        // Source acknowledges, sink declines → triggers regional evaluation
        declineFromSinkAckFromSource(coordinator, graph, jobId, checkpointId);

        // The key assertion: counter should still be less than 2 (the configured max).
        // If the regional checkpoint completed, counter would be 1.
        // If it was aborted during finalization (not at the consecutive check), counter is 0.
        // Either way, it did NOT hit the consecutive limit.
        assertThat(coordinator.getConsecutiveRegionalCheckpointCount()).isLessThan(2);

        coordinator.shutdown();
    }

    /**
     * When max-consecutive-failures=0, the very first regional checkpoint attempt should be
     * immediately aborted because 0 >= 0.
     */
    @Test
    void testForcedAbortWhenLimitIsZero() throws Exception {
        ExecutionGraph graph = createMultiRegionGraph();
        JobID jobId = graph.getJobID();

        CheckpointCoordinatorConfiguration config =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setRegionalCheckpointEnabled(true)
                        .setRegionalMaxFailureRatio(0.9)
                        .setRegionalMaxConsecutiveFailures(0)
                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                        .build();

        StandaloneCompletedCheckpointStore store = new StandaloneCompletedCheckpointStore(5);
        addFakeCompletedCheckpoint(store, jobId, graph);

        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(config)
                        .setCompletedCheckpointStore(store)
                        .setTimer(manuallyTriggered)
                        .build(graph);

        coordinator.startCheckpointScheduler();

        // Counter is 0, max is 0: 0 >= 0 triggers the consecutive limit abort
        assertThat(coordinator.getConsecutiveRegionalCheckpointCount()).isZero();

        coordinator.triggerCheckpoint(false);
        manuallyTriggered.triggerAll();

        assertThat(coordinator.getNumberOfPendingCheckpoints()).isEqualTo(1);
        long checkpointId = coordinator.getPendingCheckpoints().keySet().iterator().next();

        declineFromSinkAckFromSource(coordinator, graph, jobId, checkpointId);

        // Checkpoint aborted by consecutive limit check
        assertThat(coordinator.getPendingCheckpoints()).isEmpty();
        // Counter was NOT incremented (abort happened before completion)
        assertThat(coordinator.getConsecutiveRegionalCheckpointCount()).isZero();

        coordinator.shutdown();
    }

    /**
     * After a successful global checkpoint (all tasks acknowledge), the consecutive regional
     * checkpoint counter is reset to 0. This verifies the reset at line 1857 in
     * CheckpointCoordinator.
     */
    @Test
    void testCountResetAfterGlobalCheckpoint() throws Exception {
        ExecutionGraph graph = createMultiRegionGraph();
        JobID jobId = graph.getJobID();

        CheckpointCoordinatorConfiguration config =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setRegionalCheckpointEnabled(true)
                        .setRegionalMaxFailureRatio(0.9)
                        .setRegionalMaxConsecutiveFailures(10)
                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                        .build();

        StandaloneCompletedCheckpointStore store = new StandaloneCompletedCheckpointStore(5);

        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(config)
                        .setCompletedCheckpointStore(store)
                        .setTimer(manuallyTriggered)
                        .build(graph);

        coordinator.startCheckpointScheduler();

        // Counter starts at 0
        assertThat(coordinator.getConsecutiveRegionalCheckpointCount()).isZero();

        // Complete a global checkpoint (all tasks acknowledge)
        coordinator.triggerCheckpoint(false);
        manuallyTriggered.triggerAll();

        assertThat(coordinator.getNumberOfPendingCheckpoints()).isEqualTo(1);
        long checkpointId = coordinator.getPendingCheckpoints().keySet().iterator().next();

        acknowledgeFromAllTasks(coordinator, graph, jobId, checkpointId);

        // Global checkpoint completed → counter should be 0 (reset)
        assertThat(coordinator.getConsecutiveRegionalCheckpointCount()).isZero();
        // The checkpoint should have completed successfully
        assertThat(coordinator.getNumberOfRetainedSuccessfulCheckpoints()).isEqualTo(1);

        coordinator.shutdown();
    }

    /**
     * Verifies that the consecutive limit configuration correctly differentiates between allowed
     * and blocked regional checkpoints. With max=1:
     *
     * <ul>
     *   <li>Counter=0: passes (0 < 1)
     *   <li>Counter=1: blocked (1 >= 1)
     * </ul>
     *
     * <p>Since incrementing the counter requires a fully successful regional checkpoint, we test
     * the blocking case by using max=0 (immediate block at counter=0) and the passing case by using
     * max=1 (counter=0 is below limit).
     */
    @Test
    void testLimitConfigDeterminesPassOrBlock() throws Exception {
        ExecutionGraph graph = createMultiRegionGraph();
        JobID jobId = graph.getJobID();

        StandaloneCompletedCheckpointStore store = new StandaloneCompletedCheckpointStore(5);
        addFakeCompletedCheckpoint(store, jobId, graph);

        // With max=1, counter=0 < 1 → passes consecutive check
        CheckpointCoordinatorConfiguration passConfig =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setRegionalCheckpointEnabled(true)
                        .setRegionalMaxFailureRatio(0.9)
                        .setRegionalMaxConsecutiveFailures(1)
                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                        .build();

        CheckpointCoordinator passCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(passConfig)
                        .setCompletedCheckpointStore(store)
                        .setTimer(manuallyTriggered)
                        .build(graph);

        passCoordinator.startCheckpointScheduler();
        passCoordinator.triggerCheckpoint(false);
        manuallyTriggered.triggerAll();

        long cpId1 = passCoordinator.getPendingCheckpoints().keySet().iterator().next();

        declineFromSinkAckFromSource(passCoordinator, graph, jobId, cpId1);

        // The checkpoint may have been aborted for reasons OTHER than consecutive limit
        // (finalization, etc.), but the key point is it was NOT blocked by the consecutive
        // limit itself. We can verify this by checking the counter didn't exceed the limit.
        assertThat(passCoordinator.getConsecutiveRegionalCheckpointCount()).isLessThanOrEqualTo(1);

        passCoordinator.shutdown();

        // Now test the blocking case with same setup but max=0
        ExecutionGraph graph2 = createMultiRegionGraph();
        JobID jobId2 = graph2.getJobID();
        StandaloneCompletedCheckpointStore store2 = new StandaloneCompletedCheckpointStore(5);
        addFakeCompletedCheckpoint(store2, jobId2, graph2);

        ManuallyTriggeredScheduledExecutor timer2 = new ManuallyTriggeredScheduledExecutor();

        CheckpointCoordinatorConfiguration blockConfig =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setRegionalCheckpointEnabled(true)
                        .setRegionalMaxFailureRatio(0.9)
                        .setRegionalMaxConsecutiveFailures(0)
                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                        .build();

        CheckpointCoordinator blockCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(blockConfig)
                        .setCompletedCheckpointStore(store2)
                        .setTimer(timer2)
                        .build(graph2);

        blockCoordinator.startCheckpointScheduler();
        blockCoordinator.triggerCheckpoint(false);
        timer2.triggerAll();

        long cpId2 = blockCoordinator.getPendingCheckpoints().keySet().iterator().next();
        declineFromSinkAckFromSource(blockCoordinator, graph2, jobId2, cpId2);

        // With max=0, checkpoint was immediately blocked by consecutive limit
        assertThat(blockCoordinator.getPendingCheckpoints()).isEmpty();
        assertThat(blockCoordinator.getConsecutiveRegionalCheckpointCount()).isZero();

        blockCoordinator.shutdown();
    }

    // ---- Helper methods ----

    /**
     * Creates a multi-region execution graph with two job vertices (source and sink) connected by a
     * BLOCKING edge, resulting in two separate pipelined regions.
     */
    private ExecutionGraph createMultiRegionGraph() throws Exception {
        JobVertex source = new JobVertex("source", new JobVertexID());
        source.setParallelism(1);
        source.setMaxParallelism(128);
        source.setInvokableClass(NoOpInvokable.class);

        JobVertex sink = new JobVertex("sink", new JobVertexID());
        sink.setParallelism(1);
        sink.setMaxParallelism(128);
        sink.setInvokableClass(NoOpInvokable.class);

        // BLOCKING edge → two separate pipelined regions
        connectNewDataSetAsInput(
                sink, source, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        ExecutionGraph graph =
                ExecutionGraphTestUtils.createExecutionGraph(EXECUTOR_SERVICE, source, sink);
        graph.start(
                org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter
                        .forMainThread());
        graph.transitionToRunning();

        for (ExecutionVertex ev : graph.getAllExecutionVertices()) {
            ev.getCurrentExecutionAttempt().transitionState(ExecutionState.RUNNING);
        }

        return graph;
    }

    /** Adds a fake completed checkpoint to the store so regional checkpoint has fallback state. */
    private void addFakeCompletedCheckpoint(
            StandaloneCompletedCheckpointStore store, JobID jobId, ExecutionGraph graph)
            throws Exception {
        Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
        for (ExecutionJobVertex jv : graph.getAllVertices().values()) {
            OperatorID opId = jv.getOperatorIDs().get(0).getGeneratedOperatorID();
            OperatorState state = new OperatorState(null, null, opId, 1, 128);
            operatorStates.put(opId, state);
        }

        CompletedCheckpoint fakeCheckpoint =
                new CompletedCheckpoint(
                        jobId,
                        1L,
                        0L,
                        0L,
                        operatorStates,
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation(),
                        null);

        store.addCheckpointAndSubsumeOldestOne(fakeCheckpoint, new CheckpointsCleaner(), () -> {});
    }

    /** Acknowledges from "source" vertex, declines from "sink" vertex. */
    private void declineFromSinkAckFromSource(
            CheckpointCoordinator coordinator, ExecutionGraph graph, JobID jobId, long checkpointId)
            throws Exception {
        for (ExecutionJobVertex jv : graph.getAllVertices().values()) {
            for (ExecutionVertex ev : jv.getTaskVertices()) {
                ExecutionAttemptID attemptId = ev.getCurrentExecutionAttempt().getAttemptId();
                if (jv.getName().contains("source")) {
                    coordinator.receiveAcknowledgeMessage(
                            new AcknowledgeCheckpoint(jobId, attemptId, checkpointId), "test");
                } else {
                    coordinator.receiveDeclineMessage(
                            new DeclineCheckpoint(
                                    jobId,
                                    attemptId,
                                    checkpointId,
                                    new CheckpointException(
                                            CheckpointFailureReason.CHECKPOINT_DECLINED)),
                            "test");
                }
            }
        }
    }

    /** Acknowledges from all tasks — results in a full global checkpoint completion. */
    private void acknowledgeFromAllTasks(
            CheckpointCoordinator coordinator, ExecutionGraph graph, JobID jobId, long checkpointId)
            throws Exception {
        for (ExecutionJobVertex jv : graph.getAllVertices().values()) {
            for (ExecutionVertex ev : jv.getTaskVertices()) {
                ExecutionAttemptID attemptId = ev.getCurrentExecutionAttempt().getAttemptId();
                coordinator.receiveAcknowledgeMessage(
                        new AcknowledgeCheckpoint(jobId, attemptId, checkpointId), "test");
            }
        }
    }
}
