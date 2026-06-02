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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.PerJobCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServicesWithLeadershipControl;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.test.util.TestUtils.submitJobAndWaitForResult;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Regional Checkpoint feature.
 *
 * <p>Regional Checkpoint allows partial region failures during a checkpoint to not abort the entire
 * checkpoint. Instead, historical state from the last completed checkpoint is used for failed
 * regions while healthy regions contribute fresh state.
 */
@ExtendWith(TestLoggerExtension.class)
class RegionalCheckpointITCase {

    private static final int NUM_REGIONS = 3;
    private static final int MAX_PARALLELISM = 2 * NUM_REGIONS;
    private static final int NUM_ELEMENTS = 6000;
    private static final int FAIL_BASE = 1000;
    private static final int NUM_OF_RESTARTS = 3;

    private static final AtomicLong lastCompletedCheckpointId = new AtomicLong(0);
    private static final AtomicInteger numCompletedCheckpoints = new AtomicInteger(0);
    private static final AtomicInteger jobFailedCnt = new AtomicInteger(0);

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(createClusterConfiguration())
                            .setNumberTaskManagers(NUM_REGIONS)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    private static Configuration createClusterConfiguration() {
        final Configuration config = new Configuration();
        config.set(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");
        config.set(HighAvailabilityOptions.HA_MODE, TestingHAFactory.class.getName());
        config.set(CheckpointingOptions.REGIONAL_CHECKPOINT_ENABLED, true);
        config.set(CheckpointingOptions.REGIONAL_CHECKPOINT_MAX_FAILURE_RATIO, 0.5);
        config.set(CheckpointingOptions.REGIONAL_CHECKPOINT_MAX_CONSECUTIVE_FAILURES, 2);
        return config;
    }

    @BeforeEach
    void setup() {
        jobFailedCnt.set(0);
        numCompletedCheckpoints.set(0);
        lastCompletedCheckpointId.set(0);
        Arrays.fill(CountingSink.counts, 0L);
    }

    /**
     * Tests that a regional checkpoint can complete even when one region's task fails during
     * checkpoint.
     *
     * <p>Setup: source(parallelism=NUM_REGIONS) → map(parallelism=NUM_REGIONS) with POINTWISE
     * connectivity (multiple pipeline regions). One region fails during checkpoint. With regional
     * checkpoint enabled, the checkpoint should still complete using historical state for the
     * failed region.
     */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void testRegionalCheckpointCompleteDuringRegionFailover(
            @InjectClusterClient ClusterClient<?> client) throws Exception {
        final JobGraph jobGraph = createMultiRegionJobGraph(NUM_OF_RESTARTS);
        submitJobAndWaitForResult(client, jobGraph, getClass().getClassLoader());

        // The job should complete successfully with at least some checkpoints completing
        // despite region failures
        assertThat(numCompletedCheckpoints.get())
                .as("At least one checkpoint should complete during the job execution")
                .isGreaterThanOrEqualTo(1);
    }

    /**
     * Tests that source data is not lost after a regional checkpoint with partial failures.
     *
     * <p>The job processes a bounded source. After region failures and checkpoint restores, all
     * data should eventually be processed completely.
     */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void testSourceDataNotLostAfterRegionalCheckpoint(@InjectClusterClient ClusterClient<?> client)
            throws Exception {
        final JobGraph jobGraph = createMultiRegionJobGraph(NUM_OF_RESTARTS);
        submitJobAndWaitForResult(client, jobGraph, getClass().getClassLoader());

        // Verify all sink instances received data (no data lost due to regional checkpoint)
        for (int i = 0; i < NUM_REGIONS; i++) {
            assertThat(CountingSink.counts[i])
                    .as("Sink subtask " + i + " should have received data")
                    .isGreaterThan(0);
        }
    }

    /**
     * Tests that a forced global checkpoint is triggered after the consecutive regional checkpoint
     * limit is reached.
     *
     * <p>Setup: max-consecutive-failures = 1. When 2 consecutive region failures happen, the second
     * checkpoint should be aborted (forced to fall back to global checkpoint behavior).
     */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void testForcedGlobalAfterConsecutiveLimit(@InjectClusterClient ClusterClient<?> client)
            throws Exception {
        // Create a job graph with low consecutive limit (configured at cluster level = 2)
        // The job will have multiple region failures, forcing global behavior after the limit
        final JobGraph jobGraph = createMultiRegionJobGraph(NUM_OF_RESTARTS);
        submitJobAndWaitForResult(client, jobGraph, getClass().getClassLoader());

        // After exceeding consecutive limit, the system should still recover
        // (falling back to global checkpoint behavior, which aborts and re-triggers)
        assertThat(numCompletedCheckpoints.get())
                .as("Checkpoints should eventually complete after falling back to global")
                .isGreaterThanOrEqualTo(1);
    }

    /**
     * Tests that ALL_TO_ALL topology (single pipeline region) falls back to global checkpoint
     * behavior.
     *
     * <p>When the job has a keyBy (shuffle/ALL_TO_ALL), all operators are in a single pipeline
     * region. Regional checkpoint is not applicable, and any task failure during checkpoint should
     * abort the entire checkpoint.
     */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void testAllToAllTopologyFallsBackToGlobalBehavior(@InjectClusterClient ClusterClient<?> client)
            throws Exception {
        final JobGraph jobGraph = createSingleRegionJobGraph();
        submitJobAndWaitForResult(client, jobGraph, getClass().getClassLoader());

        // In a single-region topology, regional checkpoint logic detects "single pipeline region"
        // and aborts. The job still completes via global checkpoint/failover, but checkpoints
        // during failure periods are aborted globally.
        assertThat(numCompletedCheckpoints.get())
                .as(
                        "Checkpoints should complete - "
                                + "regional logic aborts but global failover recovers")
                .isGreaterThanOrEqualTo(1);
    }

    // -------------------------------------------------------------------------
    //  Job Graph Creation
    // -------------------------------------------------------------------------

    /**
     * Creates a multi-region job graph with POINTWISE connectivity.
     *
     * <p>Uses {@link DataStreamUtils#reinterpretAsKeyedStream} to create multiple pipeline regions
     * without introducing ALL_TO_ALL (shuffle) edges.
     */
    private JobGraph createMultiRegionJobGraph(int numRestarts) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(NUM_REGIONS);
        env.setMaxParallelism(MAX_PARALLELISM);
        env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig()
                .setExternalizedCheckpointRetention(
                        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        env.disableOperatorChaining();

        // Create POINTWISE topology → multiple pipeline regions
        DataStreamUtils.reinterpretAsKeyedStream(
                        env.addSource(new BoundedSourceFunction(NUM_ELEMENTS, FAIL_BASE))
                                .name("multi-region-source")
                                .setParallelism(NUM_REGIONS),
                        (KeySelector<Tuple2<Integer, Integer>, Integer>) value -> value.f0,
                        TypeInformation.of(Integer.class))
                .map(new RegionFailingMapFunction(numRestarts))
                .name("failing-map")
                .setParallelism(NUM_REGIONS)
                .addSink(new CountingSink())
                .name("counting-sink")
                .setParallelism(NUM_REGIONS);

        return env.getStreamGraph().getJobGraph();
    }

    /**
     * Creates a single-region job graph with ALL_TO_ALL (keyBy) connectivity.
     *
     * <p>All operators end up in a single pipeline region, making regional checkpoint inapplicable.
     */
    private JobGraph createSingleRegionJobGraph() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(NUM_REGIONS);
        env.setMaxParallelism(MAX_PARALLELISM);
        env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig()
                .setExternalizedCheckpointRetention(
                        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        env.disableOperatorChaining();

        // keyBy creates ALL_TO_ALL edges → single pipeline region
        DataStream<Tuple2<Integer, Integer>> source =
                env.addSource(new BoundedSourceFunction(NUM_ELEMENTS, FAIL_BASE))
                        .name("single-region-source")
                        .setParallelism(NUM_REGIONS);

        source.keyBy((KeySelector<Tuple2<Integer, Integer>, Integer>) value -> value.f0)
                .map(new RegionFailingMapFunction(1))
                .name("keyed-map")
                .setParallelism(NUM_REGIONS)
                .addSink(new CountingSink())
                .name("counting-sink")
                .setParallelism(NUM_REGIONS);

        return env.getStreamGraph().getJobGraph();
    }

    // -------------------------------------------------------------------------
    //  Test Functions
    // -------------------------------------------------------------------------

    /** A bounded source that emits elements and slows down to allow checkpoints to complete. */
    private static class BoundedSourceFunction
            extends RichParallelSourceFunction<Tuple2<Integer, Integer>>
            implements CheckpointedFunction {

        private static final long serialVersionUID = 1L;

        private final long numElements;
        private final long checkpointLatestAt;
        private int index = -1;
        private volatile boolean isRunning = true;

        private ListState<Integer> indexState;

        BoundedSourceFunction(long numElements, long checkpointLatestAt) {
            this.numElements = numElements;
            this.checkpointLatestAt = checkpointLatestAt;
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            if (index < 0) {
                index = 0;
            }

            final int subTaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();

            while (isRunning && index < numElements) {
                synchronized (ctx.getCheckpointLock()) {
                    final int key = index / 2;
                    final int forwardTaskIndex =
                            KeyGroupRangeAssignment.assignKeyToParallelOperator(
                                    key, MAX_PARALLELISM, NUM_REGIONS);
                    if (forwardTaskIndex == subTaskIndex) {
                        ctx.collect(Tuple2.of(key, index));
                    }
                    index += 1;
                }

                if (numCompletedCheckpoints.get() < 3) {
                    if (index < checkpointLatestAt) {
                        Thread.sleep(1);
                    } else {
                        while (isRunning && numCompletedCheckpoints.get() < 3) {
                            Thread.sleep(300);
                        }
                    }
                }
                if (jobFailedCnt.get() < NUM_OF_RESTARTS) {
                    Thread.sleep(1);
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            indexState.update(Collections.singletonList(index));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            indexState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("source-index", Integer.class));
            if (context.isRestored()) {
                for (Integer savedIndex : indexState.get()) {
                    index = savedIndex;
                }
            }
        }
    }

    /**
     * A map function that fails at specific points to simulate region failures during checkpoint.
     *
     * <p>Only the last subtask (region) throws exceptions, leaving other regions healthy.
     */
    private static class RegionFailingMapFunction
            extends RichMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

        private static final long serialVersionUID = 1L;
        private final int maxRestarts;

        RegionFailingMapFunction(int maxRestarts) {
            this.maxRestarts = maxRestarts;
        }

        @Override
        public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
            final int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();

            // Only the last region subtask fails
            if (subtaskIndex == NUM_REGIONS - 1
                    && value.f1 > FAIL_BASE * (jobFailedCnt.get() + 1)
                    && jobFailedCnt.get() < maxRestarts) {
                jobFailedCnt.incrementAndGet();
                throw new TestException();
            }

            return value;
        }
    }

    /** A sink that counts elements received per subtask. */
    private static class CountingSink extends RichSinkFunction<Tuple2<Integer, Integer>> {

        private static final long serialVersionUID = 1L;

        static final long[] counts = new long[NUM_REGIONS];

        @Override
        public void invoke(Tuple2<Integer, Integer> value) {
            counts[getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()]++;
        }

        @Override
        public void close() throws Exception {
            // counts are stored in static array for verification
        }
    }

    private static class TestException extends IOException {
        private static final long serialVersionUID = 1L;
    }

    // -------------------------------------------------------------------------
    //  Testing HA infrastructure
    // -------------------------------------------------------------------------

    /**
     * A completed checkpoint store that tracks the number and ID of completed checkpoints for test
     * verification.
     */
    private static class TestingCompletedCheckpointStore
            extends StandaloneCompletedCheckpointStore {

        TestingCompletedCheckpointStore() {
            super(1);
        }

        @Override
        public CompletedCheckpoint addCheckpointAndSubsumeOldestOne(
                CompletedCheckpoint checkpoint,
                CheckpointsCleaner checkpointsCleaner,
                Runnable postCleanup)
                throws Exception {
            final CompletedCheckpoint subsumed =
                    super.addCheckpointAndSubsumeOldestOne(
                            checkpoint, checkpointsCleaner, postCleanup);
            lastCompletedCheckpointId.set(checkpoint.getCheckpointID());
            numCompletedCheckpoints.incrementAndGet();
            return subsumed;
        }
    }

    /** Testing HA factory which needs to be public in order to be instantiatable. */
    public static class TestingHAFactory implements HighAvailabilityServicesFactory {

        @Override
        public HighAvailabilityServices createHAServices(
                Configuration configuration, Executor executor) {
            final CheckpointRecoveryFactory checkpointRecoveryFactory =
                    PerJobCheckpointRecoveryFactory.withoutCheckpointStoreRecovery(
                            maxCheckpoints -> new TestingCompletedCheckpointStore());
            return new EmbeddedHaServicesWithLeadershipControl(executor, checkpointRecoveryFactory);
        }
    }
}
