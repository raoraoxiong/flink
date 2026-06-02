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

import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the regional checkpoint count metric. */
class RegionalCheckpointMetricsTest {

    @Test
    @SuppressWarnings("unchecked")
    void testRegionalCheckpointCounterIncrements() {
        final Map<String, Gauge<?>> registeredGauges = new HashMap<>();
        JobManagerJobMetricGroup metricGroup =
                new UnregisteredMetricGroups.UnregisteredJobManagerJobMetricGroup() {
                    @Override
                    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
                        registeredGauges.put(name, gauge);
                        return gauge;
                    }
                };

        DefaultCheckpointStatsTracker tracker = new DefaultCheckpointStatsTracker(10, metricGroup);

        Gauge<Long> regionalCounter =
                (Gauge<Long>)
                        registeredGauges.get(
                                DefaultCheckpointStatsTracker
                                        .NUMBER_OF_REGIONAL_CHECKPOINTS_METRIC);
        assertThat(regionalCounter).isNotNull();

        // Initially zero
        assertThat(regionalCounter.getValue()).isEqualTo(0L);

        // After one regional checkpoint completes
        tracker.reportRegionalCheckpointCompleted();
        assertThat(regionalCounter.getValue()).isEqualTo(1L);

        // After another regional checkpoint completes
        tracker.reportRegionalCheckpointCompleted();
        assertThat(regionalCounter.getValue()).isEqualTo(2L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testRegionalCheckpointCounterStaysZeroWithoutRegionalCheckpoints() {
        final Map<String, Gauge<?>> registeredGauges = new HashMap<>();
        JobManagerJobMetricGroup metricGroup =
                new UnregisteredMetricGroups.UnregisteredJobManagerJobMetricGroup() {
                    @Override
                    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
                        registeredGauges.put(name, gauge);
                        return gauge;
                    }
                };

        DefaultCheckpointStatsTracker tracker = new DefaultCheckpointStatsTracker(10, metricGroup);

        Gauge<Long> regionalCounter =
                (Gauge<Long>)
                        registeredGauges.get(
                                DefaultCheckpointStatsTracker
                                        .NUMBER_OF_REGIONAL_CHECKPOINTS_METRIC);
        assertThat(regionalCounter).isNotNull();

        // Without any regional checkpoint reported, counter stays at 0
        assertThat(regionalCounter.getValue()).isEqualTo(0L);
    }

    @Test
    void testNoOpTrackerRegionalCheckpoint() {
        // NoOpCheckpointStatsTracker should not throw when called
        NoOpCheckpointStatsTracker.INSTANCE.reportRegionalCheckpointCompleted();
    }
}
