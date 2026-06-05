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

package org.apache.flink.api.common.state;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class CheckpointListenerRegionalTest {

    @Test
    void testGlobalCheckpointInfoIsGlobal() {
        RegionalCheckpointInfo info = RegionalCheckpointInfo.globalCheckpoint();
        assertThat(info.isGlobalCheckpoint()).isTrue();
        assertThat(info.getFallbackCheckpointSubtasks()).isEmpty();
    }

    @Test
    void testRegionalCheckpointInfoNotGlobal() {
        Map<Long, Set<Integer>> fallback = new HashMap<>();
        fallback.put(99L, Set.of(0, 2));
        RegionalCheckpointInfo info = new RegionalCheckpointInfo(fallback);

        assertThat(info.isGlobalCheckpoint()).isFalse();
        assertThat(info.getFallbackCheckpointSubtasks()).containsKey(99L);
        assertThat(info.getFallbackCheckpointSubtasks().get(99L)).containsExactlyInAnyOrder(0, 2);
    }

    @Test
    void testDefaultMethodDelegatesToOriginal() throws Exception {
        AtomicLong receivedId = new AtomicLong(-1);

        CheckpointListener listener =
                new CheckpointListener() {
                    @Override
                    public void notifyCheckpointComplete(long checkpointId) {
                        receivedId.set(checkpointId);
                    }
                };

        Map<Long, Set<Integer>> fallback = new HashMap<>();
        fallback.put(99L, Set.of(0));
        RegionalCheckpointInfo info = new RegionalCheckpointInfo(fallback);

        listener.notifyCheckpointComplete(100L, info);
        assertThat(receivedId.get()).isEqualTo(100L);
    }

    @Test
    void testOverriddenMethodReceivesRegionalInfo() throws Exception {
        AtomicBoolean wasRegional = new AtomicBoolean(false);

        CheckpointListener listener =
                new CheckpointListener() {
                    @Override
                    public void notifyCheckpointComplete(long checkpointId) {}

                    @Override
                    public void notifyCheckpointComplete(
                            long checkpointId, RegionalCheckpointInfo info) {
                        wasRegional.set(!info.isGlobalCheckpoint());
                    }
                };

        Map<Long, Set<Integer>> fallback = new HashMap<>();
        fallback.put(99L, Set.of(0));
        listener.notifyCheckpointComplete(100L, new RegionalCheckpointInfo(fallback));
        assertThat(wasRegional.get()).isTrue();

        listener.notifyCheckpointComplete(101L, RegionalCheckpointInfo.globalCheckpoint());
        assertThat(wasRegional.get()).isFalse();
    }

    @Test
    void testFallbackMapIsUnmodifiable() {
        Map<Long, Set<Integer>> fallback = new HashMap<>();
        fallback.put(99L, Set.of(0));
        RegionalCheckpointInfo info = new RegionalCheckpointInfo(fallback);

        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> info.getFallbackCheckpointSubtasks().put(98L, Set.of(1)))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
