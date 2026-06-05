/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Provides context about a regional checkpoint, allowing listeners to understand which subtasks
 * used historical state from a previous checkpoint rather than the current one.
 *
 * <p>When a Regional Checkpoint completes, some subtasks may have failed to acknowledge and their
 * state is replaced by state from a previous successful checkpoint. This class carries the mapping
 * from fallback checkpoint IDs to the set of subtask indices whose state originates from that
 * historical checkpoint.
 *
 * <p>For a global checkpoint (all tasks acknowledged successfully), the {@link
 * #fallbackCheckpointSubtasks} map is empty, and {@link #isGlobalCheckpoint()} returns {@code
 * true}.
 */
@PublicEvolving
public class RegionalCheckpointInfo {

    /** Singleton instance representing a global checkpoint (no fallback subtasks). */
    private static final RegionalCheckpointInfo GLOBAL =
            new RegionalCheckpointInfo(Collections.emptyMap());

    /**
     * Mapping from fallback checkpointId to the set of subtask indices whose state originates from
     * that historical checkpoint rather than the current one.
     */
    private final Map<Long, Set<Integer>> fallbackCheckpointSubtasks;

    public RegionalCheckpointInfo(Map<Long, Set<Integer>> fallbackCheckpointSubtasks) {
        this.fallbackCheckpointSubtasks = Collections.unmodifiableMap(fallbackCheckpointSubtasks);
    }

    /** Returns a {@link RegionalCheckpointInfo} representing a global checkpoint. */
    public static RegionalCheckpointInfo globalCheckpoint() {
        return GLOBAL;
    }

    /**
     * Returns {@code true} if this is a global checkpoint where all tasks acknowledged
     * successfully.
     */
    public boolean isGlobalCheckpoint() {
        return fallbackCheckpointSubtasks.isEmpty();
    }

    /**
     * Returns the mapping from fallback checkpoint IDs to the set of subtask indices whose state
     * originates from that historical checkpoint.
     *
     * <p>For example, if subtasks 0 and 2 fell back to checkpoint 99, the map would contain {@code
     * {99 -> [0, 2]}}.
     */
    public Map<Long, Set<Integer>> getFallbackCheckpointSubtasks() {
        return fallbackCheckpointSubtasks;
    }
}
