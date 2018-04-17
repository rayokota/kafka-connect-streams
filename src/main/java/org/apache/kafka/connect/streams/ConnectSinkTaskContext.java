/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.streams;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ConnectSinkTaskContext implements SinkTaskContext {
    private Map<TopicPartition, Long> offsets;
    private long timeoutMs;
    private boolean commitRequested;

    public ConnectSinkTaskContext() {
        this.offsets = new HashMap<>();
        this.timeoutMs = -1L;
    }

    @Override
    public void offset(Map<TopicPartition, Long> offsets) {
        this.offsets.putAll(offsets);
    }

    @Override
    public void offset(TopicPartition tp, long offset) {
        offsets.put(tp, offset);
    }

    public void clearOffsets() {
        offsets.clear();
    }

    /**
     * Get offsets that the SinkTask has submitted to be reset. Used by the Kafka Connect framework.
     *
     * @return the map of offsets
     */
    public Map<TopicPartition, Long> offsets() {
        return offsets;
    }

    @Override
    public void timeout(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    /**
     * Get the timeout in milliseconds set by SinkTasks. Used by the Kafka Connect framework.
     *
     * @return the backoff timeout in milliseconds.
     */
    public long timeout() {
        return timeoutMs;
    }

    @Override
    public Set<TopicPartition> assignment() {
        // TODO unsupported
        throw new UnsupportedOperationException();
    }

    @Override
    public void pause(TopicPartition... partitions) {
        // TODO unsupported
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume(TopicPartition... partitions) {
        // TODO unsupported
        throw new UnsupportedOperationException();
    }

    @Override
    public void requestCommit() {
        commitRequested = true;
    }

    public boolean isCommitRequested() {
        return commitRequested;
    }

    public void clearCommitRequest() {
        commitRequested = false;
    }

}
