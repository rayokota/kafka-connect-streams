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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class WrappedConsumer implements Consumer<byte[], byte[]> {
    private static final Logger log = LoggerFactory.getLogger(WrappedConsumer.class);

    private final Map<String, ConnectSourceConsumer> connectConsumers;
    private final Consumer<byte[], byte[]> kafkaConsumer;


    public WrappedConsumer(Map<String, ConnectSourceConsumer> connectConsumers, Consumer<byte[], byte[]> kafkaConsumer) {
        this.connectConsumers = connectConsumers;
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public Set<TopicPartition> assignment() {
        return kafkaConsumer.assignment();
    }

    @Override
    public Set<String> subscription() {
        return kafkaConsumer.subscription();
    }

    @Override
    public void subscribe(Collection<String> topics) {
        kafkaConsumer.subscribe(topics);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        kafkaConsumer.subscribe(topics, callback);
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        kafkaConsumer.assign(filterPartitions(partitions));
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        kafkaConsumer.subscribe(pattern, callback);
    }

    @Override
    public void subscribe(Pattern pattern) {
        kafkaConsumer.subscribe(pattern);
    }

    @Override
    public void unsubscribe() {
        kafkaConsumer.unsubscribe();
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(long timeout) {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        poll(kafkaConsumer, timeout, records);
        for (ConnectSourceConsumer connectConsumer : connectConsumers.values()) {
            poll(connectConsumer, timeout, records);
        }
        return new ConsumerRecords<>(records);
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
        return poll(timeout.toMillis());
    }

    private void poll(Consumer<byte[], byte[]> consumer,
                      long timeout,
                      Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records) {
        try {
            ConsumerRecords<byte[], byte[]> rec = consumer.poll(timeout);
            for (TopicPartition tp : rec.partitions()) {
                records.put(tp, rec.records(tp));
            }
        } catch (Exception e) {
            log.error("Could not poll consumer", e);
        }
    }

    @Override
    public void commitSync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitSync(Duration timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        kafkaConsumer.commitSync(filterOffsets(offsets));
        // TODO commit connect
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        kafkaConsumer.commitSync(filterOffsets(offsets), timeout);
        // TODO commit connect
    }

    @Override
    public void commitAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        if (!connectConsumers.containsKey(partition.topic())) {
            kafkaConsumer.seek(partition, offset);
        }
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        kafkaConsumer.seekToBeginning(filterPartitions(partitions));
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        kafkaConsumer.seekToEnd(filterPartitions(partitions));
    }

    @Override
    public long position(TopicPartition partition) {
        return connectConsumers.containsKey(partition.topic()) ? 0L : kafkaConsumer.position(partition);
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        return connectConsumers.containsKey(partition.topic()) ? 0L : kafkaConsumer.position(partition, timeout);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return connectConsumers.containsKey(partition.topic()) ? null : kafkaConsumer.committed(partition);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return connectConsumers.containsKey(partition.topic()) ? null : kafkaConsumer.committed(partition, timeout);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return kafkaConsumer.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return kafkaConsumer.partitionsFor(topic);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return kafkaConsumer.partitionsFor(topic, timeout);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return kafkaConsumer.listTopics();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return kafkaConsumer.listTopics(timeout);
    }

    @Override
    public Set<TopicPartition> paused() {
        return kafkaConsumer.paused();
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        // TODO support?
        //kafkaConsumer.pause(filterPartitions(partitions));
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        // TODO support?
        //kafkaConsumer.resume(filterPartitions(partitions));
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return kafkaConsumer.offsetsForTimes(timestampsToSearch);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
                                                                   Duration timeout) {
        return kafkaConsumer.offsetsForTimes(timestampsToSearch, timeout);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return kafkaConsumer.beginningOffsets(filterPartitions(partitions));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return kafkaConsumer.beginningOffsets(filterPartitions(partitions), timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return kafkaConsumer.endOffsets(filterPartitions(partitions));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return kafkaConsumer.endOffsets(filterPartitions(partitions), timeout);
    }

    @Override
    public void close() {
        kafkaConsumer.close();
        for (ConnectSourceConsumer connectConsumer : connectConsumers.values()) {
            connectConsumer.close();
        }
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        kafkaConsumer.close(timeout, unit);
        for (ConnectSourceConsumer connectConsumer : connectConsumers.values()) {
            connectConsumer.close();
        }
    }

    @Override
    public void close(Duration timeout) {
        kafkaConsumer.close(timeout);
        for (ConnectSourceConsumer connectConsumer : connectConsumers.values()) {
            connectConsumer.close();
        }
    }

    @Override
    public void wakeup() {
    }

    private Map<TopicPartition, OffsetAndMetadata> filterOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        return offsets.entrySet().stream().filter(entry ->
                !connectConsumers.containsKey(entry.getKey().topic())).collect(
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Collection<TopicPartition> filterPartitions(Collection<TopicPartition> partitions) {
        return partitions.stream().filter(tp ->
                !connectConsumers.containsKey(tp.topic())).collect(Collectors.toList());
    }
}
