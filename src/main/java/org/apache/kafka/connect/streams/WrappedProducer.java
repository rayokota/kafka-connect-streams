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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class WrappedProducer implements Producer<byte[], byte[]> {
    private static final Logger log = LoggerFactory.getLogger(WrappedProducer.class);

    private final Map<String, ConnectSinkProducer> connectProducers;
    private final Producer<byte[], byte[]> kafkaProducer;

    public WrappedProducer(Map<String, ConnectSinkProducer> connectProducers,
                           Producer<byte[], byte[]> kafkaProducer) {
        this.connectProducers = connectProducers;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void initTransactions() {
        kafkaProducer.initTransactions();
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        kafkaProducer.beginTransaction();
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         String consumerGroupId) throws ProducerFencedException {
        kafkaProducer.sendOffsetsToTransaction(filterOffsets(offsets), consumerGroupId);
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        kafkaProducer.commitTransaction();
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        kafkaProducer.abortTransaction();
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
        return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
        String topic = record.topic();
        ConnectSinkProducer connectProducer = connectProducers.get(topic);
        if (connectProducer != null) {
            return connectProducer.send(record, callback);
        } else {
            return kafkaProducer.send(record, callback);
        }
    }

    @Override
    public void flush() {
        // TODO handle ex
        kafkaProducer.flush();
        for (ConnectSinkProducer connectProducer : connectProducers.values()) {
            connectProducer.flush();
        }
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return kafkaProducer.partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return kafkaProducer.metrics();
    }

    @Override
    public void close() {
        // TODO handle ex
        kafkaProducer.close();
        for (ConnectSinkProducer connectProducer : connectProducers.values()) {
            connectProducer.close();
        }
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        close();
    }

    private Map<TopicPartition, OffsetAndMetadata> filterOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        return offsets.entrySet().stream().filter(entry ->
                !connectProducers.containsKey(entry.getKey().topic())).collect(
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
