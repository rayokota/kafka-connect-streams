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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.streams.internals.WrappedPartitionAssignor;
import org.apache.kafka.connect.streams.internals.WrappedPartitionGrouper;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ConnectClientSupplier implements KafkaClientSupplier {
    private DefaultKafkaClientSupplier defaultSupplier = new DefaultKafkaClientSupplier();
    private String connectorName;
    private ConnectStreamsConfig connectStreamsConfig;
    private Map<String, TaskConfig> sourceTaskConfigs;
    private Map<String, TaskConfig> sinkTaskConfigs;

    public ConnectClientSupplier(String connectorName, ConnectStreamsConfig connectStreamsConfig,
                                 Map<String, TaskConfig> sourceTaskConfigs,
                                 Map<String, TaskConfig> sinkTaskConfigs) {
        this.connectorName = connectorName;
        this.connectStreamsConfig = connectStreamsConfig;
        this.sourceTaskConfigs = sourceTaskConfigs;
        this.sinkTaskConfigs = sinkTaskConfigs;
    }

    @Override
    public AdminClient getAdminClient(final Map<String, Object> config) {
        return defaultSupplier.getAdminClient(config);
    }

    @Override
    public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
        ProducerConfig producerConfig = new ProducerConfig(ProducerConfig.addSerializerToConfig(
                config, new ByteArraySerializer(), new ByteArraySerializer()));
        Map<String, ConnectSinkProducer> connectProducers =
                sinkTaskConfigs.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                        e -> ConnectSinkProducer.create(
                                connectorName, connectStreamsConfig, e.getValue(), producerConfig)));
        return new WrappedProducer(connectProducers, defaultSupplier.getProducer(config));
    }

    @Override
    public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
        ConsumerConfig consumerConfig = new ConsumerConfig(ConsumerConfig.addDeserializerToConfig(
                config, new ByteArrayDeserializer(), new ByteArrayDeserializer()));
        Map<String, ConnectSourceConsumer> connectConsumers =
                sourceTaskConfigs.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                        e -> ConnectSourceConsumer.create(
                                connectorName, connectStreamsConfig, e.getValue(), consumerConfig)));

        Map<String, Object> connectorConsumerConfig = new HashMap<>(config);
        connectorConsumerConfig.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, WrappedPartitionAssignor.class.getName());
        connectorConsumerConfig.put(StreamsConfig.PARTITION_GROUPER_CLASS_CONFIG, WrappedPartitionGrouper.class.getName());

        return new WrappedConsumer(connectConsumers, defaultSupplier.getConsumer(connectorConsumerConfig));
    }

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
        return defaultSupplier.getRestoreConsumer(config);
    }

    @Override
    public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
        return defaultSupplier.getGlobalConsumer(config);
    }
}
