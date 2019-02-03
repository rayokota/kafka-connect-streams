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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.runtime.TransformationChain;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.util.ConnectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.utils.Time.SYSTEM;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_RETRY_MAX_DELAY_DEFAULT;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_RETRY_TIMEOUT_DEFAULT;
import static org.apache.kafka.connect.runtime.errors.ToleranceType.NONE;

public class ConnectSinkProducer implements Producer<byte[], byte[]> {
    private static final Logger log = LoggerFactory.getLogger(ConnectSinkProducer.class);

    private static final RetryWithToleranceOperator NOOP_OPERATOR = new RetryWithToleranceOperator(
            ERRORS_RETRY_TIMEOUT_DEFAULT, ERRORS_RETRY_MAX_DELAY_DEFAULT, NONE, SYSTEM);

    private final ProducerConfig config;
    private final SinkTask task;
    private Map<String, String> taskConfig;
    private final Time time;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final HeaderConverter headerConverter;
    private final TransformationChain<SinkRecord> transformationChain;
    private ConnectSinkTaskContext context;
    private final List<SinkRecord> recordBatch;
    private Map<TopicPartition, OffsetAndMetadata> lastCommittedOffsets;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets;
    private final Map<TopicPartition, OffsetAndMetadata> origOffsets;

    // TODO use
    private RuntimeException rebalanceException;
    private int commitSeqno;
    private long commitStarted;
    private int commitFailures;
    private boolean pausedForRedelivery;
    private boolean committing;

    public static ConnectSinkProducer create(String connectorName, ConnectStreamsConfig connectStreamsConfig,
                                             TaskConfig taskConfig, ProducerConfig producerConfig) {
        Converter keyConverter = Utils.newConverter(connectStreamsConfig, ConnectStreamsConfig.KEY_CONVERTER_CLASS_CONFIG);
        Converter valueConverter = Utils.newConverter(connectStreamsConfig, ConnectStreamsConfig.VALUE_CONVERTER_CLASS_CONFIG);
        HeaderConverter headerConverter = Utils.newHeaderConverter(connectStreamsConfig, ConnectStreamsConfig.HEADER_CONVERTER_CLASS_CONFIG);

        Class<? extends SinkTask> taskClass = taskConfig.getClass(TaskConfig.TASK_CLASS_CONFIG).asSubclass(SinkTask.class);
        SinkTask sinkTask = Utils.newInstance(taskClass);
        ConnectSinkProducer producer = new ConnectSinkProducer(
                producerConfig,
                sinkTask,
                keyConverter,
                valueConverter,
                headerConverter,
                new TransformationChain<>(Collections.emptyList(), NOOP_OPERATOR),
                null,
                new SystemTime());
        producer.initialize(taskConfig);
        producer.start();
        return producer;
    }

    public ConnectSinkProducer(ProducerConfig config,
                               SinkTask task,
                               Converter keyConverter,
                               Converter valueConverter,
                               HeaderConverter headerConverter,
                               TransformationChain<SinkRecord> transformationChain,
                               ClassLoader loader,
                               Time time) {
        this.config = config;
        this.task = task;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.headerConverter = headerConverter;
        this.transformationChain = transformationChain;
        this.time = time;
        this.recordBatch = new ArrayList<>();
        this.currentOffsets = new HashMap<>();
        this.origOffsets = new HashMap<>();
        this.pausedForRedelivery = false;
        this.rebalanceException = null;
        this.committing = false;
        this.commitSeqno = 0;
        this.commitStarted = -1;
        this.commitFailures = 0;
    }

    public void initialize(TaskConfig taskConfig) {
        try {
            this.taskConfig = taskConfig.originalsStrings();
            this.context = new ConnectSinkTaskContext();
        } catch (Throwable t) {
            log.error("{} Task failed initialization and will not be started.", this, t);
        }
    }

    public void start() {
        task.initialize(context);
        task.start(taskConfig);
        log.info("{} Sink task finished initialization and start", this);
    }

    @Override
    public void initTransactions() {
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         String consumerGroupId) throws ProducerFencedException {
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
        return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
        convertRecords(Collections.singletonList(record));
        System.out.println("*** ConnectSinkProducer: send");
        // TODO callback
        return null; // TODO
    }

    @Override
    public void flush() {
        deliverRecords();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public void close() {
        task.stop();
        transformationChain.close();
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        close();
    }

    private void convertRecords(List<ProducerRecord<byte[], byte[]>> records) {
        origOffsets.clear();
        for (ProducerRecord<byte[], byte[]> record : records) {
            log.trace("{} Converting message in topic '{}' partition {} and timestamp {}",
                    this, record.topic(), record.partition(), record.timestamp());
            SchemaAndValue keyAndSchema = record.key() != null
                    ? keyConverter.toConnectData(record.topic(), record.key())
                    : SchemaAndValue.NULL;
            SchemaAndValue valueAndSchema = record.value() != null
                    ? valueConverter.toConnectData(record.topic(), record.value())
                    : SchemaAndValue.NULL;
            int partition = record.partition() != null ? record.partition() : 0;
            Long timestamp = ConnectUtils.checkAndConvertTimestamp(record.timestamp());
            Headers headers = convertHeadersFor(record);
            SinkRecord origRecord = new SinkRecord(record.topic(), partition,
                    keyAndSchema.schema(), keyAndSchema.value(),
                    valueAndSchema.schema(), valueAndSchema.value(),
                    0L,  // TODO
                    timestamp,
                    TimestampType.NO_TIMESTAMP_TYPE,  // TODO
                    headers);
            log.trace("{} Applying transformations to record in topic '{}' partition {} and timestamp {} with key {} and value {}",
                    this, record.topic(), record.partition(), timestamp, keyAndSchema.value(), valueAndSchema.value());
            SinkRecord transRecord = transformationChain.apply(origRecord);
            origOffsets.put(
                    new TopicPartition(origRecord.topic(), origRecord.kafkaPartition()),
                    new OffsetAndMetadata(origRecord.kafkaOffset() + 1)
            );
            if (transRecord != null) {
                recordBatch.add(transRecord);
            } else {
                log.trace("{} Transformations returned null, so dropping record in topic '{}' partition {} and timestamp {} with key {} and value {}",
                        this, record.topic(), record.partition(), timestamp, keyAndSchema.value(), valueAndSchema.value());
            }
        }
    }

    private Headers convertHeadersFor(ProducerRecord<byte[], byte[]> record) {
        Headers result = new ConnectHeaders();
        org.apache.kafka.common.header.Headers recordHeaders = record.headers();
        if (recordHeaders != null) {
            String topic = record.topic();
            for (org.apache.kafka.common.header.Header recordHeader : recordHeaders) {
                SchemaAndValue schemaAndValue = headerConverter.toConnectHeader(topic, recordHeader.key(), recordHeader.value());
                result.add(recordHeader.key(), schemaAndValue);
            }
        }
        return result;
    }

    private void deliverRecords() {
        // Finally, deliver this batch to the sink
        try {
            // Since we reuse the recordBatch buffer, ensure we give the task its own copy
            log.trace("{} Delivering batch of {} messages to task", this, recordBatch.size());
            task.put(new ArrayList<>(recordBatch));
            currentOffsets.putAll(origOffsets);
            recordBatch.clear();
        } catch (RetriableException e) {
            log.error("{} RetriableException from SinkTask:", this, e);
            // Let this exit normally, the batch will be reprocessed on the next loop.
        } catch (Throwable t) {
            log.error("{} Task threw an uncaught and unrecoverable exception. Task is being killed and will not "
                    + "recover until manually restarted.", this, t);
            throw new ConnectException("Exiting SinkTask due to unrecoverable exception.", t);
        }
    }
}
