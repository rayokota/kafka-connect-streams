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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.runtime.TransformationChain;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.streams.internals.FileOffsetBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.apache.kafka.common.utils.Time.SYSTEM;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_RETRY_MAX_DELAY_DEFAULT;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_RETRY_TIMEOUT_DEFAULT;
import static org.apache.kafka.connect.runtime.errors.ToleranceType.NONE;

public class ConnectSourceConsumer implements Consumer<byte[], byte[]> {
    private static final Logger log = LoggerFactory.getLogger(ConnectSourceConsumer.class);

    private static final AtomicInteger NEXT_ID = new AtomicInteger();
    private static final RetryWithToleranceOperator NOOP_OPERATOR = new RetryWithToleranceOperator(
            ERRORS_RETRY_TIMEOUT_DEFAULT, ERRORS_RETRY_MAX_DELAY_DEFAULT, NONE, SYSTEM);


    private final ConsumerConfig config;
    private final SourceTask task;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final HeaderConverter headerConverter;
    private final TransformationChain<SourceRecord> transformationChain;
    private final OffsetStorageReader offsetReader;
    private final OffsetStorageWriter offsetWriter;
    private final Time time;

    private final List<PartitionAssignor> assignors;

    private Map<String, String> taskConfig;
    private boolean finishedStart = false;
    private boolean startedShutdownBeforeStartCompleted = false;

    private int capacity;  // maximum number of records to buffer
    private ReaderTask readerTask;

    public static ConnectSourceConsumer create(String connectorName, ConnectStreamsConfig connectStreamsConfig,
                                               TaskConfig taskConfig, ConsumerConfig consumerConfig) {
        Converter keyConverter = Utils.newConverter(connectStreamsConfig, ConnectStreamsConfig.KEY_CONVERTER_CLASS_CONFIG);
        Converter valueConverter = Utils.newConverter(connectStreamsConfig, ConnectStreamsConfig.VALUE_CONVERTER_CLASS_CONFIG);
        HeaderConverter headerConverter = Utils.newHeaderConverter(connectStreamsConfig, ConnectStreamsConfig.HEADER_CONVERTER_CLASS_CONFIG);

        // TODO use Kafka offset backing store
        /*
        Map<String, String> config = taskConfig.originalsStrings();
        config.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        WorkerConfig workerConfig = new StandaloneConfig(config);
        KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore();
        offsetBackingStore.configure(workerConfig);
        */
        FileOffsetBackingStore offsetBackingStore = new FileOffsetBackingStore("/tmp/connect-streams.offsets");
        offsetBackingStore.start();

        Converter internalKeyConverter = Utils.newConverter(connectStreamsConfig, ConnectStreamsConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG);
        Converter internalValueConverter = Utils.newConverter(connectStreamsConfig, ConnectStreamsConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG);
        OffsetStorageReader offsetReader = new OffsetStorageReaderImpl(offsetBackingStore, connectorName,
                internalKeyConverter, internalValueConverter);
        OffsetStorageWriter offsetWriter = new OffsetStorageWriter(offsetBackingStore, connectorName,
                internalKeyConverter, internalValueConverter);
        Class<? extends SourceTask> taskClass = taskConfig.getClass(TaskConfig.TASK_CLASS_CONFIG).asSubclass(SourceTask.class);
        SourceTask sourceTask = Utils.newInstance(taskClass);
        ConnectSourceConsumer consumer = new ConnectSourceConsumer(
                consumerConfig,
                sourceTask,
                keyConverter,
                valueConverter,
                headerConverter,
                new TransformationChain<>(Collections.emptyList(), NOOP_OPERATOR),
                offsetReader,
                offsetWriter,
                null,
                new SystemTime());
        consumer.initialize(taskConfig);
        consumer.start();
        return consumer;
    }

    public ConnectSourceConsumer(ConsumerConfig config,
                                 SourceTask task,
                                 Converter keyConverter,
                                 Converter valueConverter,
                                 HeaderConverter headerConverter,
                                 TransformationChain<SourceRecord> transformationChain,
                                 OffsetStorageReader offsetReader,
                                 OffsetStorageWriter offsetWriter,
                                 ClassLoader loader,
                                 Time time) {

        this.config = config;
        this.task = task;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.headerConverter = headerConverter;
        this.transformationChain = transformationChain;
        this.offsetReader = offsetReader;
        this.offsetWriter = offsetWriter;
        this.time = time;

        this.assignors = config.getConfiguredInstances(
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                PartitionAssignor.class);
        this.capacity = 5000;   // TODO make configurable?
    }

    public void initialize(TaskConfig taskConfig) {
        try {
            this.taskConfig = taskConfig.originalsStrings();
        } catch (Throwable t) {
            log.error("{} Task failed initialization and will not be started.", this, t);
        }
    }

    public void start() {
        task.initialize(new ConnectSourceTaskContext(offsetReader));
        task.start(taskConfig);
        log.info("{} Source task finished initialization and start", this);
        synchronized (this) {
            if (startedShutdownBeforeStartCompleted) {
                task.stop();
                return;
            }

            readerTask = new ReaderTask(task, capacity);
            final Thread thread = new Thread(
                    readerTask,
                    "source-task-" + NEXT_ID.getAndIncrement());
            thread.start();

            finishedStart = true;
        }
    }

    @Override
    public Set<TopicPartition> assignment() {
        return null;
    }

    @Override
    public Set<String> subscription() {
        return null;
    }

    @Override
    public void subscribe(Collection<String> topics) {
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
    }

    @Override
    public void subscribe(Pattern pattern) {
    }

    @Override
    public void unsubscribe() {
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(long timeout) {
        try {
            List<SourceRecord> records = readerTask.getNextRecords(timeout);
            System.out.println("*** ConnectSourceConsumer: poll " + (records != null ? records.size() : "null"));
            return records != null ? convertRecords(records) : ConsumerRecords.empty();
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(final Duration timeout) {
        return poll(timeout.toMillis());
    }

    private ConsumerRecords<byte[], byte[]> convertRecords(List<SourceRecord> records)
            throws InterruptedException {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> result = new HashMap<>();
        for (final SourceRecord preTransformRecord : records) {
            final SourceRecord record = transformationChain.apply(preTransformRecord);

            if (record == null) {
                commitTaskRecord(preTransformRecord);
                continue;
            }

            RecordHeaders headers = convertHeaderFor(record);
            byte[] key = keyConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
            byte[] value = valueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            int partition = record.kafkaPartition() != null ? record.kafkaPartition() : 0;
            Long timestamp = ConnectUtils.checkAndConvertTimestamp(record.timestamp());
            final ConsumerRecord<byte[], byte[]> consumerRecord =
                    new ConsumerRecord<>(
                            record.topic(),
                            partition,
                            0L,  // TODO specify offset?
                            timestamp != null ? timestamp : 0L,
                            TimestampType.NO_TIMESTAMP_TYPE,
                            0L,
                            0,
                            0,
                            key,
                            value,
                            headers);
            TopicPartition tp = new TopicPartition(record.topic(), partition);
            List<ConsumerRecord<byte[], byte[]>> consumerRecords = result.computeIfAbsent(tp, k -> new ArrayList<>());
            consumerRecords.add(consumerRecord);
            log.trace("{} Appending record with key {}, value {}", this, record.key(), record.value());
            // We need this queued first since the callback could happen immediately (even synchronously in some cases).
            // Because of this we need to be careful about handling retries -- we always save the previously attempted
            // record as part of toSend and need to use a flag to track whether we should actually add it to the outstanding
            // messages and update the offsets.
            synchronized (this) {
                // Offsets are converted & serialized in the OffsetWriter
                offsetWriter.offset(record.sourcePartition(), record.sourceOffset());
            }
        }
        return new ConsumerRecords<>(result);
    }

    private RecordHeaders convertHeaderFor(SourceRecord record) {
        Headers headers = record.headers();
        RecordHeaders result = new RecordHeaders();
        if (headers != null) {
            String topic = record.topic();
            for (Header header : headers) {
                String key = header.key();
                byte[] rawHeader = headerConverter.fromConnectHeader(topic, key, header.schema(), header.value());
                result.add(key, rawHeader);
            }
        }
        return result;
    }

    @Override
    public void commitSync() {
    }

    @Override
    public void commitSync(Duration timeout) {
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
    }

    @Override
    public void commitAsync() {
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
    }

    @Override
    public long position(TopicPartition partition) {
        return 0L;
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        return 0L;
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return null;
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return null;
    }

    @Override
    public Set<TopicPartition> paused() {
        return null;
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
                                                                   Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return null;
    }

    @Override
    public void close() {
        stop();
        transformationChain.close();
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        close();
    }

    @Override
    public void close(Duration timeout) {
        close();
    }

    @Override
    public void wakeup() {
    }

    private void commitTaskRecord(SourceRecord record) {
        try {
            task.commitRecord(record);
        } catch (Throwable t) {
            log.error("{} Exception thrown while calling task.commitRecord()", this, t);
        }
    }

    // TODO call
    public boolean commitOffsets() {
        long commitTimeoutMs = 5000L;

        log.info("{} Committing offsets", this);

        long started = time.milliseconds();
        long timeout = started + commitTimeoutMs;

        synchronized (this) {
            boolean flushStarted = offsetWriter.beginFlush();
            // Still wait for any producer records to flush, even if there aren't any offsets to write
            // to persistent storage

            if (!flushStarted) {
                commitSourceTask();
                return true;
            }
        }

        // Now we can actually flush the offsets to user storage.
        Future<Void> flushFuture = offsetWriter.doFlush(new org.apache.kafka.connect.util.Callback<Void>() {
            @Override
            public void onCompletion(Throwable error, Void result) {
                if (error != null) {
                    log.error("{} Failed to flush offsets to storage: ", this, error);
                } else {
                    log.trace("{} Finished flushing offsets to storage", this);
                }
            }
        });
        // Very rare case: offsets were unserializable and we finished immediately, unable to store
        // any data
        if (flushFuture == null) {
            offsetWriter.cancelFlush();
            return false;
        }
        try {
            flushFuture.get(Math.max(timeout - time.milliseconds(), 0), TimeUnit.MILLISECONDS);
            // There's a small race here where we can get the callback just as this times out (and log
            // success), but then catch the exception below and cancel everything. This won't cause any
            // errors, is only wasteful in this minor edge case, and the worst result is that the log
            // could look a little confusing.
        } catch (InterruptedException e) {
            log.warn("{} Flush of offsets interrupted, cancelling", this);
            offsetWriter.cancelFlush();
            return false;
        } catch (ExecutionException e) {
            log.error("{} Flush of offsets threw an unexpected exception: ", this, e);
            offsetWriter.cancelFlush();
            return false;
        } catch (TimeoutException e) {
            log.error("{} Timed out waiting to flush offsets to storage", this);
            offsetWriter.cancelFlush();
            return false;
        }

        commitSourceTask();
        return true;
    }

    private void commitSourceTask() {
        try {
            this.task.commit();
        } catch (Throwable t) {
            log.error("{} Exception thrown while calling task.commit()", this, t);
        }
    }

    private void stop() {
        synchronized (this) {
            if (finishedStart)
                // NOTE: this is not a blocking shutdown
                readerTask.stop();
            else
                startedShutdownBeforeStartCompleted = true;
        }
    }

    private static class ReaderTask implements Runnable {
        private final SourceTask reader;
        private final LinkedBlockingQueue<SourceRecord> queue;
        private final Semaphore available;

        private volatile boolean running;
        private volatile Exception lastException;

        private ReaderTask(SourceTask reader,
                           int capacity) {
            this.reader = reader;
            this.queue = new LinkedBlockingQueue<>();
            this.available = new Semaphore(capacity);
        }

        @Override
        public void run() {
            this.running = true;

            try {
                while (running) {
                    List<SourceRecord> records = reader.poll();
                    if (records.size() > 0) {
                        available.acquire(records.size());
                        enqueueRecords(records);
                    } else {
                        //TODO: make poll interval configurable
                        Thread.sleep(50);
                    }
                }
            } catch (Exception e) {
                lastException = e;
                running = false;
            } finally {
                try {
                    reader.stop();
                } catch (Exception e) {
                    log.error("Reader task failed to close reader", e);
                }
            }

            if (lastException != null) {
                queue.clear();
            }
        }

        private void enqueueRecords(List<SourceRecord> records)
                throws InterruptedException {
            for (SourceRecord record : records) {
                queue.put(record);
            }
        }

        void stop() {
            running = false;
        }

        List<SourceRecord> getNextRecords(long timeoutMillis)
                throws InterruptedException {
            if (lastException != null) {
                throw new RuntimeException(lastException);
            }

            final List<SourceRecord> records = new ArrayList<>();
            final SourceRecord record = queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);

            if (record != null) {
                records.add(record);
                queue.drainTo(records);
            }

            final int numElements = records.size();
            available.release(numElements);

            if (lastException != null) {
                throw new RuntimeException(lastException);
            }

            return records;
        }
    }
}
