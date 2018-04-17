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
package org.apache.kafka.connect.streams.integration;

import io.confluent.connect.jdbc.sink.JdbcSinkTask;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTask;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.streams.ConnectClientSupplier;
import org.apache.kafka.connect.streams.ConnectStreamsConfig;
import org.apache.kafka.connect.streams.JsonSerde;
import org.apache.kafka.connect.streams.Utils;
import org.apache.kafka.connect.util.EmbeddedDerby;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Tests all available joins of Kafka Streams DSL.
 */
@Category({IntegrationTest.class})
@RunWith(value = Parameterized.class)
public abstract class AbstractDatabaseJoinIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
    public static EmbeddedDerby DB;
    public static AtomicInteger COUNTER = new AtomicInteger();

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(TestUtils.tempDirectory());

    @Parameterized.Parameters(name = "caching enabled = {0}")
    public static Collection<Object[]> data() {
        List<Object[]> values = new ArrayList<>();
        for (boolean cacheEnabled : Arrays.asList(true, false))
            values.add(new Object[]{cacheEnabled});
        return values;
    }

    static String appID;

    private static final Long COMMIT_INTERVAL = 100L;
    static final Properties STREAMS_CONFIG = new Properties();
    static final String INPUT_TOPIC_RIGHT = "INPUT_TOPIC_RIGHT";
    static final String INPUT_TOPIC_LEFT = "INPUT_TOPIC_LEFT";
    static final String OUTPUT_TOPIC = "OUTPUT_TOPIC";
    protected final long anyUniqueKey = 0L;

    private final static Properties PRODUCER_CONFIG = new Properties();
    private final static Properties RESULT_CONSUMER_CONFIG = new Properties();

    private KafkaProducer<Long, String> producer;
    private KafkaStreams streams;

    StreamsBuilder builder;
    int numRecordsExpected = 0;
    AtomicBoolean finalResultReached = new AtomicBoolean(false);

    private final List<Input<String>> input = Arrays.asList(
            new Input<>(INPUT_TOPIC_LEFT, (String) null),
            new Input<>(INPUT_TOPIC_RIGHT, (String) null),
            new Input<>(INPUT_TOPIC_LEFT, "A"),
            new Input<>(INPUT_TOPIC_RIGHT, "a"),
            new Input<>(INPUT_TOPIC_LEFT, "B"),
            new Input<>(INPUT_TOPIC_RIGHT, "b"),
            new Input<>(INPUT_TOPIC_LEFT, (String) null),
            new Input<>(INPUT_TOPIC_RIGHT, (String) null),
            new Input<>(INPUT_TOPIC_LEFT, "C"),
            new Input<>(INPUT_TOPIC_RIGHT, "c"),
            new Input<>(INPUT_TOPIC_RIGHT, (String) null),
            new Input<>(INPUT_TOPIC_LEFT, (String) null),
            new Input<>(INPUT_TOPIC_RIGHT, (String) null),
            new Input<>(INPUT_TOPIC_RIGHT, "d"),
            new Input<>(INPUT_TOPIC_LEFT, "D")
    );

    final ValueJoiner<String, String, String> valueJoiner = new ValueJoiner<String, String, String>() {
        @Override
        public String apply(final String value1, final String value2) {
            return value1 + "-" + value2;
        }
    };

    final boolean cacheEnabled;

    AbstractDatabaseJoinIntegrationTest(boolean cacheEnabled) {
        this.cacheEnabled = cacheEnabled;
    }

    @BeforeClass
    public static void setupConfigsAndUtils() {
        PRODUCER_CONFIG.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        PRODUCER_CONFIG.put(ProducerConfig.ACKS_CONFIG, "all");
        PRODUCER_CONFIG.put(ProducerConfig.RETRIES_CONFIG, 0);
        PRODUCER_CONFIG.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        PRODUCER_CONFIG.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, appID + "-result-consumer");
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        STREAMS_CONFIG.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL);
    }

    void prepareEnvironment() throws InterruptedException, SQLException {
        CLUSTER.createTopics(INPUT_TOPIC_LEFT, INPUT_TOPIC_RIGHT, OUTPUT_TOPIC);

        if (!cacheEnabled)
            STREAMS_CONFIG.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, testFolder.getRoot().getPath());

        DB = new EmbeddedDerby("db-" + COUNTER.getAndIncrement());
        DB.createTable(INPUT_TOPIC_LEFT,
                "id", "BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)",
                "value", "VARCHAR(256)");
        DB.createTable(INPUT_TOPIC_RIGHT,
                "id", "BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)",
                "value", "VARCHAR(256)");
        DB.createTable(OUTPUT_TOPIC,
                "id", "BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)",
                "value", "VARCHAR(256)");

        produceData();
    }

    private void produceData() throws SQLException {
        for (final Input singleInput : input) {
            DB.insert(singleInput.topic, "value", singleInput.record.value);
        }
    }

    @After
    public void cleanup() throws InterruptedException, IOException {
        DB.dropDatabase();
        CLUSTER.deleteTopicsAndWait(120000, INPUT_TOPIC_LEFT, INPUT_TOPIC_RIGHT, OUTPUT_TOPIC);
    }

    private void checkResult(final String outputTopic, final List<String> expectedResult) throws InterruptedException {
        final List<String> result = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(RESULT_CONSUMER_CONFIG, outputTopic, expectedResult.size(), 30 * 1000L);
        assertThat(result, is(expectedResult));
    }

    private void checkResult(final String outputTopic, final String expectedFinalResult, final int expectedTotalNumRecords) throws InterruptedException {
        final List<String> result = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(RESULT_CONSUMER_CONFIG, outputTopic, expectedTotalNumRecords, 30 * 1000L);
        assertThat(result.get(result.size() - 1), is(expectedFinalResult));
    }

    /*
     * Runs the actual test. Checks the result after each input record to ensure fixed processing order.
     * If an input tuple does not trigger any result, "expectedResult" should contain a "null" entry
     */
    void runTest(final List<List<String>> expectedResult) throws Exception {
        runTest(expectedResult, null);
    }


    /*
     * Runs the actual test. Checks the result after each input record to ensure fixed processing order.
     * If an input tuple does not trigger any result, "expectedResult" should contain a "null" entry
     */
    void runTest(final List<List<String>> expectedResult, final String storeName) throws Exception {
        assert expectedResult.size() == input.size();

        IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);

        createStreams();

        String expectedFinalResult = null;

        try {
            streams.start();

            long ts = System.currentTimeMillis();

            Map<Long, String> records = new HashMap<>();
            while (records.size() == 0) {
                records = consumeData();
                Thread.sleep(1000L);
            }
            for (Map.Entry<Long, String> record : records.entrySet()) {
                System.out.println("(" + record.getKey() + ", " + record.getValue() + ")");
            }

            /*
            final Iterator<List<String>> resultIterator = expectedResult.iterator();
            for (final Input<String> singleInput : input) {
                producer.send(new ProducerRecord<>(singleInput.topic, null, ++ts, singleInput.record.key, singleInput.record.value)).get();

                List<String> expected = resultIterator.next();

                if (expected != null) {
                    checkResult(OUTPUT_TOPIC, expected);
                    expectedFinalResult = expected.get(expected.size() - 1);
                }
            }
            */
        } finally {
            streams.close();
        }
    }

    /*
     * Runs the actual test. Checks the final result only after expected number of records have been consumed.
     */
    void runTest(final String expectedFinalResult) throws Exception {
        IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);

        createStreams();

        try {
            streams.start();

            long ts = System.currentTimeMillis();

            /*
            TestUtils.waitForCondition(new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return finalResultReached.get();
                }
            }, "Never received expected final result.");
            */

            checkResult(OUTPUT_TOPIC, expectedFinalResult, numRecordsExpected);

        } finally {
            streams.close();
        }
    }

    private void createStreams() {
        Properties props = new Properties();
        for (Object o : STREAMS_CONFIG.keySet()) {
            String key = o.toString();
            props.put(key, STREAMS_CONFIG.get(key));
        }

        /*
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "simple-example-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        */
        props.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                JsonSerde.class.getName()
        );
        props.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                JsonSerde.class.getName()
        );
        /*
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(
                StreamsConfig.STATE_DIR_CONFIG,
                TestUtils.tempDirectory().getAbsolutePath()
        );
        */

        props.put(ConnectStreamsConfig.SOURCE_TASK_TOPICS_CONFIG, INPUT_TOPIC_LEFT + "," + INPUT_TOPIC_RIGHT);
        props.put(ConnectStreamsConfig.SINK_TASK_TOPICS_CONFIG, OUTPUT_TOPIC);
        StreamsConfig streamsConfig = new StreamsConfig(props);

        Map<String, Object> config = Utils.fromProperties(props);
        config.put(ConnectStreamsConfig.KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        config.put(ConnectStreamsConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        config.put("key.converter." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        config.put("value.converter." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        config.put(ConnectStreamsConfig.HEADER_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        config.put(ConnectStreamsConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        config.put(ConnectStreamsConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        config.put("internal.key.converter." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
        config.put("internal.value.converter." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
        ConnectStreamsConfig connectStreamsConfig = new ConnectStreamsConfig(config);

        config = Utils.fromProperties(props);
        config.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, DB.getUrl());
        config.put(TaskConfig.TASK_CLASS_CONFIG, JdbcSinkTask.class.getName());
        TaskConfig sinkTaskConfig = new TaskConfig(config);

        config = Utils.fromProperties(props);
        config.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, DB.getUrl());
        //config.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        config.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_INCREMENTING);
        config.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, "id");
        config.put(JdbcSourceTaskConfig.TABLES_CONFIG, INPUT_TOPIC_LEFT);
        config.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, "");
        config.put(TaskConfig.TASK_CLASS_CONFIG, JdbcSourceTask.class.getName());
        TaskConfig leftTaskConfig = new TaskConfig(config);

        config = Utils.fromProperties(props);
        config.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, DB.getUrl());
        //config.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        config.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_INCREMENTING);
        config.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, "id");
        config.put(JdbcSourceTaskConfig.TABLES_CONFIG, INPUT_TOPIC_RIGHT);
        config.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, "");
        config.put(TaskConfig.TASK_CLASS_CONFIG, JdbcSourceTask.class.getName());
        TaskConfig rightTaskConfig = new TaskConfig(config);

        Map<String, TaskConfig> inputTaskConfigs = new HashMap<>();
        inputTaskConfigs.put(INPUT_TOPIC_LEFT, leftTaskConfig);
        inputTaskConfigs.put(INPUT_TOPIC_RIGHT, rightTaskConfig);

        streams = new KafkaStreams(builder.build(), streamsConfig,
                new ConnectClientSupplier("JDBC", connectStreamsConfig,
                        inputTaskConfigs,
                        Collections.singletonMap(OUTPUT_TOPIC, sinkTaskConfig)));
    }

    private Map<Long, String> consumeData() throws SQLException {
        Map<Long, String> result = new HashMap<>();
        int count = DB.select("SELECT * FROM " + OUTPUT_TOPIC, new EmbeddedDerby.ResultSetReadCallback() {
            public void read(final ResultSet rs) throws SQLException {
                result.put(rs.getLong("id"), rs.getString("value"));
            }
        });
        System.out.println("*** found " + count + " records");
        return result;
    }

    private final class Input<V> {
        String topic;
        KeyValue<Long, V> record;

        Input(final String topic, final V value) {
            this.topic = topic;
            record = KeyValue.pair(anyUniqueKey, value);
        }
    }
}
