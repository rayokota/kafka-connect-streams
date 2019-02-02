package org.apache.kafka.connect.ksql.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.sink.JdbcSinkTask;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTask;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.serde.DataSource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.ksql.KsqlJsonConverter;
import org.apache.kafka.connect.ksql.testutils.MockAvroConverter;
import org.apache.kafka.connect.ksql.util.SimpleOrderDataProvider;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.streams.ConnectClientSupplier;
import org.apache.kafka.connect.streams.ConnectStreamsConfig;
import org.apache.kafka.connect.streams.Utils;
import org.apache.kafka.connect.util.EmbeddedDerby;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


@Category({IntegrationTest.class})
public class DatabaseSelectAndProjectIntTest {

    public static EmbeddedDerby DB;
    public static AtomicInteger COUNTER = new AtomicInteger();

    private static final String OUTPUT_TOPIC = "";

    private IntegrationTestHarness testHarness;
    private KsqlContext ksqlContext;
    private final String jsonTopicName = "jsonTopic";
    private final String jsonStreamName = "orders_json";
    private final String avroTopicName = "avroTopic";
    private final String avroStreamName = "orders_avro";
    private SimpleOrderDataProvider dataProvider;

    @Before
    public void before() throws Exception {
        DB = new EmbeddedDerby("db-" + COUNTER.getAndIncrement());
    }

    private void setup(String inputTopic) throws Exception {
        DB.createTable(inputTopic,
                "id", "INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)",
                "ordertime", "BIGINT",
                "orderid", "VARCHAR(256)",
                "itemid", "VARCHAR(256)",
                "orderunits", "DOUBLE");
        String outputTopic = OUTPUT_TOPIC;
        String jdbcUrl = DB.getUrl();

        Properties props = new Properties();

        props.put(ConnectStreamsConfig.SINK_TASK_TOPICS_CONFIG, outputTopic);
        props.put(ConnectStreamsConfig.SOURCE_TASK_TOPICS_CONFIG, inputTopic);

        Map<String, Object> config = Utils.fromProperties(props);
        if (inputTopic.equals(jsonTopicName)) {
            config.put(ConnectStreamsConfig.KEY_CONVERTER_CLASS_CONFIG, KsqlJsonConverter.class.getName());
            config.put(ConnectStreamsConfig.VALUE_CONVERTER_CLASS_CONFIG, KsqlJsonConverter.class.getName());
            config.put("key.converter." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
            config.put("value.converter." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        } else {
            config.put(ConnectStreamsConfig.KEY_CONVERTER_CLASS_CONFIG, MockAvroConverter.class.getName());
            config.put(ConnectStreamsConfig.VALUE_CONVERTER_CLASS_CONFIG, MockAvroConverter.class.getName());
        }
        config.put(ConnectStreamsConfig.HEADER_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        config.put(ConnectStreamsConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        config.put(ConnectStreamsConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        config.put("internal.key.converter." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
        config.put("internal.value.converter." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
        ConnectStreamsConfig connectStreamsConfig = new ConnectStreamsConfig(config);

        config = Utils.fromProperties(props);
        config.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, jdbcUrl);
        config.put(TaskConfig.TASK_CLASS_CONFIG, JdbcSinkTask.class.getName());
        TaskConfig sinkTaskConfig = new TaskConfig(config);

        config = Utils.fromProperties(props);
        config.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, jdbcUrl);
        //config.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        config.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_INCREMENTING);
        config.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, "id");
        config.put(JdbcSourceTaskConfig.TABLES_CONFIG, inputTopic);
        config.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, "");
        config.put(TaskConfig.TASK_CLASS_CONFIG, JdbcSourceTask.class.getName());
        TaskConfig sourceTaskConfig = new TaskConfig(config);

        KafkaClientSupplier clientSupplier = new ConnectClientSupplier("JDBC", connectStreamsConfig,
                Collections.singletonMap(inputTopic, sourceTaskConfig),
                Collections.singletonMap(outputTopic, sinkTaskConfig));
        testHarness = new IntegrationTestHarness();
        testHarness.start();
        ksqlContext = KsqlContext.create(testHarness.ksqlConfig, testHarness.schemaRegistryClientFactory, clientSupplier);
        testHarness.createTopic(jsonTopicName);
        testHarness.createTopic(avroTopicName);
        MockAvroConverter.schemaRegistryClient = testHarness.schemaRegistryClient;

        /*
         * Setup test data
         */
        dataProvider = new SimpleOrderDataProvider();

        DB.insert(inputTopic, "ordertime", 1L, "orderid", "ORDER_1", "itemid", "ITEM_1", "orderunits", 10.0);
        DB.insert(inputTopic, "ordertime", 2L, "orderid", "ORDER_2", "itemid", "ITEM_2", "orderunits", 20.0);
        DB.insert(inputTopic, "ordertime", 3L, "orderid", "ORDER_3", "itemid", "ITEM_3", "orderunits", 30.0);
        DB.insert(inputTopic, "ordertime", 4L, "orderid", "ORDER_4", "itemid", "ITEM_4", "orderunits", 40.0);
        DB.insert(inputTopic, "ordertime", 5L, "orderid", "ORDER_5", "itemid", "ITEM_5", "orderunits", 50.0);
        DB.insert(inputTopic, "ordertime", 6L, "orderid", "ORDER_6", "itemid", "ITEM_6", "orderunits", 60.0);
        DB.insert(inputTopic, "ordertime", 7L, "orderid", "ORDER_7", "itemid", "ITEM_7", "orderunits", 70.0);
        DB.insert(inputTopic, "ordertime", 8L, "orderid", "ORDER_8", "itemid", "ITEM_8", "orderunits", 80.0);

        createOrdersStream();
    }

    @After
    public void after() throws Exception {
        ksqlContext.close();
        testHarness.stop();
        DB.dropDatabase();
    }

    @Ignore
    @Test
    public void testSelectProjectJson() throws Exception {
        setup(jsonTopicName);
        testSelectProject(jsonTopicName, "PROJECT_STREAM_JSON",
                jsonStreamName,
                DataSource.DataSourceSerDe.JSON);
    }

    @Ignore
    @Test
    public void testSelectProjectAvro() throws Exception {
        setup(avroTopicName);
        testSelectProject(avroTopicName, "PROJECT_STREAM_AVRO",
                avroStreamName,
                DataSource.DataSourceSerDe.AVRO);
    }

    @Test
    public void testSelectStarJson() throws Exception {
        setup(jsonTopicName);
        testSelectStar(jsonTopicName, "EASYORDERS_JSON",
                jsonStreamName,
                DataSource.DataSourceSerDe.JSON);
    }

    @Test
    public void testSelectStarAvro() throws Exception {
        setup(avroTopicName);
        testSelectStar(avroTopicName, "EASYORDERS_AVRO",
                avroStreamName,
                DataSource.DataSourceSerDe.AVRO);
    }


    @Test
    public void testSelectWithFilterJson() throws Exception {
        setup(jsonTopicName);
        testSelectWithFilter(jsonTopicName, "BIGORDERS_JSON",
                jsonStreamName,
                DataSource.DataSourceSerDe.JSON);
    }

    @Test
    public void testSelectWithFilterAvro() throws Exception {
        setup(avroTopicName);
        testSelectWithFilter(avroTopicName, "BIGORDERS_AVRO",
                avroStreamName,
                DataSource.DataSourceSerDe.AVRO);
    }

    private void testSelectProject(String inputTopic,
                                   String resultStream,
                                   String inputStreamName,
                                   DataSource
                                           .DataSourceSerDe dataSourceSerDe) throws Exception {

        Map<String, Object> overriddenProperties = new HashMap<>();
        overriddenProperties.put(ConnectStreamsConfig.SOURCE_TASK_TOPICS_CONFIG, inputTopic);
        ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT ITEMID, ORDERUNITS "
                + "FROM %s;", resultStream, inputStreamName), overriddenProperties);

        Schema resultSchema = ksqlContext.getMetaStore().getSource(resultStream).getSchema();

        List<GenericRow> easyOrdersData =
                testHarness.consumeDataToList(resultStream,
                        resultSchema,
                        dataProvider.data().size(),
                        new StringDeserializer(),
                        IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS,
                        dataSourceSerDe);

        GenericRow value = easyOrdersData.get(0);
        // skip over first to values (rowKey, rowTime)
        Assert.assertEquals("ITEM_1", value.getColumns().get(2));
    }

    private void testSelectStar(String inputTopic,
                                String resultStream,
                                String inputStreamName,
                                DataSource.DataSourceSerDe dataSourceSerDe) throws Exception {

        Map<String, Object> overriddenProperties = new HashMap<>();
        overriddenProperties.put(ConnectStreamsConfig.SOURCE_TASK_TOPICS_CONFIG, inputTopic);
        ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT * FROM %s;",
                resultStream,
                inputStreamName), overriddenProperties);

        List<GenericRow> easyOrdersData = testHarness.consumeDataToList(resultStream,
                dataProvider.schema(),
                dataProvider.data().size(),
                new StringDeserializer(),
                IntegrationTestHarness
                        .RESULTS_POLL_MAX_TIME_MS,
                dataSourceSerDe);
        Map<String, GenericRow> map = new HashMap<>();
        for (GenericRow row : easyOrdersData) {
            map.put(row.getColumns().get(0).toString(), row);
        }

        assertThat(map, equalTo(dataProvider.data()));
    }

    private void testSelectWithFilter(String inputTopic,
                                      String resultStream,
                                      String inputStreamName,
                                      DataSource.DataSourceSerDe dataSourceSerDe) throws Exception {

        Map<String, Object> overriddenProperties = new HashMap<>();
        overriddenProperties.put(ConnectStreamsConfig.SOURCE_TASK_TOPICS_CONFIG, inputTopic);
        ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT * FROM %s WHERE ORDERUNITS > 40;",
                resultStream, inputStreamName), overriddenProperties);

        List<GenericRow> results = testHarness.consumeDataToList(resultStream,
                dataProvider.schema(),
                4,
                new StringDeserializer(),
                IntegrationTestHarness
                        .RESULTS_POLL_MAX_TIME_MS,
                dataSourceSerDe);

        Assert.assertEquals(4, results.size());
    }

    private void createOrdersStream() throws Exception {
        ksqlContext.sql(String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, ITEMID "
                        + "varchar, ORDERUNITS double) "
                        + "WITH (kafka_topic='%s', value_format='%s', key='ordertime');",
                jsonStreamName,
                jsonTopicName,
                DataSource.DataSourceSerDe.JSON.name()));

        ksqlContext.sql(String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, ITEMID "
                        + "varchar, ORDERUNITS double) "
                        + "WITH (kafka_topic='%s', value_format='%s', key='ordertime');",
                avroStreamName,
                avroTopicName,
                DataSource.DataSourceSerDe.AVRO.name()));
    }

}