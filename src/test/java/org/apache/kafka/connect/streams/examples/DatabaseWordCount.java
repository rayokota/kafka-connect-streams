/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.streams.examples;

import io.confluent.connect.jdbc.sink.JdbcSinkTask;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTask;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.streams.ConnectClientSupplier;
import org.apache.kafka.connect.streams.ConnectStreamsConfig;
import org.apache.kafka.connect.streams.JsonSerde;
import org.apache.kafka.connect.streams.Utils;
import org.apache.kafka.connect.util.TestUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Serialized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DatabaseWordCount {

    private KafkaStreams streams;

    public void countWords(
            String bootstrapServers, String zookeeperConnect,
            String inputTopic, String outputTopic,
            String jdbcUrl) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-example-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                JsonSerde.class.getName()
        );
        props.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                JsonSerde.class.getName()
        );
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(
                StreamsConfig.STATE_DIR_CONFIG,
                TestUtils.tempDirectory().getAbsolutePath()
        );

        props.put(ConnectStreamsConfig.SINK_TASK_TOPICS_CONFIG, outputTopic);
        props.put(ConnectStreamsConfig.SOURCE_TASK_TOPICS_CONFIG, inputTopic);
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
        config.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, jdbcUrl);
        config.put(TaskConfig.TASK_CLASS_CONFIG, JdbcSinkTask.class.getName());
        TaskConfig sinkTaskConfig = new TaskConfig(config);

        config = Utils.fromProperties(props);
        config.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, jdbcUrl);
        config.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        config.put(JdbcSourceTaskConfig.TABLES_CONFIG, inputTopic);
        config.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, "");
        config.put(TaskConfig.TASK_CLASS_CONFIG, JdbcSourceTask.class.getName());
        TaskConfig sourceTaskConfig = new TaskConfig(config);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<SchemaAndValue, SchemaAndValue> input = builder.stream(inputTopic);

        KStream<SchemaAndValue, String> words = input
                .flatMapValues(value -> {
                    Struct lines = (Struct) value.value();
                    String[] strs = lines.get("lines").toString().toLowerCase().split("\\W+");
                    List<String> result = new ArrayList<>();
                    for (String str : strs) {
                        if (str.length() > 0) {
                            result.add(str);
                        }
                    }
                    return result;
                });

        KTable<String, Long> wordCounts = words
                .groupBy((key, word) -> word, Serialized.with(Serdes.String(), Serdes.String()))
                .count();

        wordCounts.toStream().map(
                (key, value) -> {
                    Schema schema = SchemaBuilder.struct().name("word")
                            .field("word", Schema.STRING_SCHEMA)
                            .field("count", Schema.INT64_SCHEMA).build();
                    Struct struct = new Struct(schema).put("word", key).put("count", value);
                    return new KeyValue<>(SchemaAndValue.NULL, new SchemaAndValue(schema, struct));
                }).to(outputTopic);

        streams = new KafkaStreams(builder.build(), streamsConfig,
                new ConnectClientSupplier("JDBC", connectStreamsConfig,
                        Collections.singletonMap(inputTopic, sourceTaskConfig),
                        Collections.singletonMap(outputTopic, sinkTaskConfig)));
        streams.start();
    }

    public void close() {
        streams.close();
    }
}
