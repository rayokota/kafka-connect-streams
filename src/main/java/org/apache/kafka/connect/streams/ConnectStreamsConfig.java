// (Copyright) [2016 - 2018] Confluent, Inc.

package org.apache.kafka.connect.streams;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;

import java.util.Map;
import java.util.Properties;

public class ConnectStreamsConfig extends AbstractConfig {

    public static final String SOURCE_TASK_TOPICS_CONFIG = "source.task.topics";
    public static final String SOURCE_TASK_TOPICS_CONFIG_DOC =
            "The topics for the source task.";

    public static final String SINK_TASK_TOPICS_CONFIG = "sink.task.topics";
    public static final String SINK_TASK_TOPICS_CONFIG_DOC =
            "The topics for the sink task.";

    public static final String KEY_CONVERTER_CLASS_CONFIG = "key.converter";
    public static final String KEY_CONVERTER_CLASS_DOC =
            "Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka." +
                    " This controls the format of the keys in messages written to or read from Kafka, and since this is" +
                    " independent of connectors it allows any connector to work with any serialization format." +
                    " Examples of common formats include JSON and Avro.";

    public static final String VALUE_CONVERTER_CLASS_CONFIG = "value.converter";
    public static final String VALUE_CONVERTER_CLASS_DOC =
            "Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka." +
                    " This controls the format of the values in messages written to or read from Kafka, and since this is" +
                    " independent of connectors it allows any connector to work with any serialization format." +
                    " Examples of common formats include JSON and Avro.";

    public static final String HEADER_CONVERTER_CLASS_CONFIG = "header.converter";
    public static final String HEADER_CONVERTER_CLASS_DOC =
            "HeaderConverter class used to convert between Kafka Connect format and the serialized form that is written to Kafka." +
                    " This controls the format of the header values in messages written to or read from Kafka, and since this is" +
                    " independent of connectors it allows any connector to work with any serialization format." +
                    " Examples of common formats include JSON and Avro. By default, the SimpleHeaderConverter is used to serialize" +
                    " header values to strings and deserialize them by inferring the schemas.";
    public static final String HEADER_CONVERTER_CLASS_DEFAULT = SimpleHeaderConverter.class.getName();

    public static final String INTERNAL_KEY_CONVERTER_CLASS_CONFIG = "internal.key.converter";
    public static final String INTERNAL_KEY_CONVERTER_CLASS_DOC =
            "Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka." +
                    " This controls the format of the keys in messages written to or read from Kafka, and since this is" +
                    " independent of connectors it allows any connector to work with any serialization format." +
                    " Examples of common formats include JSON and Avro." +
                    " This setting controls the format used for internal bookkeeping data used by the framework, such as" +
                    " configs and offsets, so users can typically use any functioning Converter implementation.";

    public static final String INTERNAL_VALUE_CONVERTER_CLASS_CONFIG = "internal.value.converter";
    public static final String INTERNAL_VALUE_CONVERTER_CLASS_DOC =
            "Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka." +
                    " This controls the format of the values in messages written to or read from Kafka, and since this is" +
                    " independent of connectors it allows any connector to work with any serialization format." +
                    " Examples of common formats include JSON and Avro." +
                    " This setting controls the format used for internal bookkeeping data used by the framework, such as" +
                    " configs and offsets, so users can typically use any functioning Converter implementation.";

    public static final String OFFSET_COMMIT_INTERVAL_MS_CONFIG = "offset.flush.interval.ms";
    private static final String OFFSET_COMMIT_INTERVAL_MS_DOC
            = "Interval at which to try committing offsets for tasks.";
    public static final long OFFSET_COMMIT_INTERVAL_MS_DEFAULT = 60000L;

    public static final String OFFSET_COMMIT_TIMEOUT_MS_CONFIG = "offset.flush.timeout.ms";
    private static final String OFFSET_COMMIT_TIMEOUT_MS_DOC
            = "Maximum number of milliseconds to wait for records to flush and partition offset data to be"
            + " committed to offset storage before cancelling the process and restoring the offset "
            + "data to be committed in a future attempt.";
    public static final long OFFSET_COMMIT_TIMEOUT_MS_DEFAULT = 5000L;

    private static final ConfigDef config = new ConfigDef()
            .define(SOURCE_TASK_TOPICS_CONFIG, Type.STRING,
                    Importance.HIGH, SOURCE_TASK_TOPICS_CONFIG_DOC)
            .define(SINK_TASK_TOPICS_CONFIG, Type.STRING,
                    Importance.HIGH, SINK_TASK_TOPICS_CONFIG_DOC)
            .define(KEY_CONVERTER_CLASS_CONFIG, Type.CLASS,
                    Importance.HIGH, KEY_CONVERTER_CLASS_DOC)
            .define(VALUE_CONVERTER_CLASS_CONFIG, Type.CLASS,
                    Importance.HIGH, VALUE_CONVERTER_CLASS_DOC)
            .define(HEADER_CONVERTER_CLASS_CONFIG, Type.CLASS,
                    HEADER_CONVERTER_CLASS_DEFAULT,
                    Importance.LOW, HEADER_CONVERTER_CLASS_DOC)
            .define(INTERNAL_KEY_CONVERTER_CLASS_CONFIG, Type.CLASS,
                    Importance.LOW, INTERNAL_KEY_CONVERTER_CLASS_DOC)
            .define(INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, Type.CLASS,
                    Importance.LOW, INTERNAL_VALUE_CONVERTER_CLASS_DOC)
            // TODO use for offset commit
            .define(OFFSET_COMMIT_INTERVAL_MS_CONFIG, Type.LONG, OFFSET_COMMIT_INTERVAL_MS_DEFAULT,
                    Importance.LOW, OFFSET_COMMIT_INTERVAL_MS_DOC)
            // TODO use for offset commit
            .define(OFFSET_COMMIT_TIMEOUT_MS_CONFIG, Type.LONG, OFFSET_COMMIT_TIMEOUT_MS_DEFAULT,
                    Importance.LOW, OFFSET_COMMIT_TIMEOUT_MS_DOC);

    public ConnectStreamsConfig(Properties props) {
        super(config, props);
    }

    public ConnectStreamsConfig(Map<String, ?> clientConfigs) {
        super(config, clientConfigs);
    }
}
