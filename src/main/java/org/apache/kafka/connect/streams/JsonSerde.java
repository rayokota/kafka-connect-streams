package org.apache.kafka.connect.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.Map;

public class JsonSerde implements Serde<SchemaAndValue> {

    final private JsonSerializer serializer;
    final private JsonDeserializer deserializer;

    public JsonSerde() {
        this.serializer = new JsonSerializer();
        this.deserializer = new JsonDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public JsonSerializer serializer() {
        return serializer;
    }

    @Override
    public JsonDeserializer deserializer() {
        return deserializer;
    }
}
