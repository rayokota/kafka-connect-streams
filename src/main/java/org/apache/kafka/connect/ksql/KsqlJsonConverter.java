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
package org.apache.kafka.connect.ksql;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.json.KsqlJsonSerializer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KsqlJsonConverter implements Converter {

    public KsqlJsonConverter() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        try {
            Serializer<GenericRow> serializer = new KsqlJsonSerializer(schema);
            Struct struct = (Struct) value;
            GenericRow row = toRow(struct);
            return serializer.serialize(topic, row);
        } catch (SerializationException e) {
            throw new DataException("Failed to serialize to a string: ", e);
        }
    }

    private GenericRow toRow(Struct struct) {
        if (struct == null) return null;
        Schema schema = struct.schema();
        List<Object> list = new ArrayList<>();
        for (Field field : schema.fields()) {
            list.add(struct.get(field));
        }
        return new GenericRow(list);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        // TODO unsupported
        throw new UnsupportedOperationException();
    }
}
