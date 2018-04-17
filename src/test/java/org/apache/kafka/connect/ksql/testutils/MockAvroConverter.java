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
package org.apache.kafka.connect.ksql.testutils;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import java.util.Map;

public class MockAvroConverter extends AvroConverter {

    public static SchemaRegistryClient schemaRegistryClient;

    public MockAvroConverter() {
        super(schemaRegistryClient);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> map = (Map<String, Object>) configs;
        map.put("schema.registry.url", "");
        super.configure(map, isKey);
    }
}
