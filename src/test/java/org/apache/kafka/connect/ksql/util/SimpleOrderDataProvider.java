/*
 * Copyright 2017 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.ksql.util;

import io.confluent.ksql.GenericRow;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SimpleOrderDataProvider extends TestDataProvider {

    private static final String namePrefix =
            "ORDER";

    private static final String ksqlSchemaString =
            "(ORDERTIME bigint, ORDERID varchar, ITEMID varchar, ORDERUNITS double)";

    private static final String key = "ORDERTIME";

    private static final Schema schema = SchemaBuilder.struct()
            .field("ORDERTIME", SchemaBuilder.INT64_SCHEMA)
            .field("ORDERID", SchemaBuilder.STRING_SCHEMA)
            .field("ITEMID", SchemaBuilder.STRING_SCHEMA)
            .field("ORDERUNITS", SchemaBuilder.FLOAT64_SCHEMA).build();

    private static final Map<String, GenericRow> data = new SimpleOrderDataProvider().buildData();

    public SimpleOrderDataProvider() {
        super(namePrefix, ksqlSchemaString, key, schema, data);
    }

    private Map<String, GenericRow> buildData() {

        Map<String, GenericRow> dataMap = new HashMap<>();
        dataMap.put("1", new GenericRow(Arrays.asList(1l, "ORDER_1",
                "ITEM_1", 10.0)));

        dataMap.put("2", new GenericRow(Arrays.asList(2l, "ORDER_2",
                "ITEM_2", 20.0)));

        dataMap.put("3", new GenericRow(Arrays.asList(3l, "ORDER_3",
                "ITEM_3", 30.0)));

        dataMap.put("4", new GenericRow(Arrays.asList(4l, "ORDER_4",
                "ITEM_4", 40.0)));

        dataMap.put("5", new GenericRow(Arrays.asList(5l, "ORDER_5",
                "ITEM_5", 50.0)));

        dataMap.put("6", new GenericRow(Arrays.asList(6l, "ORDER_6",
                "ITEM_6", 60.0)));

        dataMap.put("7", new GenericRow(Arrays.asList(7l, "ORDER_7",
                "ITEM_7", 70.0)));

        dataMap.put("8", new GenericRow(Arrays.asList(8l, "ORDER_8",
                "ITEM_8", 80.0)));

        return dataMap;
    }

}
