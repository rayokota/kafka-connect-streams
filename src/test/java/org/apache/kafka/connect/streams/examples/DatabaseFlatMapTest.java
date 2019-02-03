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
package org.apache.kafka.connect.streams.examples;

import org.apache.kafka.connect.util.EmbeddedDerby;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DatabaseFlatMapTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
    public static EmbeddedDerby DB;

    private static final String INPUT_TOPIC = "WORDCOUNT_INPUT";
    private static final String OUTPUT_TOPIC = "WORDCOUNT_OUTPUT";

    private static String[] getTopics() {
        return new String[]{INPUT_TOPIC, OUTPUT_TOPIC};
    }

    private static final List<String> INPUT = Arrays.asList(
            "To be, or not to be,--that is the question:--",
            "Whether tis nobler in the mind to suffer",
            "The slings and arrows of outrageous fortune",
            "Or to take arms against a sea of troubles,"
    );

    @Before
    public void prepareEnvironment() throws Exception {
        DB = new EmbeddedDerby("db-" + System.currentTimeMillis());
        DB.createTable(INPUT_TOPIC,
                "id", "INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)",
                "lines", "VARCHAR(256)");
        DB.createTable(OUTPUT_TOPIC,
                "id", "INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)",
                "word", "VARCHAR(256)");

        produceData();
    }

    private void produceData() throws SQLException {
        for (final String singleInput : INPUT) {
            DB.insert(INPUT_TOPIC, "lines", singleInput);
        }
    }

    private List<String> consumeData() throws SQLException {
        List<String> result = new ArrayList<>();
        int count = DB.select("SELECT * FROM " + OUTPUT_TOPIC, new EmbeddedDerby.ResultSetReadCallback() {
            public void read(final ResultSet rs) throws SQLException {
                result.add(rs.getString("word"));
            }
        });
        System.out.println("*** found " + count + " records");
        return result;
    }

    @Test
    public void testFlatMap() throws Exception {
        runFlatMap(new DatabaseFlatMap());
    }

    protected void runFlatMap(DatabaseFlatMap flatMap) throws Exception {

        Thread thread = new Thread(() -> {
            try {
                flatMap.flatMap(
                        CLUSTER.bootstrapServers(),
                        CLUSTER.zKConnectString(),
                        INPUT_TOPIC,
                        OUTPUT_TOPIC,
                        DB.getUrl()
                );
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();

        List<String> records = new ArrayList<>();
        while (records.size() == 0) {
            records = consumeData();
            Thread.sleep(1000L);
        }
        for (String record : records) {
            System.out.println("(" + record + ")");
        }

        flatMap.close();

        /*
        assertEquals(4L, records.get("to").longValue());
        assertEquals(2L, records.get("be").longValue());
        assertEquals(2L, records.get("or").longValue());
        assertEquals(1L, records.get("not").longValue());
        assertEquals(1L, records.get("that").longValue());
        assertEquals(1L, records.get("is").longValue());
        assertEquals(3L, records.get("the").longValue());
        assertEquals(1L, records.get("question").longValue());
        assertEquals(1L, records.get("whether").longValue());
        */
    }

    @After
    public void cleanup() throws Exception {
        DB.dropDatabase();
    }
}
