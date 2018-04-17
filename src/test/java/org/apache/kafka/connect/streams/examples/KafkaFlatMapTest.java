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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.util.TestUtils;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaFlatMapTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static final String IGNORE_KEY = "ignore-key";
    private static final String INPUT_TOPIC = "wordcount-input";
    private static final String OUTPUT_TOPIC = "wordcount-output";

    private static String[] getTopics() {
        return new String[]{INPUT_TOPIC, OUTPUT_TOPIC};
    }

    private static final List<String> INPUT = Arrays.asList(
            "To be, or not to be,--that is the question:--",
            "Whether 'tis nobler in the mind to suffer",
            "The slings and arrows of outrageous fortune",
            "Or to take arms against a sea of troubles,"
    );

    private static Properties PRODUCER_CONFIG;
    private static Properties CONSUMER_CONFIG;

    @BeforeClass
    public static void setupConfigsAndUtils() {
        PRODUCER_CONFIG = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class,
                StringSerializer.class, new Properties()
        );
        CONSUMER_CONFIG = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), "testgroup",
                StringDeserializer.class, StringDeserializer.class, new Properties()
        );
    }

    @Before
    public void prepareEnvironment() throws Exception {
        CLUSTER.createTopics(getTopics());

        produceData();
    }

    private void produceData()
            throws InterruptedException, ExecutionException {

        KafkaProducer<String, String> producer = new KafkaProducer<>(PRODUCER_CONFIG);

        for (final String singleInput : INPUT) {
            producer.send(new ProducerRecord<>(
                    INPUT_TOPIC, IGNORE_KEY, singleInput)).get();
        }

        producer.close();
    }

    private List<String> consumeData(
            int expectedNumMessages,
            long resultsPollMaxTimeMs) {

        List<String> result = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(CONSUMER_CONFIG)) {

            consumer.subscribe(Collections.singleton(OUTPUT_TOPIC));
            long pollStart = System.currentTimeMillis();
            long pollEnd = pollStart + resultsPollMaxTimeMs;
            while (System.currentTimeMillis() < pollEnd &&
                    continueConsuming(result.size(), expectedNumMessages)) {
                for (ConsumerRecord<String, String> record :
                        consumer.poll(Math.max(1, pollEnd - System.currentTimeMillis()))) {
                    if (record.value() != null) {
                        result.add(record.value());
                    }
                }
            }
        }
        return result;
    }

    private static boolean continueConsuming(int messagesConsumed, int maxMessages) {
        return maxMessages < 0 || messagesConsumed < maxMessages;
    }

    @Test
    public void testFlatMap() throws Exception {
        runFlatMap(new KafkaFlatMap());
    }

    protected void runFlatMap(KafkaFlatMap flatMap) throws Exception {

        Thread thread = new Thread(() -> {
            try {
                flatMap.flatMap(
                        CLUSTER.bootstrapServers(),
                        CLUSTER.zKConnectString(),
                        INPUT_TOPIC,
                        OUTPUT_TOPIC
                );
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();

        List<String> records = consumeData(26, 10000L);
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
        CLUSTER.deleteTopicsAndWait(120000, getTopics());
    }
}
