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
package org.apache.kafka.connect.streams.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.streams.ConnectStreamsConfig;
import org.apache.kafka.streams.processor.DefaultPartitionGrouper;
import org.apache.kafka.streams.processor.PartitionGrouper;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ConnectStreamsPartitionAssignor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WrappedPartitionGrouper extends DefaultPartitionGrouper implements Configurable {

    private static final Logger log = LoggerFactory.getLogger(DefaultPartitionGrouper.class);

    private List<String> sourceTopics;

    @Override
    public void configure(final Map<String, ?> configs) {
        String topics = (String) configs.get(ConnectStreamsConfig.SOURCE_TASK_TOPICS_CONFIG);
        sourceTopics = new ArrayList<String>(Arrays.asList(topics.split(",")));
    }

    @Override
    public Map<TaskId, Set<TopicPartition>> partitionGroups(Map<Integer, Set<String>> topicGroups, Cluster metadata) {
        Map<TaskId, Set<TopicPartition>> groups = new HashMap<>();

        for (Map.Entry<Integer, Set<String>> entry : topicGroups.entrySet()) {
            Integer topicGroupId = entry.getKey();
            Set<String> topicGroup = entry.getValue();

            int maxNumPartitions;
            try {
                maxNumPartitions = maxNumPartitions(metadata, topicGroup);
            } catch (RuntimeException e) {
                maxNumPartitions = ConnectStreamsPartitionAssignor.NOT_AVAILABLE;
            }

            for (int partitionId = 0; partitionId < maxNumPartitions; partitionId++) {
                Set<TopicPartition> group = new HashSet<>(topicGroup.size());

                for (String topic : topicGroup) {
                    List<PartitionInfo> partitions = metadata.partitionsForTopic(topic);
                    if (partitionId < partitions.size()) {
                        group.add(new TopicPartition(topic, partitionId));
                    }
                }
                groups.put(new TaskId(topicGroupId, partitionId), Collections.unmodifiableSet(group));
            }
        }

        // TODO check
        System.out.println("*** before groups " + groups);
        for (int i = 0; i < sourceTopics.size(); i++) {
            groups.put(new TaskId(i, 0), Collections.singleton(new TopicPartition(sourceTopics.get(i), 0)));
        }
        System.out.println("*** after groups " + groups);
        return Collections.unmodifiableMap(groups);
    }
}



