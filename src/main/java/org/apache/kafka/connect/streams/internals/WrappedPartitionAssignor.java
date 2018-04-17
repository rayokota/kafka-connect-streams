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

import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.internals.StreamPartitionAssignor;

import java.util.Map;
import java.util.Set;

public class WrappedPartitionAssignor implements PartitionAssignor, Configurable {

    private StreamPartitionAssignor assignor = new StreamPartitionAssignor();

    @Override
    public void configure(final Map<String, ?> configs) {
        assignor.configure(configs);
    }

    @Override
    public String name() {
        return assignor.name();
    }

    @Override
    public Subscription subscription(final Set<String> topics) {
        return assignor.subscription(topics);
    }

    @Override
    public Map<String, Assignment> assign(final Cluster metadata,
                                          final Map<String, Subscription> subscriptions) {
        // TODO check
        Map<String, Assignment> clusterAssignment = assignor.assign(metadata, subscriptions);

        /*
        Map.Entry<String, Assignment> entry = clusterAssignment.entrySet().iterator().next();
        Assignment assignment = entry.getValue();
        List<TopicPartition> topicPartitions = new ArrayList<>(assignment.partitions());
        topicPartitions.add(new TopicPartition("WORDCOUNT_INPUT", 0));
        AssignmentInfo assignmentInfo = AssignmentInfo.decode(assignment.userData());
        List<TaskId> taskIds = new ArrayList<>(assignmentInfo.activeTasks);
        taskIds.add(new TaskId(0, 0));
        AssignmentInfo newAssignmentInfo = new AssignmentInfo(
                taskIds,
                assignmentInfo.standbyTasks,
                assignmentInfo.partitionsByHost);
        clusterAssignment.put(entry.getKey(), new Assignment(topicPartitions, newAssignmentInfo.encode()));
        */

        return clusterAssignment;
    }

    /**
     * @throws TaskAssignmentException if there is no task id for one of the partitions specified
     */
    @Override
    public void onAssignment(final Assignment assignment) {
        assignor.onAssignment(assignment);
    }

}
