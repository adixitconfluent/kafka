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
package org.apache.kafka.tools;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.kafka.tools.ShareConsumerCorrectness.ConsumptionRecordKey;
import static org.apache.kafka.tools.ShareConsumerCorrectness.ConsumptionRecordValue;
import static org.apache.kafka.tools.ShareConsumerCorrectness.addOffsetsDetails;
import static org.apache.kafka.tools.ShareConsumerCorrectness.addRecordInShareConsumption;
import static org.apache.kafka.tools.ShareConsumerCorrectness.missingOffsetsInConsumedRecords;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShareConsumerCorrectnessTest {
    @Test
    public void testAddRecordInShareConsumption() {
        ConcurrentHashMap<ConsumptionRecordKey, List<ConsumptionRecordValue>> consumptionDetails = new ConcurrentHashMap<>();

        ConsumptionRecordKey key1 = Mockito.mock(ConsumptionRecordKey.class);
        ConsumptionRecordKey key2 = Mockito.mock(ConsumptionRecordKey.class);
        ConsumptionRecordValue value1 = Mockito.mock(ConsumptionRecordValue.class);
        ConsumptionRecordValue value2 = Mockito.mock(ConsumptionRecordValue.class);
        ConsumptionRecordValue value3 = Mockito.mock(ConsumptionRecordValue.class);

        addRecordInShareConsumption(key1, value1, consumptionDetails);
        assertEquals(1, consumptionDetails.size());
        assertTrue(consumptionDetails.containsKey(key1));
        assertEquals(List.of(value1), consumptionDetails.get(key1));

        addRecordInShareConsumption(key1, value2, consumptionDetails);
        assertEquals(1, consumptionDetails.size());
        assertEquals(List.of(value1, value2), consumptionDetails.get(key1));

        addRecordInShareConsumption(key2, value3, consumptionDetails);
        assertEquals(2, consumptionDetails.size());
        assertEquals(List.of(value1, value2), consumptionDetails.get(key1));
        assertEquals(List.of(value3), consumptionDetails.get(key2));
    }

    @Test
    public void testAddOffsetsDetails() {
        TopicPartition topicPartition1 = new TopicPartition("topic1", 0);
        TopicPartition topicPartition2 = new TopicPartition("topic1", 1);
        ConcurrentHashMap<TopicPartition, TreeSet<Long>> offsetDetails = new ConcurrentHashMap<>();

        addOffsetsDetails(topicPartition1, 0L, offsetDetails);
        assertEquals(1, offsetDetails.size());
        assertTrue(offsetDetails.containsKey(topicPartition1));
        assertEquals(Set.of(0L), offsetDetails.get(topicPartition1));

        // Re-insert 0L to topicPartition1 should not cause any issues
        addOffsetsDetails(topicPartition1, 0L, offsetDetails);
        assertEquals(1, offsetDetails.size());
        assertTrue(offsetDetails.containsKey(topicPartition1));
        assertEquals(Set.of(0L), offsetDetails.get(topicPartition1));

        addOffsetsDetails(topicPartition1, 2L, offsetDetails);
        assertEquals(1, offsetDetails.size());
        assertTrue(offsetDetails.containsKey(topicPartition1));
        assertEquals(Set.of(0L, 2L), offsetDetails.get(topicPartition1));

        addOffsetsDetails(topicPartition1, 1L, offsetDetails);
        assertEquals(1, offsetDetails.size());
        assertTrue(offsetDetails.containsKey(topicPartition1));
        assertEquals(Set.of(0L, 1L, 2L), offsetDetails.get(topicPartition1));

        addOffsetsDetails(topicPartition2, 0L, offsetDetails);
        assertEquals(2, offsetDetails.size());
        assertTrue(offsetDetails.containsKey(topicPartition1));
        assertTrue(offsetDetails.containsKey(topicPartition2));
        assertEquals(Set.of(0L, 1L, 2L), offsetDetails.get(topicPartition1));
        assertEquals(Set.of(0L), offsetDetails.get(topicPartition2));
    }

    @Test
    public void testMissingOffsetsInConsumedRecords() {
        TopicPartition topicPartition1 = new TopicPartition("topic1", 0);
        TopicPartition topicPartition2 = new TopicPartition("topic1", 1);
        TopicPartition topicPartition3 = new TopicPartition("topic1", 2);
        ConcurrentHashMap<TopicPartition, TreeSet<Long>> offsetDetails = new ConcurrentHashMap<>();
        TreeSet<Long> offsets1 = new TreeSet<>(Set.of(0L, 7L, 2L, 3L, 10L, 11L, 12L, 14L, 15L));
        TreeSet<Long> offsets2 = new TreeSet<>(Set.of(0L));
        TreeSet<Long> offsets3 = new TreeSet<>(Set.of(0L, 3L, 2L, 1L, 4L, 5L, 6L, 7L, 8L, 9L));

        offsetDetails.put(topicPartition1, offsets1);
        offsetDetails.put(topicPartition2, offsets2);
        offsetDetails.put(topicPartition3, offsets3);

        HashMap<TopicPartition, List<Long>> missingOffsets = missingOffsetsInConsumedRecords(offsetDetails, 2);
        System.out.println(missingOffsets);
    }
}
