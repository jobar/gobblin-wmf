/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.wikimedia.gobblin.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.kafka.common.record.TimestampType;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wikimedia.gobblin.TimestampedRecord;
import org.wikimedia.gobblin.copy.Kafka1ConsumerClient;

import com.google.common.base.Optional;

import lombok.extern.slf4j.Slf4j;


/**
 * Tests for {@link Kafka1TimestampedRecordExtractor}.
 */
@Slf4j
public class TestKafka1TimestampedRecordExtractor {

    private static final String TEST_TOPIC_NAME = "testTopic";
    private static final byte[] testPayload = "Hello World".getBytes(UTF_8);
    private static final long testTimestamp = 1420099200000L;
    private static final Optional<String> noString = Optional.absent();

    @Test
    public void testDecodeKafkaMessageWithoutTimestamp() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = TestUtils.getMockWorkUnitState(noString, noString, TEST_TOPIC_NAME);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> mockKafkaConsumerRecord =
                TestUtils.getMockKafkaConsumerRecord(TimestampType.NO_TIMESTAMP_TYPE, 0L, testPayload, TEST_TOPIC_NAME);

        TimestampedRecord<byte[]> expected = new TimestampedRecord<>(testPayload, Optional.absent());
        TimestampedRecord<byte[]> actual = kTRE.decodeKafkaMessage(mockKafkaConsumerRecord);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDecodeKafkaMessageWithoutTimestampButCurrentDefault() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = TestUtils.getMockWorkUnitState(noString, Optional.of("true"), TEST_TOPIC_NAME);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> mockKafkaConsumerRecord =
                TestUtils.getMockKafkaConsumerRecord(TimestampType.NO_TIMESTAMP_TYPE, 0L, testPayload, TEST_TOPIC_NAME);

        long before = System.currentTimeMillis();
        TimestampedRecord<byte[]> actual = kTRE.decodeKafkaMessage(mockKafkaConsumerRecord);
        Assert.assertTrue(actual.getTimestamp().isPresent());
        Assert.assertTrue(actual.getTimestamp().get() >= before);
    }

    @Test
    public void testDecodeKafkaMessageWitTimestampButNoAcceptedType() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = TestUtils.getMockWorkUnitState(noString, noString, TEST_TOPIC_NAME);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> mockKafkaConsumerRecord =
                TestUtils.getMockKafkaConsumerRecord(TimestampType.CREATE_TIME, testTimestamp, testPayload, TEST_TOPIC_NAME);

        TimestampedRecord<byte[]> expected = new TimestampedRecord<>(testPayload, Optional.absent());
        TimestampedRecord<byte[]> actual = kTRE.decodeKafkaMessage(mockKafkaConsumerRecord);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDecodeKafkaMessageWitTimestampAndAcceptedType() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = TestUtils.getMockWorkUnitState(Optional.of("CreateTime"), noString, TEST_TOPIC_NAME);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> mockKafkaConsumerRecord =
                TestUtils.getMockKafkaConsumerRecord(TimestampType.CREATE_TIME, testTimestamp, testPayload, TEST_TOPIC_NAME);

        TimestampedRecord<byte[]> expected = new TimestampedRecord<>(testPayload, Optional.of(testTimestamp));
        TimestampedRecord<byte[]> actual = kTRE.decodeKafkaMessage(mockKafkaConsumerRecord);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDecodeKafkaMessageWitTimestampAndAcceptedTypeList() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = TestUtils.getMockWorkUnitState(Optional.of("CreateTime,LogAppendTime"), noString, TEST_TOPIC_NAME);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> mockKafkaConsumerRecord =
                TestUtils.getMockKafkaConsumerRecord(TimestampType.CREATE_TIME, testTimestamp, testPayload, TEST_TOPIC_NAME);

        TimestampedRecord<byte[]> expected = new TimestampedRecord<>(testPayload, Optional.of(testTimestamp));
        TimestampedRecord<byte[]> actual = kTRE.decodeKafkaMessage(mockKafkaConsumerRecord);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDecodeKafkaMessageWitTimestampAndAcceptedTypeList2() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = TestUtils.getMockWorkUnitState(Optional.of("CreateTime,LogAppendTime"), noString, TEST_TOPIC_NAME);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> mockKafkaConsumerRecord =
                TestUtils.getMockKafkaConsumerRecord(TimestampType.LOG_APPEND_TIME, testTimestamp, testPayload, TEST_TOPIC_NAME);

        TimestampedRecord<byte[]> expected = new TimestampedRecord<>(testPayload, Optional.of(testTimestamp));
        TimestampedRecord<byte[]> actual = kTRE.decodeKafkaMessage(mockKafkaConsumerRecord);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDecodeKafkaMessageWithoutTimestampAndAcceptedType() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = TestUtils.getMockWorkUnitState(Optional.of("CreateTime"), noString, TEST_TOPIC_NAME);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> mockKafkaConsumerRecord =
                TestUtils.getMockKafkaConsumerRecord(TimestampType.NO_TIMESTAMP_TYPE, 0L, testPayload, TEST_TOPIC_NAME);

        TimestampedRecord<byte[]> expected = new TimestampedRecord<>(testPayload, Optional.absent());
        TimestampedRecord<byte[]> actual = kTRE.decodeKafkaMessage(mockKafkaConsumerRecord);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDecodeKafkaMessageWithoutTimestampAndAcceptedTypeAndCurrentTimestampDefault()
            throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = TestUtils.getMockWorkUnitState(Optional.of("CreateTime"), Optional.of("true"), TEST_TOPIC_NAME);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        Kafka1ConsumerClient.Kafka1ConsumerRecord<Object, Object> mockKafkaConsumerRecord =
                TestUtils.getMockKafkaConsumerRecord(TimestampType.NO_TIMESTAMP_TYPE, 0L, testPayload, TEST_TOPIC_NAME);

        long before = System.currentTimeMillis();
        TimestampedRecord<byte[]> actual = kTRE.decodeKafkaMessage(mockKafkaConsumerRecord);
        Assert.assertTrue(actual.getTimestamp().isPresent());
        Assert.assertTrue(actual.getTimestamp().get() >= before);
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testWrongExtractedTimestampType() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = TestUtils.getMockWorkUnitState(Optional.of("Wrong"), noString, TEST_TOPIC_NAME);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidExtractedTimestampType() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = TestUtils.getMockWorkUnitState(Optional.of("NoTimestampType"), noString, TEST_TOPIC_NAME);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testFailDecodeKafkaRecord() throws IOException, DataRecordException {
        WorkUnitState mockWorkUnitState = TestUtils.getMockWorkUnitState(Optional.of("CreateTime,LogAppendTime"), noString, TEST_TOPIC_NAME);
        Kafka1TimestampedRecordExtractor<byte[]> kTRE = new Kafka1TimestampedRecordExtractor(mockWorkUnitState);

        kTRE.decodeRecord(null);
    }

}
