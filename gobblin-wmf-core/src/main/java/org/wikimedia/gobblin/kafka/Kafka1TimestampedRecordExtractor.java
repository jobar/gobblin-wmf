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

import com.google.common.base.Optional;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.ByteArrayBasedKafkaRecord;
import org.wikimedia.gobblin.copy.Kafka1ConsumerClient;
import org.apache.gobblin.kafka.client.KafkaConsumerRecord;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaExtractor;

import lombok.extern.slf4j.Slf4j;
import org.wikimedia.gobblin.TimestampedRecord;
import org.apache.kafka.common.record.TimestampType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Extracts {@link TimestampedRecord<P>} records from Kafka using Kafka1 client.
 * The extracted record timestamp is set to the kafka record timestamp if the kafka
 * record timestamp-type is in the list of extracted-timestamp-types defined using
 * the property source.kafka.extract.timestampTypes (accepted values are CreateTime
 * and LogAppendTime, or unset-property). If the timestamp is not extracted from the
 * kafka record, it is set to current-timestamp if the state property
 * source.kafka.extract.timestamp.defaultCurrent is set to true, and is absent otherwise.
 * @param <P> The type of the deserialized payload from the kafka record.
 */
@Slf4j
public class Kafka1TimestampedRecordExtractor<P> extends KafkaExtractor<Object, Object> {

    public static final String EXTRACTED_TIMESTAMP_TYPES_KEY = "source.kafka.extract.timestampTypes";
    public static final String CURRENT_TIMESTAMP_AS_DEFAULT_KEY = "source.kafka.extract.timestamp.defaultCurrent";

    private final List<TimestampType> extractedTimestampTypes;
    private final boolean useCurrentTimestampAsDefault;

    public Kafka1TimestampedRecordExtractor(WorkUnitState state) {
        super(state);
        this.extractedTimestampTypes = getExtractedTimestampTypes(state);
        this.useCurrentTimestampAsDefault = getUseCurrentTimestampAsDefault(state);
    }

    /**
     * Uses state EXTRACTED_TIMESTAMP_TYPES_KEY property to construct a list
     * of {@link TimestampType} defining the types of kafka-timestamps that
     * will lead to extracted records having kafka timestamp set.
     * Accepted values are CreateTime and LogAppendTime, or unset-property (leading
     * to all records having the default timestamp).
     *
     * @return The {@link TimestampType} list, empty if the property is not present.
     * @Throws NoSuchElementException in case of invalid {@link TimestampType}
     * @throws IllegalArgumentException if {@link TimestampType} is not of accepted values
     */
    private List<TimestampType> getExtractedTimestampTypes(WorkUnitState state)
            throws NoSuchElementException, IllegalArgumentException  {
        List<TimestampType> result = new ArrayList<>();
        if (state.contains(EXTRACTED_TIMESTAMP_TYPES_KEY)) {
            for (String timestampTypeName : state.getPropAsList(EXTRACTED_TIMESTAMP_TYPES_KEY)) {
                TimestampType timestampType = TimestampType.forName(timestampTypeName);
                if (timestampType == TimestampType.CREATE_TIME || timestampType == TimestampType.LOG_APPEND_TIME) {
                    result.add(timestampType);
                } else {
                    throw new IllegalArgumentException("Invalid TimestampType to extract: " + timestampType.toString() +
                            ". Accepted values are CreateTime and/or LogAppendTime");
                }
            }
        }
        return result;
    }

    /**
     * Uses state CURRENT_TIMESTAMP_AS_DEFAULT_KEY property to define if record timestamp
     * should be set to current-time if kafka timestamp can't be used (by opposition to be absent).
     *
     * @return The {@link TimestampType} list, empty if the property is not present.
     */
    private boolean getUseCurrentTimestampAsDefault(WorkUnitState state) throws NoSuchElementException {
        return  (state.contains(CURRENT_TIMESTAMP_AS_DEFAULT_KEY) &&
                state.getPropAsBoolean(CURRENT_TIMESTAMP_AS_DEFAULT_KEY));
    }

    /**
     * We need to override this protected method as the kafka timestamp information is not
     * available in the {@decodeRecord} abstract method.
     */
    @Override
    protected TimestampedRecord<P> decodeKafkaMessage(KafkaConsumerRecord message) throws DataRecordException, IOException {

        TimestampedRecord<P> record = null;
        if (message instanceof Kafka1ConsumerClient.Kafka1ConsumerRecord){
            Kafka1ConsumerClient.Kafka1ConsumerRecord typedRecord = (Kafka1ConsumerClient.Kafka1ConsumerRecord)message;
            // if value is null then this is a bad record that is returned for further error handling, so raise an error
            if (typedRecord.getValue() == null) {
                throw new DataRecordException("Could not decode Kafka record");
            }

            Optional<Long> timestamp = Optional.absent();
            if (extractedTimestampTypes.contains(typedRecord.getTimestampType())) {
                timestamp = Optional.of(typedRecord.getTimestamp());
            } else if (useCurrentTimestampAsDefault) {
                timestamp = Optional.of(System.currentTimeMillis());
            }
            return new TimestampedRecord(typedRecord.getValue(), timestamp);
        } else {
            throw new IllegalStateException(
                    "Unsupported KafkaConsumerRecord type. The record should be Kafka1ConsumerRecord but is " +
                    message.getClass().getCanonicalName());
        }
    }

    /**
     * Unreachable, as {@decodeKafkaMessage} is override not to use this method.
     */
    @Override
    protected Object decodeRecord(ByteArrayBasedKafkaRecord kafkaConsumerRecord) throws IOException {
        throw new IllegalArgumentException("Could not decode Kafka record");
    }

    // TODO: Implement schema getter option (for events!)
    @Override
    public Object getSchema() {
        return null;
    }

}
