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


package org.wikimedia.gobblin.writer.partitioner;

import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;
import org.wikimedia.gobblin.utils.JsonStringTimestampExtractor;
import org.apache.gobblin.writer.partitioner.TimeBasedWriterPartitioner;


/**
 * A {@link TimeBasedWriterPartitioner} for Timestamped byte[] records whose payload contains json.
 * This class is a superclass of {@link TimestampedRecordTimeBasedWriterPartitioner} overriding the
 * {@getNonKafkaTimestamp} method only.
 *
 * This class uses {@link JsonStringTimestampExtractor} to parse the Json and extract the timestamp.
 * It takes advantage of {@link JsonStringTimeBasedWriterPartitioner} configuration keys for timestamp
 * column and timestamp format, as well as static classes to extract their values.
 *
 * Note: If a record doesn't contain the specified field, or if no field is specified,
 *       the current timestamp will be used.
 */
@Slf4j
public class TimestampedRecordOrJsonStringTimeBasedWriterPartitioner extends TimestampedRecordTimeBasedWriterPartitioner<String> {

    private final JsonStringTimestampExtractor jsonStringTimestampExtractor;

    public TimestampedRecordOrJsonStringTimeBasedWriterPartitioner(State state) {
        this(state, 1, 0);
    }

    public TimestampedRecordOrJsonStringTimeBasedWriterPartitioner(State state, int numBranches, int branchId) {
        super(state, numBranches, branchId);

        this.jsonStringTimestampExtractor = new JsonStringTimestampExtractor(
                JsonStringTimeBasedWriterPartitioner.getWriterPartitionerTimestampColumns(state, numBranches, branchId),
                JsonStringTimeBasedWriterPartitioner.getWriterPartitionerTimestampFormat(state, numBranches, branchId)
        );
    }

    @Override
    protected long getNonKafkaTimestamp(String payload) {
        return jsonStringTimestampExtractor.getRecordTimestamp(payload);
    }
}
