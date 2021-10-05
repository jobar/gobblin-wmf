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

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.writer.partitioner.TimeBasedWriterPartitioner;
import org.wikimedia.gobblin.TimestampedRecord;

import lombok.extern.slf4j.Slf4j;

/**
 * A {@link TimeBasedWriterPartitioner} for byte[] containing json.
 *
 * The json field that contains the timestamp can be specified using TIMESTAMP_COLUMN_KEY.
 * The format of the json field that contains the timestamp can be specified using TIMESTAMP_FORMAT_KEY.
 *
 * If a record doesn't contain the specified field, or if no field is specified, the current timestamp will be used.
 */
@Slf4j
public class TimestampedRecordTimeBasedWriterPartitioner<P> extends TimeBasedWriterPartitioner<TimestampedRecord<P>> {

    public TimestampedRecordTimeBasedWriterPartitioner(State state) {
        this(state, 1, 0);
    }

    public TimestampedRecordTimeBasedWriterPartitioner(State state, int numBranches, int branchId) {
        super(state, numBranches, branchId);
    }

    @Override
    public long getRecordTimestamp(TimestampedRecord<P> record) {
        // If a kafka timestamp is set, use it
        if (record.getTimestamp().isPresent()) {
            return record.getTimestamp().get();
        } else { // Otherwise return current timestamp
            return getNonKafkaTimestamp(record.getPayload());
        }
    }

    /**
     *
     * @param payload
     * @return
     */
    protected long getNonKafkaTimestamp(P payload) {
        return System.currentTimeMillis();
    }
}
