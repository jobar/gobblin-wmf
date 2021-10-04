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
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.wikimedia.gobblin.utils.JsonStringTimestampExtractor;
import org.apache.gobblin.writer.partitioner.TimeBasedWriterPartitioner;

import java.util.List;


/**
 * A {@link TimeBasedWriterPartitioner} for byte[] containing json.
 *
 * The json field that contains the timestamp can be specified using TIMESTAMP_COLUMN_KEY.
 * The format of the json field that contains the timestamp can be specified using TIMESTAMP_FORMAT_KEY.
 *
 * If a record doesn't contain the specified field, or if no field is specified, the current timestamp will be used.
 */
@Slf4j
public class JsonStringTimeBasedWriterPartitioner extends TimeBasedWriterPartitioner<String> {

    public static final String TIMESTAMP_COLUMNS_KEY = ConfigurationKeys.WRITER_PREFIX + ".partition.timestamp.columns";
    public static final String DEFAULT_TIMESTAMP_COLUMN = "timestamp";

    public static final String TIMESTAMP_FORMAT_KEY = ConfigurationKeys.WRITER_PREFIX + ".partition.timestamp.format";
    public static final String DEFAULT_TIMESTAMP_FORMAT = "ISO_8601";

    private final JsonStringTimestampExtractor jsonStringTimestampExtractor;

    public JsonStringTimeBasedWriterPartitioner(State state) {
        this(state, 1, 0);
    }

    public JsonStringTimeBasedWriterPartitioner(State state, int numBranches, int branchId) {
        super(state, numBranches, branchId);

        this.jsonStringTimestampExtractor = new JsonStringTimestampExtractor(
                getWriterPartitionerTimestampColumns(state, numBranches, branchId),
                getWriterPartitionerTimestampFormat(state, numBranches, branchId)
        );
    }

    @Override
    public long getRecordTimestamp(String record) {
        return jsonStringTimestampExtractor.getRecordTimestamp(record);
    }

    /**
     * Utility function facilitating getting a timestamp-column
     */
    public static List<String> getWriterPartitionerTimestampColumns(State state, int numBranches, int branchId) {
        String propName = ForkOperatorUtils.getPropertyNameForBranch(TIMESTAMP_COLUMNS_KEY, numBranches, branchId);
        List<String> prop = state.getPropAsList(propName, DEFAULT_TIMESTAMP_COLUMN);

        log.info("timestamp column for dataset {} is: {}", state.getProp(ConfigurationKeys.DATASET_URN_KEY), prop);
        return prop;
    }

    /**
     * Utility function facilitating getting a timestamp-format
     */
    public static String getWriterPartitionerTimestampFormat(State state, int numBranches, int branchId) {
        String propName = ForkOperatorUtils.getPropertyNameForBranch(TIMESTAMP_FORMAT_KEY, numBranches, branchId);
        String prop = state.getProp(propName, DEFAULT_TIMESTAMP_FORMAT);

        log.info("timestamp format for dataset {} is: {}", state.getProp(ConfigurationKeys.DATASET_URN_KEY), prop);
        return prop;
    }
}
