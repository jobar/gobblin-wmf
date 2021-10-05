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

package org.wikimedia.gobblin.publisher;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.publisher.DataPublisher;
import org.apache.gobblin.util.ParallelRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

/**
 * IMPORTANT NOTES:
 *  - Gobblin published folders are expected to be in the form {PUBLISHERDIR}/{TABLE_NAME}/{PARTITION}
 *    This means that the "writer.file.path.type" property is expected to be "tablename"
 *  - Gobblin writter partition-scheme is expected to follow time-order when sorted alphabetically
 *
 * Publisher output flag path: {PUBLISHERDIR}/{TABLE_NAME}/{PARTITION}/{FLAG}
 * For topics having crossed time-partitions boundary across all their kafka-partitions.
 *
 * The partitions to be flagged are processed by topic. The flag is written on every time-partition
 * folder having been written to by writers, and for which every kafka partition have also written
 * to a later partition.
 *
 */
@Slf4j
public class TimePartitionedFlagDataPublisher extends DataPublisher {

    public static final String PUBLISHER_PUBLISHED_FLAGS_KEY = ConfigurationKeys.DATA_PUBLISHER_PREFIX + ".published.flags";

    public static final String PUBLISHER_TIME_PARTITION_FLAG_KEY = ConfigurationKeys.DATA_PUBLISHER_PREFIX + ".timepartition.flag";
    public static final String DEFAULT_PUBLISHER_TIME_PARTITION_FLAG = "_IMPORTED";

    private final String flag;

    private final Closer closer = Closer.create();
    private final ParallelRunnerWithTouch parallelRunner;

    private final Set<Path> publishedFlags = Sets.newHashSet();

    public TimePartitionedFlagDataPublisher(State state) throws IOException {
        super(state);

        int parallelRunnerThreads = state.getPropAsInt(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY, ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS);
        this.flag = state.getProp(PUBLISHER_TIME_PARTITION_FLAG_KEY, DEFAULT_PUBLISHER_TIME_PARTITION_FLAG);
        log.info("Time-partition flag for dataset {} is: {}", state.getProp(ConfigurationKeys.DATASET_URN_KEY), flag);

        FileSystem publisherFs = getPublisherFileSystem(state);
        // This publisher writes empty files - no checksum needed
        publisherFs.setWriteChecksum(false);

        this.parallelRunner = new ParallelRunnerWithTouch(parallelRunnerThreads, publisherFs);
        this.closer.register(this.parallelRunner);
    }


    public static FileSystem getPublisherFileSystem(State state) throws IOException {
        Configuration conf = new Configuration();

        // Add all job configuration properties so they are picked up by Hadoop
        for (String key : state.getPropertyNames()) {
            conf.set(key, state.getProp(key));
        }
        URI writerUri = URI.create(state.getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, ConfigurationKeys.LOCAL_FS_URI));
        URI publisherUri = URI.create(state.getProp(ConfigurationKeys.DATA_PUBLISHER_FILE_SYSTEM_URI, writerUri.toString()));
        return FileSystem.get(publisherUri, conf);
    }

    @Deprecated
    @Override
    public void initialize() throws IOException {
    }

    @Override
    public void close() throws IOException {
        try {
            for (Path path : this.publishedFlags) {
                this.state.appendToSetProp(PUBLISHER_PUBLISHED_FLAGS_KEY, path.toString());
            }
        } finally {
            this.closer.close();
        }
    }

    /**
     * This function loops over workUnitStates and extracts for each processed table the time-partitions to flag as:
     *  - Any time-partition being less than the minimum across the maximum-time-partitions per table-partition.
     */
    private Map<String, Set<String>> getPartitionsToFlagByTable(Collection<? extends WorkUnitState> states) {
        List<String> tablesNotToFlag = new ArrayList<>();

        // This map contains, for valid tables:
        //  - The minimum of the maximum time-partitions across table-partitions
        //  - The list of written time-partitions whose value is before the minimum of maximums
        Map<String, Map.Entry<String, Set<String>>> tablesTimePartitions = new HashMap<>();

        // Loop over single-tasks state
        for (WorkUnitState workUnitState : states) {
            Preconditions.checkArgument(workUnitState.contains(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY));

            String tableName = workUnitState.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY);
            // Don't process tables having a failed task
            if (workUnitState.getWorkingState() == WorkUnitState.WorkingState.FAILED) {
                tablesTimePartitions.remove(tableName);
                tablesNotToFlag.add(tableName);
                log.debug(" Marking table {} as NOT to be flagged due to failed tasks", tableName);
            }

            // Process only committed tasks with actual data on accepted tables
            if (workUnitState.getWorkingState() == WorkUnitState.WorkingState.COMMITTED &&
                    workUnitState.getPropAsInt(ConfigurationKeys.WRITER_RECORDS_WRITTEN) > 0 &&
                    !tablesNotToFlag.contains(tableName)) {

                // loop over state properties to find written partitions and sort them in descending order
                TreeSet<String> writtenPartitions = new TreeSet<>(Collections.reverseOrder());
                for (Map.Entry<Object, Object> property : workUnitState.getProperties().entrySet()) {
                    if (((String) property.getKey()).startsWith(ConfigurationKeys.WRITER_PARTITION_PATH_KEY)) {
                        writtenPartitions.add((String) property.getValue());
                    }
                }

                // NOTE: From now on writtenPartitions contains partitions to be flagged,
                // as the first element has been removed.
                String writtenMax = writtenPartitions.pollFirst();
                // Only process time-partitions if there are some
                if (null != writtenMax) {
                    // Initialization of table data if not in map
                    if (!tablesTimePartitions.containsKey(tableName)) {
                        tablesTimePartitions.put(tableName, new AbstractMap.SimpleEntry<>(writtenMax, new HashSet<>(writtenPartitions)));
                    } else { // Merge writtenPartitions with existing time-partitions data for table
                        String tableMax = tablesTimePartitions.get(tableName).getKey();
                        Set<String> tablePartitionsToFlag = tablesTimePartitions.get(tableName).getValue();

                        // Add written partitions to flag to the set
                        tablePartitionsToFlag.addAll(writtenPartitions);
                        // Define new max (writtenMax can't be null as writtenPartitions
                        final String newMax = (writtenMax.compareTo(tableMax) < 0) ? writtenMax : tableMax;
                        // Clear set from partitions higher than newMax
                        tablePartitionsToFlag.removeIf(p -> p.compareTo(newMax) >= 0);

                        tablesTimePartitions.put(tableName, new AbstractMap.SimpleEntry<>(newMax, tablePartitionsToFlag));
                    }
                }
            }
        }
        Map<String, Set<String>> result = new HashMap<>();
        tablesTimePartitions.forEach((k, v) -> result.put(k, v.getValue()));

        return result;
    }

    /**
     * This function creates tasks to asynchronously write flags as defined in the tablesPartitions
     * parameter.
     * @param tablesPartitions A map of tables and their partitions to be flagged
     */
    private void writeFlags(Map<String, Set<String>> tablesPartitions) {
        String publisherFinalBaseDir = state.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR);
        // To validate that the folder to flag is coherent with published folders
        Set<String> publishedDirs = state.getPropAsSet(ConfigurationKeys.PUBLISHER_DIRS, "");

        for (Map.Entry<String, Set<String>> tableAndPartitions: tablesPartitions.entrySet()) {
            String table = tableAndPartitions.getKey();
            for (String partitionToFlag: tableAndPartitions.getValue()) {
                String pathToFlag = publisherFinalBaseDir + "/" + table + "/" + partitionToFlag;
                if (publishedDirs.contains(pathToFlag)) {
                    Path flagPath = new Path(pathToFlag, flag);
                    parallelRunner.touchPath(flagPath);
                    publishedFlags.add(flagPath);
                } else {
                    log.warn("Path-to-flag {} is not present in the list of published-directories", pathToFlag);
                }
            }
        }
    }

    @Override
    public void publishData(Collection<? extends WorkUnitState> states) throws IOException {

        log.debug("Compute time-partitions to flag");
        Map<String, Set<String>> tablesPartitions = getPartitionsToFlagByTable(states);

        log.debug("Write flags in time-partition folders");
        writeFlags(tablesPartitions);

        List<String> publishedFlagsString = this.publishedFlags.stream().map(Path::toString).collect(Collectors.toList());
        log.info(this.publishedFlags.size() + " time-partition flags published: " + publishedFlagsString);
    }

    @Override
    public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {
        // Nothing to do
    }

}