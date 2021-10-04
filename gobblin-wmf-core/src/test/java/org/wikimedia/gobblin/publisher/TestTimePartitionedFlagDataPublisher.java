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

import com.google.common.io.Files;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

@Test
public class TestTimePartitionedFlagDataPublisher {

    private String publisherDir;
    private List<File> publishedDirs;
    private List<File> publishedDirsToFlag;
    private List<File> publishedDirsNotToFlag;

    private void prepareTestDirs() {
        File publishPath = Files.createTempDir();
        this.publisherDir = publishPath.getAbsolutePath();
        this.publishedDirs = new ArrayList<>();
        this.publishedDirsToFlag = new ArrayList<>();
        this.publishedDirsNotToFlag = new ArrayList<>();

        this.publishedDirsToFlag.add(addPublishedDir("t1/2021/05/01"));
        this.publishedDirsNotToFlag.add(addPublishedDir("t1/2021/05/02"));
        this.publishedDirsNotToFlag.add(addPublishedDir("t1/2021/05/03"));

        this.publishedDirsNotToFlag.add(addPublishedDir("t2/2021/05/01"));
        this.publishedDirsNotToFlag.add(addPublishedDir("t2/2021/05/02"));

        this.publishedDirsNotToFlag.add(addPublishedDir("t3/2021/05/01"));
        this.publishedDirsNotToFlag.add(addPublishedDir("t3/2021/05/02"));

        this.publishedDirsToFlag.add(addPublishedDir("t4/2021/05/01"));
        this.publishedDirsToFlag.add(addPublishedDir("t4/2021/05/02"));
        this.publishedDirsNotToFlag.add(addPublishedDir("t4/2021/05/03"));
        this.publishedDirsNotToFlag.add(addPublishedDir("t4/2021/05/04"));
    }

    private File addPublishedDir(String relativePath) {
        File publishedDir = new File(this.publisherDir, relativePath);
        Assert.assertTrue(publishedDir.mkdirs());
        this.publishedDirs.add(publishedDir);
        return publishedDir;
    }

    private State prepareMainState() {
        State mainState = new State();
        mainState.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, this.publisherDir);
        // Only set parent publisherDir for t1 (others are done in the children tasks)
        for (File publishedDir: this.publishedDirs){
            mainState.appendToSetProp(ConfigurationKeys.PUBLISHER_DIRS, publishedDir.getAbsolutePath());
        }
        return mainState;
    }

    private List<WorkUnitState> prepareWorkUnitStates() {
        return Arrays.asList(
                // t1 should be in the table list and have partition "2021/05/01" to flag
                buildWorkUnitState(WorkUnitState.WorkingState.COMMITTED, "t1", Arrays.asList("2021/05/01", "2021/05/02", "2021/05/03")),
                buildWorkUnitState(WorkUnitState.WorkingState.COMMITTED, "t1", Collections.singletonList("2021/05/02")),
                // t2 should be in the table list but with an empty list of partitions
                buildWorkUnitState(WorkUnitState.WorkingState.COMMITTED, "t2", Collections.singletonList("2021/05/02")),
                buildWorkUnitState(WorkUnitState.WorkingState.COMMITTED, "t2", Collections.singletonList("2021/05/01")),
                // t3 should not show in tables to flag as it has a FAILED task
                buildWorkUnitState(WorkUnitState.WorkingState.FAILED, "t3", Collections.singletonList("2021/05/01")),
                buildWorkUnitState(WorkUnitState.WorkingState.COMMITTED, "t3", Arrays.asList("2021/05/01", "2021/05/02")),
                // t4 should be in the table list and have partition "2021/05/01" and "2021/05/02" to flag
                buildWorkUnitState(WorkUnitState.WorkingState.COMMITTED, "t4", Arrays.asList("2021/05/02", "2021/05/04", "2021/05/01")),
                buildWorkUnitState(WorkUnitState.WorkingState.COMMITTED, "t4", Collections.singletonList("2021/05/03")),
                buildWorkUnitState(WorkUnitState.WorkingState.COMMITTED, "t4", Arrays.asList("2021/05/04", "2021/05/03"))
        );
    }

    private WorkUnitState buildWorkUnitState(WorkUnitState.WorkingState workingState, String tableName, List<String> partitions) {
        WorkUnitState state = new WorkUnitState(WorkUnit.createEmpty(), new State());
        state.setWorkingState(workingState);
        state.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, tableName);
        for (int i = 0; i < partitions.size(); i++) {
            String partition = partitions.get(i);
            String relativePath = tableName + "/" + partition;
            state.setProp(ConfigurationKeys.WRITER_RECORDS_WRITTEN, 10);
            state.setProp(ConfigurationKeys.WRITER_PARTITION_PATH_KEY + "_" + i, partition);
        }

        return state;
    }

    private void checkFlaggedDirs() {
        // Check folders that shouldn't have been flagged are empty
        for (File dirNotFlagged: this.publishedDirsNotToFlag) {
            Assert.assertEquals(dirNotFlagged.list().length, 0,
                    "Directory " + dirNotFlagged.getAbsolutePath() + " length");
        }
        // Check folders that should have been flagged contain the flag
        for (File dirFlagged: this.publishedDirsToFlag) {
            String[] dirFiles = dirFlagged.list();
            Assert.assertEquals(dirFiles.length, 1,
                    "Directory " + dirFlagged.getAbsolutePath() + " length");
            Assert.assertEquals(dirFiles[0], "_IMPORTED",
                    "Directory " + dirFlagged.getAbsolutePath() + " file");
        }
    }

    public void testTimePartitionedFlagDataPublisher() throws IOException {
        prepareTestDirs();
        State mainState = prepareMainState();
        List<? extends WorkUnitState> childrenStates = prepareWorkUnitStates();

        TimePartitionedFlagDataPublisher timePartitionedFlagDataPublisher  = new TimePartitionedFlagDataPublisher(mainState);
        timePartitionedFlagDataPublisher.publishData(childrenStates);
        // Force closing to materialize async actions
        timePartitionedFlagDataPublisher.close();

        checkFlaggedDirs();
    }
}