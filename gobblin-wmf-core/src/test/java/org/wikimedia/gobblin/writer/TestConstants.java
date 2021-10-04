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

package org.wikimedia.gobblin.writer;

/**
 * Copied and modified from gobblin-core:org.apache.gobblin.writer.TestConstants
 */
public class TestConstants {

    public static final String[] JSON_RECORDS =
            {"{\"name\": \"Alyssa\", \"favorite_number\": 256, \"favorite_color\": \"yellow\"}", "{\"name\": \"Ben\", \"favorite_number\": 7, \"favorite_color\": \"red\"}", "{\"name\": \"Charlie\", \"favorite_number\": 68, \"favorite_color\": \"blue\"}"};

    public static final String TEST_FS_URI = "file://localhost/";

    public static final String TEST_ROOT_DIR = "test";

    public static final String TEST_STAGING_DIR = TEST_ROOT_DIR + "/staging";

    public static final String TEST_OUTPUT_DIR = TEST_ROOT_DIR + "/output";

    public static final String TEST_FILE_NAME = "test.txt";

    public static final String TEST_WRITER_ID = "writer-1";

    public static final String TEST_EXTRACT_NAMESPACE = "org.wikimedia.writer.test";

    public static final String TEST_EXTRACT_ID = String.valueOf(System.currentTimeMillis());

    public static final String TEST_EXTRACT_TABLE = "TestTable";

    public static final String TEST_EXTRACT_PULL_TYPE = "FULL";

}