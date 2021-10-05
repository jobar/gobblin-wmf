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

package org.wikimedia.gobblin.utils;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link JsonStringTimestampExtractor}.
 */
@Test
public class TestJsonStringTimestampExtractor {

    private JsonStringTimestampExtractor timestampExtractor;
    private static final List<String> tsColumn = Arrays.asList("meta.dt");

    @Test
    public void testUnixExtractor() {
        timestampExtractor = new JsonStringTimestampExtractor(tsColumn, "unix");
        Assert.assertEquals(timestampExtractor.getRecordTimestamp("{\"meta\": {\"dt\": 1420099200}}"), 1420099200000L);
    }

    @Test
    public void testUnixSecondsExtractor() {
        timestampExtractor = new JsonStringTimestampExtractor(tsColumn, "unix_seconds");
        Assert.assertEquals(timestampExtractor.getRecordTimestamp("{\"meta\": {\"dt\": 1420099200}}"), 1420099200000L);
    }

    @Test
    public void testUnixMillisecondsExtractor() {
        timestampExtractor = new JsonStringTimestampExtractor(tsColumn, "unix_milliseconds");
        Assert.assertEquals(timestampExtractor.getRecordTimestamp("{\"meta\": {\"dt\": 1420099200000}}"), 1420099200000L);
    }

    @Test
    public void testIso8601Extractor() {
        timestampExtractor = new JsonStringTimestampExtractor(tsColumn, "ISO_8601");
        Assert.assertEquals(timestampExtractor.getRecordTimestamp("{\"meta\": {\"dt\": \"2015-01-01T08:00:00.000Z\"}}"), 1420099200000L);
    }

    @Test
    public void testCustomExtractor() {
        timestampExtractor = new JsonStringTimestampExtractor(tsColumn, "dd/MM/yyyy HH:mm:ssX");
        Assert.assertEquals(timestampExtractor.getRecordTimestamp("{\"meta\": {\"dt\": \"01/01/2015 08:00:00Z\"}}"), 1420099200000L);
    }

    @Test
    public void testUnixMultiFieldExtractor() {
        timestampExtractor = new JsonStringTimestampExtractor(Arrays.asList("meta.wrong", "meta.dt"), "unix");
        Assert.assertEquals(timestampExtractor.getRecordTimestamp("{\"meta\": {\"dt\": 1420099200}}"), 1420099200000L);
    }

    @Test
    public void testWrongColumnExtractor() {
        // Check that returned timestamp is current
        long currentTime = System.currentTimeMillis();
        timestampExtractor = new JsonStringTimestampExtractor(Arrays.asList("meta.wrong"), "unix_milliseconds");
        Assert.assertTrue(timestampExtractor.getRecordTimestamp("{\"meta\": {\"dt\": 1420099200000}}") >= currentTime);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullFormatExtractor() {
        timestampExtractor = new JsonStringTimestampExtractor(tsColumn, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testWrongFormatExtractor() {
        timestampExtractor = new JsonStringTimestampExtractor(tsColumn, "wrong format");
    }

}
