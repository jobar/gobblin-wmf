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

import static java.util.Locale.ENGLISH;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.stream.Collectors;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;


/**
 * A utility class parsing JSON from byte[] ans extracting a timestamp from the data.
 *
 * The class provides static functions to initialize both the timestamp and the
 * timestamp-format fields from a WriterPartitioner.
 *
 * If a record doesn't contain the specified field, or if no field is specified, the current timestamp will be used.
 *
 * Accepted formats are:
 * <ul>
 * <li>unix (same as unix_seconds)</li>
 * <li>unix_seconds</li>
 * <li>unix_milliseconds</li>
 * <li>ISO_8601</li>
 * <li>CUSTOM - Using the timestampCustomFormat parameter (can be empty for other values)</li>
 * </ul>
 *
 */
@Slf4j
public class JsonStringTimestampExtractor {

    private final List<String> timestampPointers;
    private final String timestampFormatStr;
    // Those two fields can't be made final due to initialization in try/catch
    private SimpleDateFormat timestampParser;
    private TimestampFormat timestampFormat;
    private final ObjectMapper cachingMapper;

    public JsonStringTimestampExtractor(List<String> timestampPointers, String timestampFormat) {
        // Convert the timestampPointer column from JSON to Jackson syntax
        this.timestampPointers = timestampPointers.stream()
                .map(s -> "/" + s.replaceAll("\\.", "/"))
                .collect(Collectors.toList());
        this.timestampFormatStr = timestampFormat;
        this.cachingMapper = new ObjectMapper();
        try {
            this.timestampFormat = TimestampFormat.valueOf(timestampFormatStr);
            this.timestampParser = null;
        } catch (IllegalArgumentException iae) {
            this.timestampFormat = TimestampFormat.CUSTOM;
            this.timestampParser = new SimpleDateFormat(timestampFormatStr, ENGLISH);
        } catch (NullPointerException npe) {
            throw new IllegalArgumentException("Parameter timestampFormat shouldn't be null", npe);
        }
    }

    public long getRecordTimestamp(String payload) {
        JsonNode root;
        long ifNotFound = System.currentTimeMillis();
        try {
            root = cachingMapper.readTree(payload);
        } catch (IOException ie) {
            log.warn("Couldn't parse the json payload for timestamp extraction: " + payload, ie);
            return ifNotFound;
        }
        for (String timestampPointer: timestampPointers) {
            try {
                JsonNode match = root.at(timestampPointer);

                if (match.canConvertToLong()) {
                    long found = match.asLong();
                    switch (timestampFormat) {
                        case unix:
                        case unix_seconds:
                            return found * 1000L;
                        case unix_milliseconds:
                            return found;
                    }
                } else if (match.isTextual()) {
                    String found = match.asText();
                    switch (timestampFormat) {
                        case ISO_8601:
                            return new DateTime(found).getMillis();
                        case CUSTOM:
                            return timestampParser.parse(found).getTime();
                    }
                }
                log.warn("No timestamp extracted from json payload: " + payload);
            } catch (Exception ex) {
                log.warn("Failed to extract timestamp from json payload: " + payload, ex);
            }
        }
        return ifNotFound;
    }

    public enum TimestampFormat {
        unix,
        unix_seconds,
        unix_milliseconds,
        ISO_8601,
        CUSTOM
    }

}
