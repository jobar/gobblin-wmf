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


import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.SourceState;
import org.wikimedia.gobblin.copy.KafkaSource;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaTopic;
import org.apache.gobblin.util.DatasetFilterUtils;
import org.wikimedia.eventutilities.core.event.EventStreamConfig;
import org.wikimedia.eventutilities.core.event.WikimediaDefaults;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public abstract class WikimediaKafkaSource<S, D> extends KafkaSource<S, D> {

    private static final String TOPIC_INCLUDE_LIST = "topic.include";
    private static final String TOPIC_EXCLUDE_LIST = "topic.exclude";
    private static final String EVENT_STREAM_CONFIG_URI = "event_stream_config.uri";
    private static final String EVENT_STREAM_CONFIG_IS_WMF_PROD = "event_stream_config.is_wmf_production";
    private static final String EVENT_STREAM_CONFIG_STREAM_NAMES = "event_stream_config.stream_names";
    private static final String EVENT_STREAM_CONFIG_SETTINGS_FILTERS = "event_stream_config.settings_filters";

    /**
     * Finds Kafka topics to ingest.  Defaults to a literal include list.  In absence of that,
     * uses Wikimedia's EventStreamConfig service to get a list of topics based on:
     *  - a configured service URI
     *  - a list of stream names to get topics from
     *  - a list of settings filters in the form key1:value1,key2:value2
     *
     * @param state Work unit state
     * @return List of configured topics
     */
    @Override
    protected List<KafkaTopic> getFilteredTopics(SourceState state) {
        // default to a literal include list
        List<Pattern> excludeTopicsPatterns = DatasetFilterUtils.getPatternList(state, TOPIC_EXCLUDE_LIST);
        List<Pattern> includeTopicPatterns = DatasetFilterUtils.getPatternList(state, TOPIC_INCLUDE_LIST);

        // fetch topics from Wikimedia's EventStreamConfig service
        if (includeTopicPatterns.size() == 0) {
            if (state.contains(EVENT_STREAM_CONFIG_URI)) {
                final String configUri = state.getProp(EVENT_STREAM_CONFIG_URI);

                // streamNames needs to be null if not defined, otherwise EventStreamConfig uses it
                // and returns an empty topics list.
                final List<String> streamNames;
                if (state.contains(EVENT_STREAM_CONFIG_STREAM_NAMES)) {
                    streamNames = state.getPropAsList(EVENT_STREAM_CONFIG_STREAM_NAMES);
                } else {
                    streamNames = null;
                }
                final List<String> settingsFilters = state.getPropAsList(EVENT_STREAM_CONFIG_SETTINGS_FILTERS, "");

                String logMessage = "Getting " + TOPIC_INCLUDE_LIST + " from EventStreamConfig at " + configUri;
                if (streamNames != null) {
                    logMessage += " for streams " + String.join(",", streamNames);
                }
                if (settingsFilters.size() > 0) {
                    logMessage += " with settings " + String.join(",", settingsFilters);
                }
                logMessage += ".";
                log.info(logMessage);

                // Use WikimediaDefault EventStreamConfig if the provided uri matches
                // the default one. This allows for http-routes to be set correctly
                // in HTTP headers, and prevent having to use a proxy when querying
                // the service from WMF production servers.
                final EventStreamConfig eventStreamConfig;
                if (configUri.equals(WikimediaDefaults.EVENT_STREAM_CONFIG_URI) &&
                        state.getPropAsBoolean(EVENT_STREAM_CONFIG_IS_WMF_PROD, false)) {
                    // Get WikimediaDefaults eventstream config
                    eventStreamConfig = WikimediaDefaults.EVENT_STREAM_CONFIG;
                } else {
                    // Get an EventStreamConfig instance using eventStreamConfigUri.
                    eventStreamConfig = EventStreamConfig.builder()
                            .setEventStreamConfigLoader(configUri)
                            // This is a hack to make EventStreamConfig work
                            // EventStreamConfig expects a map to configure
                            // to which service to produce event. As we're not
                            // producing events but just reading config, an
                            // empty map does the trick.
                            .setEventServiceToUriMap(Collections.EMPTY_MAP)
                            .build();
                }

                // Get the list of topics matching the target streamNames and settingsFilters.
                List<String> includeTopics = eventStreamConfig.collectTopicsMatchingSettings(streamNames, settingsListToMap(settingsFilters));
                if (includeTopics.isEmpty()) {
                    throw new RuntimeException("Failed to obtain topics to consume from EventStreamConfig at " + configUri);
                }
                includeTopicPatterns = DatasetFilterUtils.getPatternsFromStrings(includeTopics);
            } else {
                log.warn("No topic configured (no include-list nor eventStreamConfig)");
                return Collections.emptyList();
            }
        } else {
            log.info("Getting " + TOPIC_INCLUDE_LIST + " from include-list: " + state.getProp(TOPIC_INCLUDE_LIST));
        }

        return kafkaConsumerClient.get().getFilteredTopics(excludeTopicsPatterns, includeTopicPatterns);
    }

    protected Map<String, String> settingsListToMap(List<String> settingsFilters) {
        return settingsFilters.stream()
            .map(s -> s.split(":"))
            .collect(Collectors.toMap(kv -> kv[0], kv -> kv[1]));
    }
}