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

import java.io.IOException;

import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;
import org.apache.gobblin.writer.MetadataWriterWrapper;
import org.wikimedia.gobblin.TimestampedRecord;

import lombok.extern.slf4j.Slf4j;

/**
 * A {@link DataWriterBuilder} for building {@link DataWriter} that writes a
 * {@link TimestampedRecord} containing a byte[] payload.
 */
@Slf4j
public class TimestampedStringRecordDataWriterBuilder extends SimpleStringWriterBuilder {

    /**
     * Build a {@link org.apache.gobblin.writer.DataWriter}.
     *
     * @return the built {@link org.apache.gobblin.writer.DataWriter}
     * @throws java.io.IOException if there is anything wrong building the writer
     */
    @Override
    public DataWriter<Object> build() throws IOException {
        return
                new MetadataWriterWrapper<TimestampedRecord<String>>(
                        new TimestampedRecordWriterWrapper<String>(
                                new SimpleStringWriter(this, this.destination.getProperties())),
                        (Class<TimestampedRecord<String>>) ((Class)TimestampedRecord.class),
                        this.branches,
                        this.branch,
                        this.destination.getProperties());
    }
}
