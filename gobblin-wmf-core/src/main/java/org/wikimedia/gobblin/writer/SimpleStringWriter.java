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
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.writer.FsDataWriter;

import com.google.common.base.Preconditions;

/**
 * Copied and updated from SimpleDataWriter.
 */
public class SimpleStringWriter extends FsDataWriter<String> {

    private final Byte recordDelimiter; // optional byte to place between each record write

    private int recordsWritten;
    private int bytesWritten;

    private final OutputStream stagingFileOutputStream;

    public SimpleStringWriter(SimpleStringWriterBuilder builder, State properties)
            throws IOException {
        super(builder, properties);

        Preconditions.checkArgument(properties.contains(ConfigurationKeys.SIMPLE_WRITER_DELIMITER),
                "The writer " + this.getClass().getName() +
                        " cannot be used without setting the property "
                        + ConfigurationKeys.SIMPLE_WRITER_DELIMITER);

        this.recordDelimiter = properties.getProp(ConfigurationKeys.SIMPLE_WRITER_DELIMITER)
                .getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING)[0];
        this.recordsWritten = 0;
        this.bytesWritten = 0;
        this.stagingFileOutputStream = createStagingFileOutputStream();

        setStagingFileGroup();
    }

    /**
     * Write a source record to the staging file.
     *
     * @param record data record to write
     * @throws java.io.IOException if there is anything wrong writing the record
     */
    @Override
    public void write(String record) throws IOException {
        Preconditions.checkNotNull(record);

        byte[] toWrite = record.getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING);
            toWrite = Arrays.copyOf(toWrite, toWrite.length + 1);
            toWrite[toWrite.length - 1] = this.recordDelimiter;
        this.stagingFileOutputStream.write(toWrite);
        this.bytesWritten += toWrite.length;
        this.recordsWritten++;
    }

    /**
     * Get the number of records written.
     *
     * @return number of records written
     */
    @Override
    public long recordsWritten() {
        return this.recordsWritten;
    }

    /**
     * Get the number of bytes written.
     *
     * @return number of bytes written
     */
    @Override
    public long bytesWritten() throws IOException {
        return this.bytesWritten;
    }

    @Override
    public boolean isSpeculativeAttemptSafe() {
        return this.writerAttemptIdOptional.isPresent() && this.getClass() == SimpleStringWriter.class;
    }

    /**
     * Flush the staging file.
     * @throws IOException
     */
    @Override
    public void flush() throws IOException {
        this.stagingFileOutputStream.flush();
    }
}
