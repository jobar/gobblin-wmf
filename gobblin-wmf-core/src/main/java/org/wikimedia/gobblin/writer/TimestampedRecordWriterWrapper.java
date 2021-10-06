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
import org.apache.gobblin.writer.FsDataWriter;
import org.wikimedia.gobblin.TimestampedRecord;


/**
 * Copied and updated from SimpleDataWriter.
 */
public class TimestampedRecordWriterWrapper<P> implements DataWriter<TimestampedRecord<P>> {

    protected final DataWriter<P> wrappedWriter;

    /**
     * Initialize a new metadata wrapper.
     * @param wrappedWriter Writer to wrap
     */
    public TimestampedRecordWriterWrapper(DataWriter<P> wrappedWriter) {
        this.wrappedWriter = wrappedWriter;
    }

    @Override
    public void write(TimestampedRecord<P> untypedRecord) throws IOException {
        wrappedWriter.write((untypedRecord).getPayload());
    }

    @Override
    public void commit() throws IOException {
        wrappedWriter.commit();
    }

    @Override
    public void cleanup() throws IOException {
        wrappedWriter.cleanup();
    }

    @Override
    public long recordsWritten() {
        return wrappedWriter.recordsWritten();
    }

    @Override
    public long bytesWritten() throws IOException {
        return wrappedWriter.bytesWritten();
    }

    @Override
    public void close() throws IOException {
        wrappedWriter.close();
    }

    @Override
    public void flush() throws IOException {
        wrappedWriter.flush();
    }

    public String getOutputFilePath() {
        if (wrappedWriter instanceof FsDataWriter) {
            return ((FsDataWriter)wrappedWriter).getOutputFilePath();
        } else {
            throw new ClassCastException("Wrong class getting output file path: " +
                    wrappedWriter.getClass().getCanonicalName());
        }
    }
}
