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

import com.google.common.base.Optional;
import org.apache.commons.io.IOUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.crypto.EncryptionConfigParser;
import org.apache.gobblin.crypto.EncryptionFactory;
import org.wikimedia.gobblin.TimestampedRecord;
import org.apache.gobblin.writer.Destination;
import org.apache.gobblin.writer.WriterOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.*;
import java.net.URI;
import java.util.Collections;
import java.util.zip.GZIPInputStream;

/**
 * Tests for {@link TimestampedRecordWriterWrapper}
 * Copied and updated from gobblin-core:org.apache.gobblin.writer.SimpleDataWriterTest
 */
@Test
public class TestTimestampedStringRecordWriter {

    private String filePath;
    private final String schema = "";
    private final int newLine = "\n".getBytes()[0];
    private State properties;
    private static final String ENCRYPT_PREFIX = "writer.encrypt.";

    @BeforeMethod
    public void setUp() throws Exception {
        properties = new State();

        // Making the staging and/or output dirs if necessary
        File stagingDir = new File(TestConstants.TEST_STAGING_DIR);
        File outputDir = new File(TestConstants.TEST_OUTPUT_DIR);
        if (!stagingDir.exists()) {
            stagingDir.mkdirs();
        }
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        this.filePath = TestConstants.TEST_EXTRACT_NAMESPACE.replaceAll("\\.", "/") + "/" + TestConstants.TEST_EXTRACT_TABLE
                + "/" + TestConstants.TEST_EXTRACT_ID + "_" + TestConstants.TEST_EXTRACT_PULL_TYPE;

        properties.setProp(ConfigurationKeys.SIMPLE_WRITER_DELIMITER, "\n");
        properties.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, TestConstants.TEST_FS_URI);
        properties.setProp(ConfigurationKeys.WRITER_STAGING_DIR, TestConstants.TEST_STAGING_DIR);
        properties.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, TestConstants.TEST_OUTPUT_DIR);
        properties.setProp(ConfigurationKeys.WRITER_FILE_PATH, this.filePath);
        properties.setProp(ConfigurationKeys.WRITER_FILE_NAME, TestConstants.TEST_FILE_NAME);
        properties.setProp(ConfigurationKeys.SIMPLE_WRITER_PREPEND_SIZE, false);
    }

    private TimestampedRecord<String> makeTR(String payload) {
        return new TimestampedRecord<>(payload, Optional.absent());
    }
    /**
     * Test writing records without a delimiter and make sure it works.
     * @throws IOException
     */

    /**
     * Use the data writer to write random bytes to a file and ensure
     * they are the same when read back.
     *
     * @throws IOException
     */
    @Test
    public void testWriteRandomBytes() throws IOException {
        // Build a writer to write test records
        TimestampedRecordWriterWrapper writer = buildTimestampedRecordDataWriter();
        String rec1 = "abcd";
        String rec2 = "efghijk";
        String rec3 = "lmnopqrstuvwxyz";

        writer.write(makeTR(rec1));
        writer.write(makeTR(rec2));
        writer.write(makeTR(rec3));

        writer.close();
        writer.commit();

        Assert.assertEquals(writer.recordsWritten(), 3);
        Assert.assertEquals(writer.bytesWritten(), rec1.getBytes().length + rec2.getBytes().length + rec3.getBytes().length + 3); // 3 bytes for newline character

        File outputFile = new File(writer.getOutputFilePath());
        InputStream is = new FileInputStream(outputFile);
        int c, resNum = 0, resi = 0;
        byte[][] records = { rec1.getBytes(), rec2.getBytes(), rec3.getBytes() };
        while ((c = is.read()) != -1) {
            if (c != newLine) {
                Assert.assertEquals(c, records[resNum][resi]);
                resi++;
            } else {
                resNum++;
                resi = 0;
            }
        }
    }

    @Test
    public void testSupportsGzip() throws IOException {
        properties.setProp(ConfigurationKeys.WRITER_CODEC_TYPE, "gz");

        String toWrite = "abcd";

        TimestampedRecordWriterWrapper writer = buildTimestampedRecordDataWriter();

        writer.write(makeTR(toWrite));
        writer.close();
        writer.commit();

        File outputFile = new File(writer.getOutputFilePath());
        InputStream in = new GZIPInputStream(new FileInputStream(outputFile));

        String contents = new String(IOUtils.toByteArray(in));
        Assert.assertEquals(contents, toWrite + "\n", "Expected gzip'd content to be written out");
        Assert.assertTrue(outputFile.getName().endsWith(".gz"), "Expected gzip'd file to end in .gz");
    }

    @Test
    public void testSupportsGzipAndEncryption() throws IOException {
        final String ENCRYPTION_TYPE = "insecure_shift";
        final String COMPRESSION_TYPE = "gz";

        properties.setProp(ConfigurationKeys.WRITER_CODEC_TYPE, COMPRESSION_TYPE);
        properties.setProp(ENCRYPT_PREFIX + EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY,
                ENCRYPTION_TYPE);

        String toWrite = "abcd";

        TimestampedRecordWriterWrapper writer = buildTimestampedRecordDataWriter();

        writer.write(makeTR(toWrite));
        writer.close();
        writer.commit();

        File outputFile = new File(writer.getOutputFilePath());
        Assert.assertTrue(outputFile.getName().endsWith("." + COMPRESSION_TYPE + "." + ENCRYPTION_TYPE),
                "Expected compression & encryption in file name!");

        InputStream decryptedFile =
                EncryptionFactory.buildStreamCryptoProvider(ENCRYPTION_TYPE, Collections.<String, Object>emptyMap())
                        .decodeInputStream(new FileInputStream(outputFile));
        InputStream uncompressedFile = new GZIPInputStream(decryptedFile);

        String contents = new String(IOUtils.toByteArray(uncompressedFile));
        Assert.assertEquals(contents, toWrite + "\n", "expected to decode same contents");
    }

    /**
     * Use the writer to write json entries to a file and ensure that
     * they are the same when read back.
     *
     * @throws IOException
     */
    @Test
    public void testWrite() throws IOException {
        TimestampedRecordWriterWrapper writer = buildTimestampedRecordDataWriter();
        int totalBytes = 3; // 3 extra bytes for the newline character
        // Write all test records
        for (String record : TestConstants.JSON_RECORDS) {
            Assert.assertEquals(record.getBytes().length, record.length()); // ensure null byte does not get added to end
            writer.write(makeTR(record));
            totalBytes += record.getBytes().length;
        }

        writer.close();
        writer.commit();

        Assert.assertEquals(writer.recordsWritten(), 3);
        Assert.assertEquals(writer.bytesWritten(), totalBytes);

        File outputFile = new File(writer.getOutputFilePath());
        BufferedReader br = new BufferedReader(new FileReader(outputFile));
        String line;
        int lineNumber = 0;
        while ((line = br.readLine()) != null) {
            Assert.assertEquals(line, TestConstants.JSON_RECORDS[lineNumber]);
            lineNumber++;
        }
        br.close();
        Assert.assertEquals(lineNumber, 3);
    }

    private TimestampedRecordWriterWrapper<String> buildTimestampedRecordDataWriter()
            throws IOException {

        SimpleStringWriterBuilder builder = (SimpleStringWriterBuilder)new SimpleStringWriterBuilder()
                .writeTo(Destination.of(Destination.DestinationType.HDFS, properties)).writeInFormat(WriterOutputFormat.TXT)
                .withWriterId(TestConstants.TEST_WRITER_ID).withSchema(this.schema).forBranch(0);
        SimpleStringWriter wrappedWriter = new SimpleStringWriter(builder, properties);
        return new TimestampedRecordWriterWrapper<>(wrappedWriter);
    }

    /**
     * If the staging file exists, the data writer should overwrite its contents.
     *
     * @throws IOException
     */
    @Test
    public void testOverwriteExistingStagingFile() throws IOException {
        String randomStringStage = "staging string";
        String randomStringWrite = "to-write string";
        Path stagingFile = new Path(TestConstants.TEST_STAGING_DIR + Path.SEPARATOR + this.filePath,
                TestConstants.TEST_FILE_NAME + "." + TestConstants.TEST_WRITER_ID + "." + "tmp");
        Configuration conf = new Configuration();
        // Add all job configuration properties so they are picked up by Hadoop
        for (String key : properties.getPropertyNames()) {
            conf.set(key, properties.getProp(key));
        }
        FileSystem fs = FileSystem.get(URI.create(TestConstants.TEST_FS_URI), conf);

        OutputStream os = fs.create(stagingFile);
        os.write(randomStringStage.getBytes());
        os.flush();
        os.close();

        TimestampedRecordWriterWrapper writer = buildTimestampedRecordDataWriter();

        writer.write(makeTR(randomStringWrite));
        writer.close();
        writer.commit();

        Assert.assertEquals(writer.recordsWritten(), 1);
        Assert.assertEquals(writer.bytesWritten(), randomStringWrite.getBytes().length + 1);

        File writeFile = new File(writer.getOutputFilePath());
        int c, i = 0;
        InputStream is = new FileInputStream(writeFile);
        while ((c = is.read()) != -1) {
            if (i == 15) {
                Assert.assertEquals(c, (byte) newLine); // the last byte should be newline
                i++;
                continue;
            }
            Assert.assertEquals(randomStringWrite.getBytes()[i], c);
            i++;
        }
    }

    @AfterMethod
    public void tearDown() throws IOException {
        // Clean up the staging and/or output directories if necessary
        File testRootDir = new File(TestConstants.TEST_ROOT_DIR);
        if (testRootDir.exists()) {
            FileUtil.fullyDelete(testRootDir);
        }
    }

}