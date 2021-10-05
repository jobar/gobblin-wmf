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

package org.wikimedia.gobblin.converter;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.wikimedia.gobblin.TimestampedRecord;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;


public class TimestampedRecordConverterWrapper<I, O>
        extends Converter<String, String, TimestampedRecord<I>, TimestampedRecord<O>> {

    public static final String CONVERTER_TIMESTAMPED_RECORD_WRAPPED = "converter.timestampedrecord.wrapped.class";

    private Converter<String, String, I, O> wrappedConverter;

    @Override
    public Converter<String, String, TimestampedRecord<I>, TimestampedRecord<O>> init(WorkUnitState workUnit) {
        String wrappedConverterClassKey = ForkOperatorUtils.getPropertyNameForBranch(workUnit, CONVERTER_TIMESTAMPED_RECORD_WRAPPED);

        Preconditions.checkArgument(workUnit.contains(wrappedConverterClassKey),
                "The converter " + this.getClass().getName() + " cannot be used without setting the property "
                        + CONVERTER_TIMESTAMPED_RECORD_WRAPPED);

        try {
            wrappedConverter = (Converter) Class.forName(workUnit.getProp(wrappedConverterClassKey)).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Problem instantiating wrapper converter class: " +
                    workUnit.getProp(wrappedConverterClassKey), e);
        }

        return this;
    }

    @Override
    public String convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
        return inputSchema;
    }

    @Override
    public Iterable<TimestampedRecord<O>> convertRecord(String outputSchema, TimestampedRecord<I> inputRecord, WorkUnitState workUnit)
            throws DataConversionException {
        Iterable<O> convertedOutput = wrappedConverter.convertRecord(outputSchema, inputRecord.getPayload(), workUnit);
        Optional<Long> recordTimestamp = inputRecord.getTimestamp();
        return StreamSupport.stream(convertedOutput.spliterator(), false).
                map(outputItem -> new TimestampedRecord<O>(outputItem, recordTimestamp))
                .collect(Collectors.toList());
    }

}
