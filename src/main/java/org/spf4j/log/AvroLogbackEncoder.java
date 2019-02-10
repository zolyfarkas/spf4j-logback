/*
 * Copyright 2019 SPF4J.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.spf4j.log;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.encoder.EncoderBase;
import ch.qos.logback.core.spi.LifeCycle;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.ExtendedJsonEncoder;
import org.apache.avro.specific.ExtendedSpecificDatumWriter;
import org.apache.avro.util.Arrays;
import org.spf4j.base.avro.LogRecord;
import org.spf4j.io.ByteArrayBuilder;

/**
 *
 * @author Zoltan Farkas
 */
public class AvroLogbackEncoder extends EncoderBase<ILoggingEvent> implements LifeCycle {

  private final DatumWriter<LogRecord> writer;

  private final Encoder encoder;

  private final ByteArrayBuilder bab;

  public AvroLogbackEncoder() {
    writer = new ExtendedSpecificDatumWriter(LogRecord.class);
    bab = new ByteArrayBuilder(128);
    try {
      encoder = new ExtendedJsonEncoder(LogRecord.getClassSchema(), bab);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  @Override
  public byte[] headerBytes() {
    return Arrays.EMPTY_BYTE_ARRAY;
  }

  @Override
  public byte[] encode(final ILoggingEvent event) {
    try {
      bab.reset();
      LogRecord record = Converters.convert(event);
      writer.write(record, encoder);
      encoder.flush();
      return bab.toByteArray();
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  @Override
  public byte[] footerBytes() {
    return Arrays.EMPTY_BYTE_ARRAY;
  }

}
