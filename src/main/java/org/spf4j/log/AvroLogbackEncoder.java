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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.io.ExtendedJsonEncoder;
import org.apache.avro.specific.ExtendedSpecificDatumWriter;
import org.apache.avro.util.Arrays;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.MinimalPrettyPrinter;
import org.spf4j.base.Strings;
import org.spf4j.base.Throwables;
import org.spf4j.base.avro.LogRecord;
import org.spf4j.io.ByteArrayBuilder;

/**
 *
 * @author Zoltan Farkas
 */
public class AvroLogbackEncoder extends EncoderBase<ILoggingEvent> implements LifeCycle {

  private final ExtendedSpecificDatumWriter<LogRecord> writer;

  private final ByteArrayBuilder bab;

  private  JsonGenerator gen;

  public AvroLogbackEncoder() {
    writer = new ExtendedSpecificDatumWriter(LogRecord.class);
    bab = new ByteArrayBuilder(128);
    gen = createJsonGen(bab);
  }

  private  JsonGenerator createJsonGen(final ByteArrayBuilder bab) {
    JsonGenerator agen;
    try {
      agen = Schema.FACTORY.createJsonGenerator(bab, JsonEncoding.UTF8);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
    agen.setPrettyPrinter(new MinimalPrettyPrinter(Strings.EOL) {
      @Override
      public void writeArrayValueSeparator(final JsonGenerator jg) throws IOException, JsonGenerationException {
        jg.writeRaw(',');
        jg.writeRaw(Strings.EOL);
        jg.writeRaw('\t');
      }


      @Override
      public void beforeArrayValues(final JsonGenerator jg) throws IOException, JsonGenerationException {
        jg.writeRaw(Strings.EOL);
        jg.writeRaw('\t');
      }
    });
    return agen;
  }

  @Override
  public byte[] headerBytes() {
    return Arrays.EMPTY_BYTE_ARRAY;
  }

  @Override
  public synchronized byte[] encode(final ILoggingEvent event) {
    LogRecord record = null;
    try {
      record = Converters.convert(event);
      return serializeAvro(record);
    } catch (IOException ex) {
      System.err.append("\nFailed to log: " + record + '\n');
      try {
        gen.flush();
      } catch (IOException ex1) {
       throw new UncheckedIOException(ex1);
      }
      System.err.append("\nWritten: " + new String(bab.toByteArray()) + '\n');
      Throwables.writeTo(ex, System.err, Throwables.PackageDetail.SHORT);
      gen = createJsonGen(bab);
      throw new UncheckedIOException(ex);
    } catch (RuntimeException ex) {
      System.err.append("\nFailed to log " + record + '\n');
      try {
        gen.flush();
      } catch (IOException ex1) {
       throw new UncheckedIOException(ex1);
      }
      System.err.append("\nWritten: " + new String(bab.toByteArray()) + '\n');
      Throwables.writeTo(ex, System.err, Throwables.PackageDetail.SHORT);
      gen = createJsonGen(bab);
      throw ex;
    }
  }

  public byte[] serializeAvro(final LogRecord record) throws IOException {
    ExtendedJsonEncoder encoder = new ExtendedJsonEncoder(LogRecord.getClassSchema(), gen);
    bab.reset();
    writer.write(record, encoder);
    encoder.flush();
    return bab.toByteArray();
  }

  @Override
  public byte[] footerBytes() {
    return Arrays.EMPTY_BYTE_ARRAY;
  }

}
