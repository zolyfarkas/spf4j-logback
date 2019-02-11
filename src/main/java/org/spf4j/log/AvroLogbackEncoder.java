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
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import org.apache.avro.AvroNamesRefResolver;
import org.apache.avro.Schema;
import org.apache.avro.SchemaResolvers;
import org.apache.avro.io.ExtendedJsonEncoder;
import org.apache.avro.specific.ExtendedSpecificDatumWriter;
import org.apache.avro.util.Arrays;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.MinimalPrettyPrinter;
import org.spf4j.base.Json;
import org.spf4j.base.Strings;
import org.spf4j.base.avro.LogRecord;
import org.spf4j.io.ByteArrayBuilder;

/**
 *
 * @author Zoltan Farkas
 */
public class AvroLogbackEncoder extends EncoderBase<ILoggingEvent> implements LifeCycle {

  private final ExtendedSpecificDatumWriter<LogRecord> writer;

  private ExtendedJsonEncoder encoder;

  private final ByteArrayBuilder bab;

  private  JsonGenerator gen;

  public AvroLogbackEncoder() throws IOException {
    writer = new ExtendedSpecificDatumWriter(LogRecord.class);
    bab = new ByteArrayBuilder(128);
    gen = createJsonGen(bab);
    encoder = new ExtendedJsonEncoder(LogRecord.getClassSchema(), gen);
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

    try {
      ByteArrayBuilder bb = new ByteArrayBuilder();
      OutputStreamWriter osw = new OutputStreamWriter(bb, StandardCharsets.UTF_8);
      JsonGenerator jgen = Json.FACTORY.createJsonGenerator(osw);
      LogRecord.getClassSchema().toJson(new AvroNamesRefResolver(SchemaResolvers.getDefault()), jgen);
      jgen.flush();
      osw.append('\n');
      osw.flush();
      return bb.toByteArray();
    } catch (IOException ex) {
      this.addError("Failed to write header for " + LogRecord.class, ex);
      return Arrays.EMPTY_BYTE_ARRAY;
    }
  }

  @Override
  public byte[] encode(final ILoggingEvent event) {
    LogRecord record;
    try {
       record = Converters.convert(event);
    } catch (RuntimeException ex){
      this.addError("Failed to convert " + event, ex);
      return Arrays.EMPTY_BYTE_ARRAY;
    }
    synchronized (bab) {
      try {
        return serializeAvro(record);
      } catch (IOException | RuntimeException ex) {
        this.addError("Failed to serialize " + record, ex);
        try {
          gen.flush();
        } catch (IOException ex1) {
         throw new UncheckedIOException(ex1);
        }
        this.addError("Failed at:" + new String(bab.toByteArray()) + '\n');
        gen = createJsonGen(bab);
        try {
          encoder = new ExtendedJsonEncoder(LogRecord.getClassSchema(), gen);
        } catch (IOException ex1) {
          this.addError("Cannot ", ex1);
        }
        return Arrays.EMPTY_BYTE_ARRAY;
      }
    }
  }

  public byte[] serializeAvro(final LogRecord record) throws IOException {
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
