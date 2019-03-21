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
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.avro.AvroNamesRefResolver;
import org.apache.avro.Schema;
import org.apache.avro.SchemaResolvers;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.ExtendedJsonEncoder;
import org.apache.avro.specific.ExtendedSpecificDatumWriter;
import org.apache.avro.util.Arrays;
import org.spf4j.base.Json;
import org.spf4j.base.Strings;
import org.spf4j.base.avro.LogRecord;
import org.spf4j.io.ByteArrayBuilder;

/**
 * An encoder that will encode a log event as json.
 * @author Zoltan Farkas
 */
public final class AvroLogbackEncoder extends EncoderBase<ILoggingEvent> implements LifeCycle {

    private static final MinimalPrettyPrinter JSON_FMT = new MinimalPrettyPrinter(Strings.EOL) {
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
    };

  private final ByteArrayBuilder bab;

  private final DatumWriter writer;

  private Encoder encoder;

  private Charset charset;

  public AvroLogbackEncoder() {
    charset = Charset.defaultCharset();
    bab = new ByteArrayBuilder(128);
    writer = new ExtendedSpecificDatumWriter(LogRecord.class);
    initEncoder();
  }

  public void setCharset(final String charsetName) {
    this.charset = Charset.forName(charsetName);
  }

  public void initEncoder() {
      try {
        encoder = new ExtendedJsonEncoder(LogRecord.getClassSchema(), createJsonGen(bab));
      } catch (IOException ex) {
        this.addError("Cannot ", ex);
      }
  }

  private  JsonGenerator createJsonGen(final ByteArrayBuilder bab) {
    JsonGenerator agen;
    try {
      agen = Schema.FACTORY.createGenerator(new OutputStreamWriter(bab, charset));
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
    agen.setPrettyPrinter(JSON_FMT);
    return agen;
  }

  @Override
  public byte[] headerBytes() {
    try {
      ByteArrayBuilder bb = new ByteArrayBuilder();
      OutputStreamWriter osw = new OutputStreamWriter(bb, charset);
      JsonGenerator jgen = Json.FACTORY.createGenerator(osw);
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
      org.spf4j.base.Runtime.error("Failed to convert " + event, ex);
      this.addError("Failed to convert " + event, ex);
      return Arrays.EMPTY_BYTE_ARRAY;
    }
    synchronized (bab) {
      try {
        return serializeAvro(record);
      } catch (IOException | RuntimeException ex) {
        org.spf4j.base.Runtime.error("Failed to serialize " + record, ex);
        this.addError("Failed to serialize " + record, ex);
        try {
          encoder.flush();
        } catch (IOException ex1) {
         throw new UncheckedIOException(ex1);
        } finally {
          initEncoder();
        }
        this.addError("Failed at:" + new String(bab.toByteArray()) + '\n');
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
