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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spf4j.base.avro.LogLevel;
import org.spf4j.base.avro.LogRecord;

/**
 *
 * @author Zoltan Farkas
 */
@SuppressFBWarnings("PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS")
public class ConvertersTest {

  private static final Logger LOG = LoggerFactory.getLogger(ConvertersTest.class);

  @Test
  public void testSer() throws IOException {
    StringBuilder msgBuilder = new StringBuilder("someMessage");
    for (int i = 0; i < 20; i++) {
      msgBuilder.append('a');
    }
    LogRecord rec = new LogRecord("", "bla", LogLevel.WARN, Instant.now(), "test", "text", msgBuilder.toString(),
            Collections.EMPTY_LIST,
            ImmutableMap.of("a", "b", "c", "d"), null, Collections.EMPTY_LIST);
    AvroLogbackEncoder avroLogbackEncoder = new AvroLogbackEncoder();
    LOG.debug(new String(avroLogbackEncoder.serializeAvro(rec), avroLogbackEncoder.getCharset()));
    LogRecord rec2 = new LogRecord("", "", LogLevel.DEBUG, Instant.now(), "test", "text", "someMessage",
            Collections.EMPTY_LIST,
            ImmutableMap.of("a", "b", "c", ""),
            org.spf4j.base.avro.Converters.convert(new RuntimeException(new IOException("bla"))),
            Collections.EMPTY_LIST);
    String sr2 = new String(avroLogbackEncoder.serializeAvro(rec2), avroLogbackEncoder.getCharset());
    LOG.debug(sr2);
    Assert.assertThat(sr2, Matchers.containsString("\"throwable\""));
    LOG.debug(new String(avroLogbackEncoder.serializeAvro(rec), avroLogbackEncoder.getCharset()));
  }

  @Test
  @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION") // on purpose
  public void testSerErrorr() throws IOException {
    LogRecord rec = new LogRecord("", "bla", null, Instant.now(), "test", "text", "abc",
            Collections.EMPTY_LIST,
            ImmutableMap.of("a", "b", "c", "d"), null, Collections.EMPTY_LIST);
    AvroLogbackEncoder avroLogbackEncoder = new AvroLogbackEncoder();
    try {
      LOG.debug(new String(avroLogbackEncoder.serializeAvro(rec), avroLogbackEncoder.getCharset()));
      Assert.fail();
    } catch (RuntimeException ex) {
      // expected
    }
    LogRecord rec2 = new LogRecord("", "bla", LogLevel.WARN, Instant.now(), "test", "text", "abc",
            Collections.EMPTY_LIST,
            ImmutableMap.of("a", "b", "c", "d"), null, Collections.EMPTY_LIST);
    avroLogbackEncoder.initEncoder();
    LOG.debug(new String(avroLogbackEncoder.serializeAvro(rec2), avroLogbackEncoder.getCharset()));
  }

  @Test
  public void testSerException() {
    LogRecord rec =
            Converters.convert(new TestLogEvent(Instant.now(), "Bla {} ",  "aaa", new RuntimeException(), "boo"));
    Assert.assertNotNull(rec.getThrowable());
  }



  @Test
  public void testConverter() {
    RuntimeException ex = new RuntimeException("test");
    RuntimeException ex2 = new RuntimeException("test2", ex);
    ex.addSuppressed(ex2);
    LogRecord lr = Converters.convert(new TestLogEvent(Instant.now(), "Test {}", ThrowableProxy.create(ex), "arg",
                            ThrowableProxy.create(ex2)));
    Assert.assertEquals("Test arg", lr.getMsg());
    LOG.debug("log message", lr);
  }


  /** Logback will throw java.lang.StackOverflowError for this */
  @Test
  public void testLogEvent() {
    RuntimeException ex = new RuntimeException("test");
    RuntimeException ex2 = new RuntimeException("test2", ex);
    ex.addSuppressed(ex2);
    LoggingEvent ev = new LoggingEvent("test",  new LoggerContext().getLogger("test"),
            ch.qos.logback.classic.Level.DEBUG, "message {}",
            ex2, new Object[] {"arg"});
    Assert.assertEquals("message arg", ev.getFormattedMessage());
  }


}
