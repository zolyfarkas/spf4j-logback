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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import java.io.File;
import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.spf4j.base.Arrays;
import org.spf4j.base.avro.LogRecord;

/**
 * @author Zoltan Farkas
 */
public class AvroDataFileAppenderTest {

  private static final Logger LOG = LoggerFactory.getLogger(AvroDataFileAppenderTest.class);

  @Test
  public void testAvroDataFileAppender() throws IOException {
    AvroDataFileAppender appender = new AvroDataFileAppender();
    appender.setDestinationPath(new File(org.spf4j.base.Runtime.TMP_FOLDER));
    appender.setFileNameBase("testAvroLog");
    appender.setPartitionZoneID(ZoneId.systemDefault().getId());
    appender.start();
    appender.append(new TestLogEvent());
    appender.append(new TestLogEvent());
    appender.stop();
    int i = 0;
    for (LogRecord rec : appender.getLogs()) {
      LOG.debug("retrieved", rec);
      i++;
    }
    Assert.assertEquals(2, i);

  }

  private static class TestLogEvent implements ILoggingEvent {

    public TestLogEvent() {
    }

    @Override
    public String getThreadName() {
      return "test";
    }

    @Override
    public Level getLevel() {
      return Level.INFO;
    }

    @Override
    public String getMessage() {
      return "message";
    }

    @Override
    public Object[] getArgumentArray() {
      return Arrays.EMPTY_OBJ_ARRAY;
    }

    @Override
    public String getFormattedMessage() {
      return "message";
    }

    @Override
    public String getLoggerName() {
      return "logger";
    }

    @Override
    public LoggerContextVO getLoggerContextVO() {
      throw new UnsupportedOperationException();
    }

    @Override
    public IThrowableProxy getThrowableProxy() {
      return null;
    }

    @Override
    public StackTraceElement[] getCallerData() {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean hasCallerData() {
      return false;
    }

    @Override
    public Marker getMarker() {
      return null;
    }

    @Override
    public Map<String, String> getMDCPropertyMap() {
      return Collections.EMPTY_MAP;
    }

    @Override
    public Map<String, String> getMdc() {
      return Collections.EMPTY_MAP;
    }

    @Override
    public long getTimeStamp() {
      return System.currentTimeMillis();
    }

    @Override
    public void prepareForDeferredProcessing() {
      // do notyhing;
    }
  }

}
