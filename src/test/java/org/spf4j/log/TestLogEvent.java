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
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Marker;
import org.spf4j.base.Arrays;

class TestLogEvent implements ILoggingEvent {

    private static final AtomicLong CNT = new AtomicLong();

    private final Instant instant;

    private final long id;

    public TestLogEvent() {
      this(Instant.now());
    }

    public TestLogEvent(final Instant instant) {
      this.instant = instant;
      this.id = CNT.getAndIncrement();
    }

    @Override
    public String getThreadName() {
      return "test";
    }

    @Override
    public ch.qos.logback.classic.Level getLevel() {
      return ch.qos.logback.classic.Level.INFO;
    }

    @Override
    public String getMessage() {
      return "message " + id;
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
      return instant.toEpochMilli();
    }

    @Override
    public void prepareForDeferredProcessing() {
      // do notyhing;
    }
  }
