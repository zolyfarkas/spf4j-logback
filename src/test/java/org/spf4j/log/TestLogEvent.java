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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.slf4j.Marker;
import org.slf4j.event.KeyValuePair;
import org.spf4j.base.Arrays;

final class TestLogEvent implements ILoggingEvent {

  private static final AtomicLong CNT = new AtomicLong();

  private final Instant instant;

  private final String message;

  private final Object[] args;

  private final IThrowableProxy throwable;
  
  private final long sequence;

  public static void resetCounter() {
    CNT.set(0);
  }


  TestLogEvent() {
    this(Instant.now());
  }

  TestLogEvent(final String msg) {
    this(Instant.now(), msg);
  }

  TestLogEvent(final Instant instant) {
    this(instant, "message", Arrays.EMPTY_OBJ_ARRAY);
  }

  TestLogEvent(final Instant instant, final String message,  final Object... args) {
    this(instant, message, null, args);
  }

  TestLogEvent(final Instant instant, final String message, final IThrowableProxy throwable, final Object... args) {
    this.instant = instant;
    this.args = args;
    this.throwable = throwable;
    this.sequence = CNT.getAndIncrement();
    this.message = message;
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
    return message;
  }

  @Override
  public Object[] getArgumentArray() {
    return args;
  }

  @Override
  public String getFormattedMessage() {
    return message + ' ' + sequence;
  }

  @Override
  public String getLoggerName() {
    return "test.logger";
  }

  @Override
  public LoggerContextVO getLoggerContextVO() {
    throw new UnsupportedOperationException();
  }

  @Override
  @Nullable
  public IThrowableProxy getThrowableProxy() {
    return throwable;
  }

  @Override
  public StackTraceElement[] getCallerData() {
    throw new UnsupportedOperationException();
   }

  @Override
  public boolean hasCallerData() {
    return false;
  }

  @Override
  @Nullable
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

  @Override
  public String toString() {
    return "TestLogEvent{" + "instant=" + instant + ", message=" + message
            + ", args=" + java.util.Arrays.toString(args) + '}';
  }

  @Override
  public List<Marker> getMarkerList() {
    return Collections.emptyList();
  }

  @Override
  public int getNanoseconds() {
    return instant.getNano();
  }

  @Override
  public long getSequenceNumber() {
    return sequence;
  }

  @Override
  public List<KeyValuePair> getKeyValuePairs() {
    return Collections.emptyList();
  }

}
