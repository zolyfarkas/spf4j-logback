/*
 * Copyright (c) 2001-2017, Zoltan Farkas All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 *
 * Additionally licensed with:
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
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import ch.qos.logback.classic.spi.ThrowableProxy;
import com.google.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.ParametersAreNonnullByDefault;
import org.slf4j.Marker;
import org.spf4j.base.Arrays;
import org.spf4j.base.Slf4jMessageFormatter;
import org.spf4j.base.avro.FileLocation;
import org.spf4j.base.avro.LogLevel;
import org.spf4j.base.avro.LogRecord;
import org.spf4j.base.avro.Method;
import org.spf4j.base.avro.RemoteException;
import org.spf4j.base.avro.StackTraceElement;
import org.spf4j.base.avro.Throwable;

/**
 * @author Zoltan Farkas
 */
@ParametersAreNonnullByDefault
public final class Converters {

  private Converters() {
  }

  public static StackTraceElement convert(final StackTraceElementProxy stackTrace) {
    java.lang.StackTraceElement stackTraceElement = stackTrace.getStackTraceElement();
    String className = stackTraceElement.getClassName();
    String fileName = stackTraceElement.getFileName();
    return new StackTraceElement(new Method(className, stackTraceElement.getMethodName()),
            fileName == null ? null : new FileLocation(fileName, stackTraceElement.getLineNumber(), -1),
            org.spf4j.base.PackageInfo.getPackageInfo(className));
  }

  public static List<StackTraceElement> convert(final StackTraceElementProxy[] stackTraces) {
    int l = stackTraces.length;
    if (l == 0) {
      return Collections.EMPTY_LIST;
    }
    List<StackTraceElement> result = new ArrayList<>(l);
    for (StackTraceElementProxy st : stackTraces) {
      result.add(convert(st));
    }
    return result;
  }

  public static List<Throwable> convert(final IThrowableProxy[] throwables) {
    int l = throwables.length;
    if (l == 0) {
      return Collections.EMPTY_LIST;
    }
    List<Throwable> result = new ArrayList<>(l);
    for (IThrowableProxy t : throwables) {
      result.add(convert(t));
    }
    return result;
  }

  public static Throwable convert(final IThrowableProxy throwable) {
    String message = throwable.getMessage();
    if (throwable instanceof ThrowableProxy && throwable.getClassName().equals(RemoteException.class.getName())) {
        return new Throwable(throwable.getClassName(),
                message == null ? "" : message, convert(throwable.getStackTraceElementProxyArray()),
                ((RemoteException) ((ThrowableProxy) throwable).getThrowable()).getRemoteCause(),
                convert(throwable.getSuppressed()));
    }
    IThrowableProxy cause = throwable.getCause();
    return new Throwable(throwable.getClassName(),
            message == null ? "" : message,
            convert(throwable.getStackTraceElementProxyArray()),
            cause == null ? null : convert(cause),
            convert(throwable.getSuppressed()));
  }

  public static java.lang.Throwable convert2(final IThrowableProxy throwable) {
    if (throwable instanceof ThrowableProxy) {
      return ((ThrowableProxy) throwable).getThrowable();
    }
    throw new UnsupportedOperationException("Cannot convert " + throwable);
  }

  public static LogLevel convert(final ch.qos.logback.classic.Level level) {
    if (level.levelInt >= ch.qos.logback.classic.Level.ERROR_INT) {
      return LogLevel.ERROR;
    } else if (level.levelInt >= ch.qos.logback.classic.Level.WARN_INT) {
      return LogLevel.WARN;
    } else if (level.levelInt >= ch.qos.logback.classic.Level.INFO_INT) {
      return LogLevel.INFO;
    } else if (level.levelInt >= ch.qos.logback.classic.Level.DEBUG_INT) {
      return LogLevel.DEBUG;
    } else {
      return LogLevel.TRACE;
    }
  }


  public static Level convert2(final ch.qos.logback.classic.Level level) {
    if (level.levelInt >= ch.qos.logback.classic.Level.ERROR_INT) {
      return Level.ERROR;
    } else if (level.levelInt >= ch.qos.logback.classic.Level.WARN_INT) {
      return Level.WARN;
    } else if (level.levelInt >= ch.qos.logback.classic.Level.INFO_INT) {
      return Level.INFO;
    } else if (level.levelInt >= ch.qos.logback.classic.Level.DEBUG_INT) {
      return Level.DEBUG;
    } else {
      return Level.TRACE;
    }
  }



  @SuppressFBWarnings("WOC_WRITE_ONLY_COLLECTION_LOCAL")
  public static LogRecord convert(final ILoggingEvent event) {
    IThrowableProxy extraThrowable = event.getThrowableProxy();
    Marker marker = event.getMarker();
    Object[] arguments = event.getArgumentArray();
    if (arguments == null) {
      arguments = Arrays.EMPTY_OBJ_ARRAY;
    }
    String fmt = event.getMessage();
    StringBuilder msgBuilder = new StringBuilder(fmt.length() + 8);
    int index;
    try {
      index = Slf4jMessageFormatter.format(msgBuilder, fmt, arguments);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
    String traceId = "";
    Map<String, Object> attribs = null;
    List<Object> xArgs;
    if (index >= arguments.length) {
      xArgs = Collections.EMPTY_LIST;
    } else {
      int nrAttribs = 0;
      for (int i = index; i < arguments.length; i++) {
        Object obj = arguments[i];
        if (obj instanceof LogAttribute) {
          String attrName = ((LogAttribute) obj).getName();
          if ("trId".equals(attrName)) {
            traceId = ((LogAttribute) obj).getValue().toString();
          } else {
            nrAttribs++;
          }
        }
      }
      if (nrAttribs + index == arguments.length) {
        xArgs = Collections.EMPTY_LIST;
      } else {
        xArgs = new ArrayList<>(arguments.length - nrAttribs - index);
      }
      attribs = Maps.newHashMapWithExpectedSize(nrAttribs + (marker == null ? 0 : 1));
      for (int i = index; i < arguments.length; i++) {
        Object obj = arguments[i];
        if (obj instanceof LogAttribute) {
          String name = ((LogAttribute) obj).getName();
          if (!"trId".equals(name)) {
            attribs.put(name, ((LogAttribute) obj).getValue());
          }
        } else {
          xArgs.add(obj);
        }
      }
      if (marker != null) {
        attribs.put(marker.getName(), marker);
      }
    }
    return new LogRecord("", traceId, convert(event.getLevel()),
            Instant.ofEpochMilli(event.getTimeStamp()),
            event.getLoggerName(), event.getThreadName(), msgBuilder.toString(),
            extraThrowable == null ? null : convert(extraThrowable), xArgs,
            attribs == null ? Collections.EMPTY_MAP : attribs);
  }



  @SuppressFBWarnings("WOC_WRITE_ONLY_COLLECTION_LOCAL")
  public static Slf4jLogRecord convert2(final ILoggingEvent event) {
    IThrowableProxy extraThrowable = event.getThrowableProxy();
    Object [] arguments;
    if (extraThrowable == null) {
      arguments = event.getArgumentArray();
    } else {
      arguments = Arrays.append(event.getArgumentArray(), convert2(extraThrowable));
    }
    return new Slf4jLogRecordImpl(false, event.getLoggerName(), convert2(event.getLevel()),
            event.getMarker(), event.getTimeStamp(), event.getMessage(), arguments);
  }




}
