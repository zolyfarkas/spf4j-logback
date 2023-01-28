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
import com.google.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.spf4j.ds.IdentityHashSet;
import org.spf4j.io.ObjectAppenderSupplier;

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

  public static List<Throwable> convert(final IThrowableProxy[] throwables, final Set<IThrowableProxy> seen) {
    int l = throwables.length;
    if (l == 0) {
      return Collections.EMPTY_LIST;
    }
    List<Throwable> result = new ArrayList<>(l);
    for (IThrowableProxy t : throwables) {
      result.add(convert(t, seen));
    }
    return result;
  }

  public static Throwable convert(final IThrowableProxy throwable) {
    return convert(throwable, new IdentityHashSet<>(8));
  }

  @SuppressFBWarnings("ITC_INHERITANCE_TYPE_CHECKING")
  public static Throwable convert(final IThrowableProxy throwable, final Set<IThrowableProxy> seen) {
    if (seen.contains(throwable)) {
      return new Throwable(throwable.getClassName(),
              "CIRCULAR REFERENCE: " + throwable.getMessage(), Collections.EMPTY_LIST, null, Collections.EMPTY_LIST);
    }
    seen.add(throwable);
    String message = throwable.getMessage();
    RemoteException rex = null;
    if (throwable instanceof ThrowableProxy) {
      java.lang.Throwable jThr = ((ThrowableProxy) throwable).getThrowable();
      if (jThr instanceof RemoteException) {
        rex = (RemoteException) jThr;
      }
    } else if (throwable instanceof ch.qos.logback.classic.spi.ThrowableProxy) {
      java.lang.Throwable jThr = ((ch.qos.logback.classic.spi.ThrowableProxy) throwable).getThrowable();
      if (jThr instanceof RemoteException) {
        rex = (RemoteException) jThr;
      }
    }
    if (rex != null) {
      return new Throwable(throwable.getClassName(),
              message == null ? "" : message, convert(throwable.getStackTraceElementProxyArray()),
              rex.getRemoteCause(),
              convert(throwable.getSuppressed(), seen));
    } else {
      IThrowableProxy cause = throwable.getCause();
      return new Throwable(throwable.getClassName(),
              message == null ? "" : message,
              convert(throwable.getStackTraceElementProxyArray()),
              cause == null ? null : convert(cause, seen),
              convert(throwable.getSuppressed(), seen));
    }
  }

  @SuppressFBWarnings("ITC_INHERITANCE_TYPE_CHECKING")
  public static java.lang.Throwable convert2(final IThrowableProxy throwable) {
    if (throwable instanceof ThrowableProxy) {
      return ((ThrowableProxy) throwable).getThrowable();
    } else if (throwable instanceof ch.qos.logback.classic.spi.ThrowableProxy) {
      return ((ch.qos.logback.classic.spi.ThrowableProxy) throwable).getThrowable();
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

  @SuppressFBWarnings({ "WOC_WRITE_ONLY_COLLECTION_LOCAL", "ITC_INHERITANCE_TYPE_CHECKING",
                      "CC_CYCLOMATIC_COMPLEXITY"})
  // WOC_WRITE_ONLY_COLLECTION_LOCAL a false positive.
  // ITC_INHERITANCE_TYPE_CHECKING not other good way that  I know of...
  // CC_CYCLOMATIC_COMPLEXITY is a walid concern, will need to revisit this.
  public static LogRecord convert(final ILoggingEvent event) {
    IThrowableProxy extraThrowable = event.getThrowableProxy();
    Marker marker = event.getMarker();
    Object[] arguments = event.getArgumentArray();
    if (arguments == null) {
      arguments = Arrays.EMPTY_OBJ_ARRAY;
    }
    String fmt = event.getMessage();
    int index = Slf4jMessageFormatter.getFormatParameterNumber(fmt);
    List<String> msgArgs;
    if (index == 0) {
      msgArgs = Collections.emptyList();
    } else {
      String[] ma = new String[index];
      for (int i = 0; i < index; i++) {
        Object arg = arguments[i];
        if (arg == null) {
          ma[i] = "null";
        } else {
          ma[i] = ObjectAppenderSupplier.getDefaultToStringAppenderSupplier().get(arg.getClass()).toString(arg);
        }
      }
      msgArgs = java.util.Arrays.asList(ma);
    }
    String traceId = "";
    Map<String, Object> attribs = null;
    List<Object> xArgs;
    if (index >= arguments.length) {
      xArgs = Collections.emptyList();
    } else {
      int nrXArgs = 0;
      int nrAttribs = 0;
      for (int i = index; i < arguments.length; i++) {
        Object obj = arguments[i];
        if (obj instanceof LogAttribute) {
          LogAttribute la = (LogAttribute) obj;
          String attrName = la.getName();
          switch (attrName) {
            case LogAttribute.ID_ATTR_NAME:
                traceId = la.getValue().toString();
              break;
            default:
              nrAttribs++;
          }
        } else if (obj instanceof java.lang.Throwable) {
          if (extraThrowable == null) {
            extraThrowable = ThrowableProxy.create((java.lang.Throwable) obj);
          } else {
            extraThrowable = ThrowableProxy.addSuppressed(extraThrowable,
                    ThrowableProxy.create((java.lang.Throwable) obj));
          }
        } else if (obj instanceof IThrowableProxy) {
           if (extraThrowable == null) {
            extraThrowable = (IThrowableProxy) obj;
          } else {
            extraThrowable = ThrowableProxy.addSuppressed(extraThrowable,
                    (ThrowableProxy) obj);
          }
        } else {
          nrXArgs++;
        }
      }
      if (nrXArgs == 0) {
        xArgs = Collections.EMPTY_LIST;
      } else {
        xArgs = new ArrayList<>(nrXArgs);
      }
      attribs = Maps.newHashMapWithExpectedSize(nrAttribs + (marker == null ? 0 : 1));
      for (int i = index; i < arguments.length; i++) {
        Object obj = arguments[i];
        if (obj instanceof LogAttribute) {
          String name = ((LogAttribute) obj).getName();
          if (!LogAttribute.ID_ATTR_NAME.equals(name)) {
            attribs.put(name, ((LogAttribute) obj).getValue());
          }
        } else if (!(obj instanceof java.lang.Throwable || obj instanceof IThrowableProxy)) {
          xArgs.add(obj);
        }
      }
      if (marker != null) {
        attribs.put(marker.getName(), marker);
      }
    }
    Map<String, String> mdc = event.getMDCPropertyMap();
    if (!mdc.isEmpty()) {
      if (attribs == null) {
        attribs = (Map) mdc;
      } else {
        attribs.putAll(mdc);
      }
    }
    return new LogRecord("", traceId, convert(event.getLevel()),
            Instant.ofEpochMilli(event.getTimeStamp()),
            event.getLoggerName(), event.getThreadName(), fmt, msgArgs, xArgs,
            attribs == null ? Collections.emptyMap() : attribs,
            extraThrowable == null ? null : convert(extraThrowable));
  }

  @SuppressFBWarnings("WOC_WRITE_ONLY_COLLECTION_LOCAL")
  public static Slf4jLogRecord convert2(final ILoggingEvent event) {
    IThrowableProxy extraThrowable = event.getThrowableProxy();
    Object[] arguments;
    Object[] argumentArray = event.getArgumentArray();
    if (extraThrowable == null) {
      arguments = argumentArray == null ? Arrays.EMPTY_OBJ_ARRAY : argumentArray;
    } else {
      java.lang.Throwable convertedEx = convert2(extraThrowable);
      arguments = argumentArray == null
              ? new Object[] {convertedEx}
              : Arrays.append(argumentArray, convertedEx);
    }
    return new Slf4jLogRecordImpl(false, event.getLoggerName(), convert2(event.getLevel()),
            event.getMarker(), event.getTimeStamp(), event.getMessage(), arguments);
  }

}
