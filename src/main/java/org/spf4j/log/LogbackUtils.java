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

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.spi.AppenderAttachable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

/**
 * a set of logback utilities to query for the configured file appenders/etc
 * @author Zoltan Farkas
 */
public final class LogbackUtils {

  private LogbackUtils() { }

  public static Map<String, AvroDataFileAppender> getConfiguredFileAppenders() {
    Map<String, AvroDataFileAppender> result = new HashMap<>(4);
    configuredFileAppenders((a) -> result.put(a.getName(), a));
    return result;
  }

  public static void configuredFileAppenders(final Consumer<AvroDataFileAppender> consumer) {
    ILoggerFactory iLoggerFactory = LoggerFactory.getILoggerFactory();
    if (!(iLoggerFactory instanceof LoggerContext)) {
      return;
    }
    LoggerContext context = (LoggerContext) iLoggerFactory;
    for (Logger logger : context.getLoggerList()) {
      configuredFileAppenders(logger, consumer);
    }
  }

  public static void configuredFileAppenders(final AppenderAttachable aa, final Consumer<AvroDataFileAppender> consumer) {
    for (Iterator<Appender<ILoggingEvent>> it = aa.iteratorForAppenders(); it.hasNext();) {
      Appender<ILoggingEvent> appender = it.next();
      if (appender instanceof AvroDataFileAppender) {
        consumer.accept((AvroDataFileAppender) appender);
      } else if (appender instanceof AppenderAttachable) {
        configuredFileAppenders((AppenderAttachable) appender, consumer);
      }
    }
  }

}
