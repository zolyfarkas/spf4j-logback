/*
 * Copyright 2020 SPF4J.
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
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

/**
 * Filters events above the threshold level.
 *
 * Events with a level above the specified level will be denied, while events with a level equal or above the specified
 * level will trigger a FilterReply.NEUTRAL result, to allow the rest of the filter chain process the event.
 *
 * For more information about filters, please refer to the online manual at
 * http://logback.qos.ch/manual/filters.html#thresholdFilter
 */


public final class UpToThresholdFilter extends Filter<ILoggingEvent> {

  private Level level;

  public UpToThresholdFilter() {
    level = Level.WARN;
  }

  @Override
  public FilterReply decide(final ILoggingEvent event) {
    if (!isStarted()) {
      return FilterReply.NEUTRAL;
    }

    if (level.isGreaterOrEqual(event.getLevel())) {
      return FilterReply.NEUTRAL;
    } else {
      return FilterReply.DENY;
    }
  }

  public void setLevel(final String level) {
    this.level = Level.toLevel(level);
  }

  public void start() {
    if (this.level != null) {
      super.start();
    }
  }

  @Override
  public String toString() {
    return "UpToThresholdFilter{" + "level=" + level + '}';
  }

}
