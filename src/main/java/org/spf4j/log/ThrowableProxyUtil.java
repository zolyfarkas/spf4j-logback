/**
 * Logback: the reliable, generic, fast and flexible logging framework.
 * Copyright (C) 1999-2015, QOS.ch. All rights reserved.
 *
 * This program and the accompanying materials are dual-licensed under
 * either the terms of the Eclipse Public License v1.0 as published by
 * the Eclipse Foundation
 *
 *   or (per the licensee's choosing)
 *
 * under the terms of the GNU Lesser General Public License version 2.1
 * as published by the Free Software Foundation.
 */
package org.spf4j.log;

import ch.qos.logback.classic.spi.StackTraceElementProxy;

/**
 * Convert a Throwable into an array of ThrowableDataPoint objects.
 *
 *
 * @author Ceki G&uuml;lc&uuml;
 */
public final class ThrowableProxyUtil {

  public static final int REGULAR_EXCEPTION_INDENT = 1;
  public static final int SUPPRESSED_EXCEPTION_INDENT = 1;

  private ThrowableProxyUtil() { }

  static StackTraceElementProxy[] steArrayToStepArray(final StackTraceElement[] stea) {
    if (stea == null) {
      return new StackTraceElementProxy[0];
    }
    StackTraceElementProxy[] stepa = new StackTraceElementProxy[stea.length];
    for (int i = 0; i < stepa.length; i++) {
      stepa[i] = new StackTraceElementProxy(stea[i]);
    }
    return stepa;
  }

  static int findNumberOfCommonFrames(final StackTraceElement[] steArray,
  final StackTraceElementProxy[] parentSTEPArray) {
    if (parentSTEPArray == null || steArray == null) {
      return 0;
    }

    int steIndex = steArray.length - 1;
    int parentIndex = parentSTEPArray.length - 1;
    int count = 0;
    while (steIndex >= 0 && parentIndex >= 0) {
      StackTraceElement ste = steArray[steIndex];
      StackTraceElement otherSte = parentSTEPArray[parentIndex].getStackTraceElement();
      if (ste.equals(otherSte)) {
        count++;
      } else {
        break;
      }
      steIndex--;
      parentIndex--;
    }
    return count;
  }


}
