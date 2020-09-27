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
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.spi.AppenderAttachable;
import ch.qos.logback.core.spi.AppenderAttachableImpl;
import ch.qos.logback.core.util.InterruptUtil;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.spf4j.perf.CloseableMeasurementRecorderSource;
import org.spf4j.perf.impl.RecorderFactory;

/**
 * A custom async implementation, to add statistics about logging. We will record metrics for queued logs and dropped
 * logs.
 * This implementation also eliminated the forever blocking implementation option which makes
 * no sense in any circumstance. (nothing/nobody waits forever)
 *
 * @author Zoltan Farkas
 */
public class Spf4jAsyncAppender extends UnsynchronizedAppenderBase<ILoggingEvent>
        implements AppenderAttachable<ILoggingEvent> {

  /**
   * The default maximum queue flush time allowed during appender stop. If the worker takes longer than this time it
   * will exit, discarding any remaining items in the queue
   */
  public static final int DEFAULT_MAX_FLUSH_TIME_MILLIS = 1000;
  /**
   * The default buffer size.
   */
  public static final int DEFAULT_QUEUE_SIZE = 256;
  public static final int UNDEFINED = -1;

  private final AppenderAttachableImpl<ILoggingEvent> aai = new AppenderAttachableImpl<>();
  private BlockingQueue<ILoggingEvent> blockingQueue;

  private int queueSize = DEFAULT_QUEUE_SIZE;

  private int appenderCount = 0;

  private int discardingThreshold = UNDEFINED;

  private Worker worker;

  private int maxFlushTimeMillis = DEFAULT_MAX_FLUSH_TIME_MILLIS;

  private boolean includeCallerData = false;

  private CloseableMeasurementRecorderSource droppedRec;

  private CloseableMeasurementRecorderSource queuedRec;

  private long maxLogEnqueueWaitMillis = 1;

  public final long getMaxLogEnqueueWaitMillis() {
    return maxLogEnqueueWaitMillis;
  }

  public final void setMaxLogEnqueueWaitMillis(final long maxLogEnqueueWaitMillis) {
    this.maxLogEnqueueWaitMillis = maxLogEnqueueWaitMillis;
  }

  /**
   * Events of level TRACE, DEBUG and INFO are deemed to be discardable.
   *
   * @param event
   * @return true if the event is of level TRACE, DEBUG or INFO false otherwise.
   */
  protected boolean isDiscardable(final ILoggingEvent event) {
    return event.getLevel().toInt() <= ch.qos.logback.classic.Level.INFO_INT;
  }

  /**
   * Extend this method for extra pre-processing.
   *
   * @param eventObject
   */
  protected void preprocess(final ILoggingEvent eventObject) {
    eventObject.prepareForDeferredProcessing();
    if (includeCallerData) {
      eventObject.getCallerData();
    }
  }

  public final boolean isIncludeCallerData() {
    return includeCallerData;
  }

  public final void setIncludeCallerData(final boolean includeCallerData) {
    this.includeCallerData = includeCallerData;
  }

  @Override
  public final void start() {
    if (isStarted()) {
      return;
    }
    if (appenderCount == 0) {
      addError("No attached appenders found.");
      return;
    }
    if (queueSize < 1) {
      addError("Invalid queue size [" + queueSize + "]");
      return;
    }
    blockingQueue = new ArrayBlockingQueue<>(queueSize);

    if (discardingThreshold == UNDEFINED) {
      discardingThreshold = queueSize / 5;
    }
    addInfo("Setting discardingThreshold to " + discardingThreshold);

    droppedRec = RecorderFactory.createScalableSimpleCountingRecorderSource(name + "_logs_dropped", "count", 60000);
    queuedRec = RecorderFactory.createScalableSimpleCountingRecorderSource(name + "_logs_queued", "count", 60000);
    worker = new Worker();
    worker.setDaemon(true);
    worker.setName("AsyncAppender-Worker-" + name);
    // make sure this instance is marked as "started" before staring the worker Thread
    super.start();
    worker.start();
  }

  private void queuePut(final ILoggingEvent eventObject) {
    Level level = eventObject.getLevel();
    try {
      if (blockingQueue.offer(eventObject, maxLogEnqueueWaitMillis, TimeUnit.MILLISECONDS)) {
        queuedRec.getRecorder(level).increment();
      } else {
        droppedRec.getRecorder(level).increment();
      }
    } catch (InterruptedException ex) {
      droppedRec.getRecorder(level).increment();
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public final void stop() {
    if (!isStarted()) {
      return;
    }

    // mark this appender as stopped so that Worker can also processPriorToRemoval if it is invoking
    // aii.appendLoopOnAppenders
    // and sub-appenders consume the interruption
    super.stop();

    // interrupt the worker thread so that it can terminate. Note that the interruption can be consumed
    // by sub-appenders
    worker.interrupt();

    InterruptUtil interruptUtil = new InterruptUtil(context);

    try {
      interruptUtil.maskInterruptFlag();

      worker.join(maxFlushTimeMillis);

      // check to see if the thread ended and if not add a warning message
      if (worker.isAlive()) {
        addWarn("Max queue flush timeout (" + maxFlushTimeMillis + " ms) exceeded. Approximately "
                + blockingQueue.size()
                + " queued events were possibly discarded.");
      } else {
        addInfo("Queue flush finished successfully within timeout.");
      }

    } catch (InterruptedException e) {
      int remaining = blockingQueue.size();
      addError("Failed to join worker thread. " + remaining + " queued events may be discarded.", e);
    } finally {
      interruptUtil.unmaskInterruptFlag();
      worker = null;
    }
    queuedRec.close();
    droppedRec.close();
  }

  /**
   * Implement/Overwrite to handle log events.
   *
   * @param eventObject
   */
  @Override
  protected void append(final ILoggingEvent eventObject) {
    if (isQueueBelowDiscardingThreshold() && isDiscardable(eventObject)) {
      droppedRec.getRecorder(eventObject.getLevel()).increment();
      return;
    }
    preprocess(eventObject);
    queuePut(eventObject);
  }

  private boolean isQueueBelowDiscardingThreshold() {
    return (blockingQueue.remainingCapacity() < discardingThreshold);
  }

  public final int getQueueSize() {
    return queueSize;
  }

  public final void setQueueSize(final int queueSize) {
    this.queueSize = queueSize;
  }

  public final int getDiscardingThreshold() {
    return discardingThreshold;
  }

  public final void setDiscardingThreshold(final int discardingThreshold) {
    this.discardingThreshold = discardingThreshold;
  }

  public final int getMaxFlushTimeMillis() {
    return maxFlushTimeMillis;
  }

  public final void setMaxFlushTimeMillis(final int maxFlushTimeMillis) {
    this.maxFlushTimeMillis = maxFlushTimeMillis;
  }

  /**
   * Returns the number of elements currently in the blocking queue.
   *
   * @return number of elements currently in the queue.
   */
  public final int getNumberOfElementsInQueue() {
    return blockingQueue.size();
  }

  /**
   * The remaining capacity available in the blocking queue.
   *
   * @return the remaining capacity
   * @see {@link java.util.concurrent.BlockingQueue#remainingCapacity()}
   */
  public final int getRemainingCapacity() {
    return blockingQueue.remainingCapacity();
  }

  public final void addAppender(final Appender<ILoggingEvent> newAppender) {
    if (appenderCount == 0) {
      appenderCount++;
      addInfo("Attaching appender named [" + newAppender.getName() + "] to AsyncAppender.");
      aai.addAppender(newAppender);
    } else {
      addWarn("One and only one appender may be attached to AsyncAppender.");
      addWarn("Ignoring additional appender named [" + newAppender.getName() + "]");
    }
  }

  public final Iterator<Appender<ILoggingEvent>> iteratorForAppenders() {
    return aai.iteratorForAppenders();
  }

  public final Appender<ILoggingEvent> getAppender(final String name) {
    return aai.getAppender(name);
  }

  public final boolean isAttached(final Appender<ILoggingEvent> eAppender) {
    return aai.isAttached(eAppender);
  }

  public final void detachAndStopAllAppenders() {
    aai.detachAndStopAllAppenders();
  }

  public final boolean detachAppender(final Appender<ILoggingEvent> eAppender) {
    return aai.detachAppender(eAppender);
  }

  public final boolean detachAppender(final String name) {
    return aai.detachAppender(name);
  }

  private class Worker extends Thread {

    public void run() {
      Spf4jAsyncAppender parent = Spf4jAsyncAppender.this;
      AppenderAttachableImpl<ILoggingEvent> laai = parent.aai;
      int wBuffSize = Spf4jAsyncAppender.this.queueSize / 4;
      List<ILoggingEvent> writeBuffer = new ArrayList<>(wBuffSize);
      BlockingQueue<ILoggingEvent> queue = parent.blockingQueue;
      // loop while the parent is started
      while (parent.isStarted()) {
        try {
          ILoggingEvent e = queue.poll(100, TimeUnit.MILLISECONDS);
          if (e != null) {
            laai.appendLoopOnAppenders(e);
            int drained = queue.drainTo(writeBuffer, wBuffSize);
            if (drained > 0) {
              for (ILoggingEvent d : writeBuffer) {
                laai.appendLoopOnAppenders(d);
              }
              writeBuffer.clear();
            }
          }
        } catch (InterruptedException ie) {
          break;
        }
      }
      addInfo("Worker thread will flush remaining events before exiting. ");
      for (ILoggingEvent e : queue) {
        laai.appendLoopOnAppenders(e);
        queue.remove(e);
      }
      laai.detachAndStopAllAppenders();
    }
  }
}
