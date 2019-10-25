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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spf4j.base.avro.LogRecord;
import org.spf4j.concurrent.DefaultExecutor;
import org.spf4j.test.log.LogAssert;
import org.spf4j.test.log.TestLoggers;
import org.spf4j.test.matchers.LogMatchers;
import org.spf4j.zel.vm.CompileException;
import org.spf4j.zel.vm.Program;

/**
 * @author Zoltan Farkas
 */
@SuppressFBWarnings({ "MDM_THREAD_YIELD", "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE" })
public class AvroDataFileAppenderTest {

  private static final Logger LOG = LoggerFactory.getLogger(AvroDataFileAppenderTest.class);

  @Test
  public void testAvroDataFileAppender() throws IOException {
    deleteTestFiles("testAvroLog");
    AvroDataFileAppender appender = new AvroDataFileAppender();
    appender.setDestinationPath(org.spf4j.base.Runtime.TMP_FOLDER);
    appender.setFileNameBase("testAvroLog");
    appender.setPartitionZoneID(ZoneId.systemDefault().getId());
    appender.start();
    appender.append(new TestLogEvent());
    appender.append(new TestLogEvent());
    appender.stop();
    int i = 0;
    for (LogRecord rec : appender.getCurrentLogs()) {
      LOG.debug("retrieved", rec);
      i++;
    }
    Assert.assertEquals(2, i);

  }

  private static class BrokenBean {

    public String getCrap() {
      throw new RuntimeException("yes! " + this);
    }
  }

  @Test
  public void testAvroDataFileAppender3() throws IOException {
    deleteTestFiles("testAvroLog");
    AvroDataFileAppender appender = new AvroDataFileAppender();
    appender.setDestinationPath(org.spf4j.base.Runtime.TMP_FOLDER);
    appender.setFileNameBase("testAvroLog");
    appender.setPartitionZoneID(ZoneId.systemDefault().getId());
    appender.start();
    appender.append(new TestLogEvent(Instant.now(), "", new BrokenBean()));
    appender.append(new TestLogEvent());
    appender.append(new TestLogEvent());
    appender.stop();
    int i = 0;
    for (LogRecord rec : appender.getCurrentLogs()) {
      LOG.debug("retrieved", rec);
      i++;
    }
    Assert.assertEquals(2, i);

  }

  @Test
  public void testAvroDataFileAppender2()
          throws IOException, CompileException, ExecutionException, InterruptedException {
    TestLogEvent.resetCounter();
    deleteTestFiles("testAvroLog");
    AvroDataFileAppender appender = new AvroDataFileAppender();
    appender.setDestinationPath(org.spf4j.base.Runtime.TMP_FOLDER);
    appender.setFileNameBase("testAvroLog");
    appender.setPartitionZoneID(ZoneId.systemDefault().getId());
    LOG.debug("Existing Files {}", appender.getLogFiles());
    appender.start();
    appender.append(new TestLogEvent());
    Instant now = Instant.now();
    appender.append(new TestLogEvent(now.minus(1, ChronoUnit.DAYS)));
    appender.append(new TestLogEvent(now.minus(2, ChronoUnit.DAYS)));
    appender.append(new TestLogEvent(now.minus(3, ChronoUnit.DAYS)));
    appender.append(new TestLogEvent());
    appender.stop();
    LOG.debug("All Log files: {}", appender.getLogFiles());
    int i = 0;
    Iterable<LogRecord> logs = appender.getLogs("local", 0, 100);
    for (LogRecord rec : logs) {
      LOG.debug("retrieved1", rec);
      i++;
    }
    Assert.assertEquals(5, i);
    i = 0;
    logs = appender.getLogs("local", 2, 100);
    for (LogRecord rec : logs) {
      LOG.debug("retrieved2", rec);
      i++;
    }
    Assert.assertEquals(3, i);
    i = 0;
    logs = appender.getLogs("local", 3, 100);
    for (LogRecord rec : logs) {
      LOG.debug("retrieved3", rec);
      i++;
    }
    Assert.assertEquals(2, i);

    List<LogRecord> filteredLogs = appender.getFilteredLogs("test", 0, 10,
            Program.compilePredicate("log.msg == 'message 4'", "log"));
    LOG.debug("filtered logs", filteredLogs);
    Assert.assertEquals(1, filteredLogs.size());
    Assert.assertTrue(filteredLogs.get(0).getOrigin().endsWith("1"));
  }

  @Test
  public void testAvroDataFileAppenderCleanup()
          throws IOException, CompileException, ExecutionException, InterruptedException {
    deleteTestFiles("testAvroLog");
    AvroDataFileAppender appender = new AvroDataFileAppender();
    appender.setDestinationPath(org.spf4j.base.Runtime.TMP_FOLDER);
    appender.setFileNameBase("testAvroLog");
    appender.setPartitionZoneID(ZoneId.systemDefault().getId());
    appender.setMaxNrFiles(2);
    appender.setMaxLogsBytes(102400);
    appender.start();
    Instant now = Instant.now();
    LogAssert expect = TestLoggers.sys().expect(AvroDataFileAppender.class.getName(), Level.INFO, 2,
            LogMatchers.hasMessageWithPattern("Deleting \\./target/testAvroLog.*"));
    appender.append(new TestLogEvent(now.minus(1, ChronoUnit.DAYS)));
    appender.append(new TestLogEvent(now.minus(2, ChronoUnit.DAYS)));
    appender.append(new TestLogEvent(now.minus(3, ChronoUnit.DAYS)));
    appender.append(new TestLogEvent());
    appender.stop();
    LOG.debug("Appender stopped");
    List<Path> logFiles = appender.getLogFiles();
    LOG.debug("All Log files: {}", logFiles);
    expect.assertObservation();
  }



  private void deleteTestFiles(final String fileNameBase) throws IOException {
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(org.spf4j.base.Runtime.TMP_FOLDER), (p)
            -> p.getFileName().toString().startsWith(fileNameBase))) {
      stream.forEach((p) -> {
              try {
                Files.delete(p);
              } catch (IOException ex) {
                throw new UncheckedIOException(ex);
              }
            });
    }
  }

  @Test
  @Ignore
  public void testLoadLogFile() throws IOException, InterruptedException {
    final AvroDataFileAppender appender = new AvroDataFileAppender();
    appender.setDestinationPath(new File("./src/test/resources").getCanonicalPath());
    appender.setFileNameBase("jaxrs-spf4j-demo");
    appender.setPartitionZoneID(ZoneId.systemDefault().getId());
    List<LogRecord> logs = appender.getLogs("test", 0, 10);
    LogPrinter printer = new LogPrinter(StandardCharsets.UTF_8);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    for (LogRecord record : logs) {
      printer.print(record, bos);
    }
    LOG.debug("written {} bytes", bos.size());
//    LOG.debug("logs", logs.get(0));
//    LOG.debug("logs", logs.toArray());
    Assert.assertEquals(10, logs.size());
  }

  @Test
  public void testAvroDataFileAppenderAsync() throws IOException, InterruptedException {
    deleteTestFiles("testAvroLog");
    final AvroDataFileAppender appender = new AvroDataFileAppender();
    appender.setDestinationPath(org.spf4j.base.Runtime.TMP_FOLDER);
    appender.setFileNameBase("testAvroLog");
    appender.setPartitionZoneID(ZoneId.systemDefault().getId());
    appender.start();
    Future<Object> submit = DefaultExecutor.instance().submit(() -> {
      while (true) {
        appender.append(new TestLogEvent(Instant.now(), "", "a", 3, 4, LogAttribute.traceId("cucu")));
        Thread.sleep(1);
      }
    });
    for (int i = 1; i < 100; i++) {
      List<LogRecord> logs = appender.getLogs("local", 0, 100);
      LOG.debug("read {} logs", logs.size());
      Assert.assertTrue(logs.size() <= 100);
      Thread.sleep(1);
    }
    submit.cancel(true);
    appender.stop();
  }

}
