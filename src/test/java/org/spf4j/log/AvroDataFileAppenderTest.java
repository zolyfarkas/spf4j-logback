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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.Future;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spf4j.base.avro.LogRecord;
import org.spf4j.concurrent.DefaultExecutor;

/**
 * @author Zoltan Farkas
 */
public class AvroDataFileAppenderTest {

  private static final Logger LOG = LoggerFactory.getLogger(AvroDataFileAppenderTest.class);

  @Test
  public void testAvroDataFileAppender() throws IOException {
    deleteTestFiles();
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


 @Test
  public void testAvroDataFileAppender2() throws IOException {
    deleteTestFiles();
    AvroDataFileAppender appender = new AvroDataFileAppender();
    appender.setDestinationPath(org.spf4j.base.Runtime.TMP_FOLDER);
    appender.setFileNameBase("testAvroLog");
    appender.setPartitionZoneID(ZoneId.systemDefault().getId());
    appender.start();
    appender.append(new TestLogEvent());
    appender.append(new TestLogEvent(Instant.now().minus(1, ChronoUnit.DAYS)));
    appender.append(new TestLogEvent(Instant.now().minus(2, ChronoUnit.DAYS)));
    appender.append(new TestLogEvent(Instant.now().minus(3, ChronoUnit.DAYS)));
    appender.append(new TestLogEvent());
    appender.stop();
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


  }

  public void deleteTestFiles() throws IOException {
    Files.walk(Paths.get(org.spf4j.base.Runtime.TMP_FOLDER))
            .filter((p) ->
                    p.getFileName().toString().startsWith("testAvroLog")
            )
            .forEach((p) -> {
              try {
                Files.delete(p);
              } catch (IOException ex) {
                throw new UncheckedIOException(ex);
              }
            });
  }


 @Test
  public void testAvroDataFileAppenderAsync() throws IOException, InterruptedException {
    deleteTestFiles();
    final AvroDataFileAppender appender = new AvroDataFileAppender();
    appender.setDestinationPath(org.spf4j.base.Runtime.TMP_FOLDER);
    appender.setFileNameBase("testAvroLog");
    appender.setPartitionZoneID(ZoneId.systemDefault().getId());
    appender.start();
    Future<Object> submit = DefaultExecutor.instance().submit(() -> {
      while (true)  {
        appender.append(new TestLogEvent(Instant.now(), "", "a", 3, 4, LogAttribute.traceId("cucu")));
        Thread.sleep(1);
      }
    });
    for (int i = 1; i < 100; i++) {
      List<LogRecord> logs = appender.getLogs("local", 0, 100);
      Thread.sleep(1);
      LOG.debug("read {} logs", logs.size());
    }
    submit.cancel(true);
    appender.stop();
  }

 

}
