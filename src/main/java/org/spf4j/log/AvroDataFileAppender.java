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
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.spf4j.base.avro.LogRecord;
import org.spf4j.concurrent.FileBasedLock;
import org.spf4j.jmx.JmxExport;
import org.spf4j.jmx.Registry;

/**
 * an Appender that will log into binary avro data files.
 * Date will be partition by date
 *
 * @author Zoltan Farkas
 */
public final class AvroDataFileAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {

  private static final ZoneId ZULU = ZoneId.of("Z");

  private String fileNameBase;

  private DataFileWriter<LogRecord> writer;

  private LocalDate fileDate;

  private File currentFile;

  private FileBasedLock currentFileLock;

  private File destinationPath;

  private ZoneId zoneId;

  private CodecFactory codecFact;

  public AvroDataFileAppender() {
    zoneId = ZULU;
    fileNameBase = ManagementFactory.getRuntimeMXBean().getName();
    setName("avroLogAppender");

    try {
      Class.forName("org.xerial.snappy.Snappy");
      codecFact = CodecFactory.snappyCodec();
    } catch (ClassNotFoundException ex) {
     codecFact = null;
    }
  }

  public void setFileNameBase(final String fileNameBase) {
    this.fileNameBase = fileNameBase;
  }

  public void setDestinationPath(final File destinationPath) {
    this.destinationPath = destinationPath;
  }

  public void setPartitionZoneID(final String zoneId) {
    this.zoneId = ZoneId.of(zoneId);
  }

  @JmxExport
  public File getCurrentFile() {
    return currentFile;
  }

  @JmxExport
  public void flush() throws IOException {
    writer.flush();
  }

  public Iterable<LogRecord> getLogs() throws IOException {
    return DataFileReader.openReader(currentFile, new SpecificDatumReader<>(LogRecord.class));
  }

  @Override
  public void stop() {
    try {
      writer.close();
      super.stop();
    } catch (IOException | RuntimeException ex) {
      addError("Unable to close writer " + writer, ex);
    }
    Registry.unregister("avro.log.appender", this.getName());
  }

  @Override
  public void start() {
    Instant now = Instant.now();
    try {
      ensurePartition(now);
      super.start();
    } catch (IOException | InterruptedException | RuntimeException ex) {
       addError("Unable to ensure file partition " + now, ex);
    }
    Registry.export("avro.log.appender", this.getName(), this);
  }

  private void ensurePartition(final Instant instant) throws IOException, InterruptedException {
    ZonedDateTime zdt = instant.atZone(zoneId);
    LocalDate target = zdt.toLocalDate();
    if (target.equals(fileDate)) {
      return;
    } else {
      if (writer != null) {
        writer.close();
        currentFileLock.unlock();
      }
      fileDate = target;
      writer = new DataFileWriter<>(new SpecificDatumWriter<>(LogRecord.class));
      if (codecFact != null) {
        writer.setCodec(codecFact);
      }
      String fileName = fileNameBase + '_' + target.toString() + ".avro";
      currentFile = new File(destinationPath, fileName);
      currentFileLock = FileBasedLock.getLock(new File(destinationPath, fileName + ".lock"));
      boolean locked = currentFileLock.tryLock(1, TimeUnit.MINUTES);
      if (locked) {
        if (currentFile.canWrite()) {
          writer = writer.appendTo(currentFile);
        } else {
          writer.create(LogRecord.getClassSchema(), currentFile);
        }
      } else {
        throw new IOException("cannot acquire lock " + currentFileLock);
      }
    }
  }

  @Override
  protected void append(final ILoggingEvent eventObject) {
    LogRecord record = Converters.convert(eventObject);
    synchronized (this) {
      try {
        ensurePartition(record.getTs());
      } catch (IOException | InterruptedException | RuntimeException ex) {
        this.addError("Unable to setup log file", ex);
      }
      try {
        this.writer.append(record);
      } catch (IOException | RuntimeException ex) {
        this.addError("Unable to write log " + record, ex);
      }
    }
  }

  @Override
  public String toString() {
    return "AvroDataFileAppender{" + "fileNameBase=" + fileNameBase
            + ", writer=" + writer + ", fileDate=" + fileDate + ", currentFile="
            + currentFile + ", destinationPath=" + destinationPath + ", zoneId=" + zoneId + '}';
  }




}
