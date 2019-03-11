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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
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

  private Path currentFile;

  private FileBasedLock currentFileLock;

  private Path destinationPath;

  private ZoneId zoneId;

  private CodecFactory codecFact;

  public AvroDataFileAppender() {
    zoneId = ZULU;
    fileNameBase = org.spf4j.base.Runtime.PROCESS_NAME;
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

  public void setDestinationPath(final String destinationPath) {
    this.destinationPath = Paths.get(destinationPath);
  }

  public void setPartitionZoneID(final String zoneId) {
    this.zoneId = ZoneId.of(zoneId);
  }

  @JmxExport
  public Path getDestinationPath() {
    return destinationPath;
  }

  public List<Path> getLogFiles() throws IOException {
    List<Path> files = Files.walk(destinationPath)
            .filter((path) -> {
              Path fileName = path.getFileName();
              if (fileName == null) {
                return false;
              }
              String name = fileName.toString();
              return name.startsWith(fileNameBase) && name.endsWith(".avro");
             })
            .collect(Collectors.toList());
    Collections.sort(files, (p1 , p2) -> {
      return p1.getFileName().toString().compareTo(p2.getFileName().toString());
    });
    return files;
  }

  public List<Path> getOldLogFiles() throws IOException {
    List<Path> logFiles = getLogFiles();
    int idx = 0;
    for (Path p : logFiles) {
      if (p.equals(currentFile)) {
        break;
      }
      idx++;
    }
    return logFiles.subList(0, idx);
  }

  @JmxExport
  public Path getCurrentFile() {
    return currentFile;
  }

  @JmxExport
  public synchronized long flush() throws IOException {
    long sync = writer.sync();
    writer.flush();
    return sync;
  }

  public FileReader<LogRecord> getCurrentLogs() throws IOException {
    if (isStarted()) {
      flush();
    }
    return DataFileReader.openReader(currentFile.toFile(), new SpecificDatumReader<>(LogRecord.class));
  }

  public List<LogRecord> getLogs(final long ptailOffset, final int limit) throws IOException {
    if (isStarted()) {
      flush();
    }
    List<LogRecord> result = Collections.EMPTY_LIST;
    // try to get from previous file.
    List<Path> logFiles = getLogFiles();
    if (logFiles.isEmpty()) {
      return result;
    }
    int i = logFiles.size() - 1;
    long tailOffset = ptailOffset;
    while (result.size() < limit && i >= 0) {
      Path p = logFiles.get(i);
      long nrRecs = getNrLogs(p);
      nrRecs -= tailOffset;
      if (nrRecs <= 0) {
        tailOffset = -nrRecs;
      }  else {
        tailOffset = 0;
      }
      int left = limit - result.size();
      long toSkip = nrRecs - left;
      FileReader<LogRecord> reader = DataFileReader.openReader(p.toFile(), new SpecificDatumReader<>(LogRecord.class));
      if (toSkip > 0) {
        skip(reader, toSkip);
      }
      List<LogRecord> intermediate = new ArrayList<>(left);
      while (nrRecs > 0 && intermediate.size() < limit) {
        intermediate.add(reader.next());
        nrRecs--;
      }
      intermediate.addAll(result);
      result = intermediate;
      i--;
    }
    return result;
  }

  public static void skip(final FileReader<LogRecord> it,  final long count) throws IOException {
    LogRecord tmp = new LogRecord();
    for (long i = 0; i< count; i++) {
      it.next(tmp);
    }
  }

  public static long getNrLogs(final Path file) throws IOException {
    GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
    try (DataFileStream<Object> streamReader = new DataFileStream<Object>(Files.newInputStream(file), reader)) {
      long count = 0L;
      try {
        while (streamReader.hasNext()) {
          count += streamReader.getBlockCount();
          streamReader.nextBlock();
        }
      } catch (AvroRuntimeException ex) {
        // reached the end and it might be corupt due to concurent writing.
      }
      return count;
    }
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
      currentFile = destinationPath.resolve(fileName);
      currentFileLock = FileBasedLock.getLock(new File(destinationPath.toFile(), fileName + ".lock"));
      boolean locked = currentFileLock.tryLock(1, TimeUnit.MINUTES);
      if (locked) {
        if (Files.isWritable(currentFile)) {
          writer = writer.appendTo(currentFile.toFile());
        } else {
          writer.create(LogRecord.getClassSchema(), currentFile.toFile());
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
