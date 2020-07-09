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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.logging.Logger;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.spf4j.base.AbstractRunnable;
import org.spf4j.base.avro.LogRecord;
import org.spf4j.concurrent.DefaultExecutor;
import org.spf4j.jmx.JmxExport;
import org.spf4j.jmx.Registry;

/**
 * an Appender that will log into binary avro data files.
 * Files will be be partition by day.
 * Day boundaries are determined by the provided zone id.
 *
 * @author Zoltan Farkas
 */
@SuppressFBWarnings("PATH_TRAVERSAL_IN") // Paths should be comming from trusted sources.
public final class AvroDataFileAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {

  private static final ZoneId ZULU = ZoneId.of("Z");

  private final Object sync = new Object();

  private String fileNameBase;

  private DataFileWriter<LogRecord> writer;

  private LocalDate fileDate;

  private Path currentFile;

  private Path destinationPath;

  private ZoneId zoneId;

  private CodecFactory codecFact;

  private int maxNrFiles;

  private int appendRetries;

  private long maxLogsBytes;

  private CompletableFuture<Void> cleanup;

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
    this.maxNrFiles = 15;
    this.maxLogsBytes = 1024 * 1024 * 100;
    this.cleanup = CompletableFuture.completedFuture(null);
    this.appendRetries = 5;
  }

  /**
   * When serializing a object a ConcurrentModificationEnception can be thrown if this is
   * mutated by another thread. (async logging)
   * this can re-tried, a few times, and hopefully log this successfully.
   *
   * @param appendRetries
   */
  public void setAppendRetries(final int appendRetries) {
    this.appendRetries = appendRetries;
  }

  public void setMaxNrFiles(final int maxNrFiles) {
    if (maxNrFiles < 2) {
      throw new IllegalArgumentException("At least 2 files must be configured:" + maxNrFiles);
    }
    this.maxNrFiles = maxNrFiles;
  }

  public void setMaxLogsBytes(final long maxLogsBytes) {
    if (maxLogsBytes < 10240) {
      throw new IllegalArgumentException("max size too small " + maxLogsBytes);
    }
    this.maxLogsBytes = maxLogsBytes;
  }


  public void setCodec(final String codec) {
    if (codec == null) {
      codecFact = null;
      return;
    }
    switch (codec) {
      case "snappy":
        codecFact = CodecFactory.snappyCodec();
        return;
      case "bzip2":
        codecFact = CodecFactory.bzip2Codec();
        return;
      case "deflate":
        codecFact = CodecFactory.deflateCodec(1);
        return;
      default:
        throw new UnsupportedOperationException("Unsupported codec: " + codec);
    }
  }

  public void setFileNameBase(final String fileNameBase) {
    this.fileNameBase = fileNameBase;
  }

  public void setDestinationPath(final String destinationPath) {
    this.destinationPath = Paths.get(destinationPath);
  }

  public void setPartitionZoneID(final String zoneIdStr) {
    this.zoneId = ZoneId.of(zoneIdStr);
  }

  @JmxExport
  public Path getDestinationPath() {
    return destinationPath;
  }

  @JmxExport
  public void cleanup() throws IOException {
    synchronized (AvroDataFileAppender.class) {
      List<Path> logFiles = getLogFiles();
      if (logFiles.size() > maxNrFiles) {
        int toDelete =  logFiles.size() - maxNrFiles;
        int i = 0;
        Iterator<Path> iterator = logFiles.iterator();
        while (iterator.hasNext()) {
          Path path = iterator.next();
          if (i >= toDelete) {
            break;
          }
          Logger.getLogger(AvroDataFileAppender.class.getName())
                  .log(java.util.logging.Level.INFO, "Deleting {0}", path);
          Files.delete(path);
          iterator.remove();
          i++;
        }
      }
      long size = 0;
      for (Path path : logFiles) {
        size += path.toFile().length();
      }
      Iterator<Path> iterator = logFiles.iterator();
      while (size > maxLogsBytes && iterator.hasNext()) {
        Path path = iterator.next();
        if (iterator.hasNext()) { // do not delete last file...
          size -= path.toFile().length();
          Logger.getLogger(AvroDataFileAppender.class.getName())
                  .log(java.util.logging.Level.INFO, "Deleting {0}", path);
          Files.delete(path);
        }
      }
    }
  }


  /**
   * @return all log files in chronological order.
   * @throws IOException
   */
  @JmxExport
  public List<Path> getLogFiles() throws IOException {
    List<Path> contents = new ArrayList<>();
    try (DirectoryStream<Path> dStream = Files.newDirectoryStream(destinationPath, (path) -> {
      Path fileName = path.getFileName();
      if (fileName == null) {
        return false;
      }
      String name = fileName.toString();
      return name.startsWith(fileNameBase) && name.endsWith(".logs.avro");
    })) {
      for (Path p : dStream) {
        contents.add(p);
      }
    }
    Collections.sort(contents, FileChronoComparator.INSTANCE);
    return contents;
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
     synchronized (sync) {
      return currentFile;
    }
  }

  @JmxExport
  public long flush() throws IOException {
    synchronized (sync) {
      if (this.isStarted()) {
        long location = writer.sync();
        writer.fSync();
        return location;
      } else {
        return -1;
      }
    }
  }



  @JmxExport
  public long getCurrentFileNrLogs() throws IOException {
    return getNrLogs(getCurrentFile());
  }

  public FileReader<LogRecord> getCurrentLogs() throws IOException {
    if (isStarted()) {
      flush();
    }
    return DataFileReader.openReader(getCurrentFile().toFile(), new SpecificDatumReader<>(LogRecord.class));
  }

  public void getLogs(
          final String originPrefix, final long ptailOffset, final long limit, final Consumer<LogRecord> records)
          throws IOException {
     getLogs(getLogFiles(), originPrefix, ptailOffset, limit, records);
  }

  /**
   * Returns all logs in order they have been written to the log files, and reverse order of the input files.
   *
   * @param logFiles
   * @param originPrefix
   * @param ptailOffset
   * @param limit
   * @return
   * @throws IOException
   */
  private void getLogs(final List<Path> logFiles,
          final String originPrefix, final long ptailOffset, final long limit, final Consumer<LogRecord> records)
          throws IOException {
    // try to get from previous file.
    if (logFiles.isEmpty()) {
      return;
    }
    if (isStarted()) {
      flush();
    }
    int i = logFiles.size() - 1;
    long tailOffset = ptailOffset;
    SpecificDatumReader<LogRecord> reader = new SpecificDatumReader<>(LogRecord.class);
    long nrAccepted = 0;
    while (i >= 0 && nrAccepted < limit) {
      Path p = logFiles.get(i);
      long nrRecs = getNrLogs(p);
      nrRecs -= tailOffset;
      if (nrRecs <= 0) {
        tailOffset = -nrRecs;
      }  else {
        tailOffset = 0;
      }
      long left = limit - nrAccepted;
      long toSkip = nrRecs - left;
      try (DataFileStream<LogRecord> stream = new DataFileStream<LogRecord>(Files.newInputStream(p), reader)) {
        if (toSkip > 0) {
          skip(stream, toSkip);
        } else {
          toSkip = 0;
        }
        int j = 0;
        while (nrRecs > 0 && j < left) {
          LogRecord log = stream.next();
          if (originPrefix != null) {
            log.setOrigin(originPrefix + ':' + p + ':' + (toSkip++));
          }
          records.accept(log);
          nrAccepted++;
          nrRecs--;
          j++;
        }
      }
      i--;
    }
  }

  public void getFilteredLogs(final String originPrefix, final long ptailOffset,
          final long limit, final Predicate<LogRecord> pred,
          final Consumer<LogRecord> records)
          throws IOException {
    List<Path> logFiles = getLogFiles();
    if (logFiles.isEmpty()) {
      return;
    }
    if (isStarted()) {
      flush();
    }
    File tmp = writeResultSet(logFiles, originPrefix, pred);
    try {
       getLogs(Collections.singletonList(tmp.toPath()), null, ptailOffset, limit, records);
    } finally {
      if (!tmp.delete()) {
        addError("Unable to delete temp file " + tmp);
      }
    }
  }

  private File writeResultSet(final List<Path> logFiles, final String originPrefix,
          final Predicate<LogRecord> pred)
          throws IOException {
    File tmp = File.createTempFile("scan", "tmp.avro", this.destinationPath.toFile());
    try (DataFileWriter<LogRecord> wr = new DataFileWriter<>(new SpecificDatumWriter<>(LogRecord.class))) {
      if (codecFact != null) {
        wr.setCodec(codecFact);
      }
      wr.create(LogRecord.getClassSchema(), tmp);
      for (Path p : logFiles) {
        FileReader<LogRecord> reader = DataFileReader.openReader(p.toFile(),
                new SpecificDatumReader<>(LogRecord.class));
        int loc = 0;
        while (reader.hasNext()) {
          LogRecord log = reader.next();
          log.setOrigin(originPrefix + ':' + p + ':' + (loc++));
          if (pred.test(log)) {
            wr.append(log);
          }
        }
      }
    } catch (IOException | RuntimeException ex)  {
      if (!tmp.delete()) {
        IOException ioEx = new IOException("Cannot delete " + tmp);
        ioEx.addSuppressed(ex);
        throw ioEx;
      }
      throw ex;
    }
    return tmp;
  }

  public static void skip(final DataFileStream<LogRecord> it,  final long count) throws IOException {
    long i = count;
    while (it.hasNext()) {
      long blockCount = it.getBlockCount();
      if (blockCount > i) {
        break;
      }
      i -= blockCount;
      it.nextBlock();
    }
    LogRecord tmp = new LogRecord();
    for (; i > 0; i--) {
      it.next(tmp);
    }
  }

  public static long getNrLogs(final Path file) throws IOException {
    SpecificDatumReader<LogRecord> reader = new SpecificDatumReader<>(LogRecord.class);
    try (DataFileStream<LogRecord> streamReader = new DataFileStream<LogRecord>(Files.newInputStream(file), reader)) {
      long count = 0L;
      while (streamReader.hasNext()) {
        count += streamReader.getBlockCount();
        streamReader.nextBlock();
      }
      return count;
    }
  }


  @Override
  public void stop() {
    synchronized (sync) {
      if (isStarted()) {
        try {
          writer.close();
        } catch (IOException | RuntimeException ex) {
          addError("Unable to close writer " + writer, ex);
        } finally {
          writer = null;
        }
        Registry.unregister("avro.log.appender", this.getName());
        super.stop();
        try {
          this.cleanup.get(30, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
  }

  @Override
  public void start() {
    Instant now = Instant.now();
    try {
      synchronized (sync) {
        if (!isStarted()) {
          ensurePartition(now);
          Registry.export("avro.log.appender", this.getName(), this);
          super.start();
        }
      }
    } catch (IOException | InterruptedException | RuntimeException ex) {
       addError("Unable to ensure file partition " + now, ex);
    }
  }

  private void ensurePartition(final Instant instant) throws IOException, InterruptedException {
    ZonedDateTime zdt = instant.atZone(zoneId);
    LocalDate target = zdt.toLocalDate();
    if (target.equals(fileDate)) {
      return;
    } else {
      if (writer != null) {
        try {
         writer.close();
        } finally {
          writer = null;
        }
      }
      this.cleanup = this.cleanup.thenRunAsync(new AbstractRunnable(true) {
        @Override
        public void doRun() throws IOException {
          cleanup();
        }
      }, DefaultExecutor.INSTANCE);
      fileDate = target;
      writer = new DataFileWriter<>(new SpecificDatumWriter<>(LogRecord.class));
      if (codecFact != null) {
        writer.setCodec(codecFact);
      }
      String fileName = fileNameBase + '_' + target + ".logs.avro";
      currentFile = destinationPath.resolve(fileName);
      if (Files.isWritable(currentFile) && isValidFile(currentFile)) {
        writer = writer.appendTo(currentFile.toFile());
      } else {
        writer.create(LogRecord.getClassSchema(), currentFile.toFile());
      }
    }
  }

  public static boolean isValidFile(final Path file) throws IOException {
    boolean valid  = true;
    try {
      getNrLogs(file);
    } catch (AvroRuntimeException ex) {
      org.spf4j.base.Runtime.error("Invalid log file " + file, ex);
      rename(file);
      valid =  false;
    }
    return valid;
  }

  private static void rename(final Path file) throws IOException {
    Path parent = file.getParent();
    Path fileName = file.getFileName();
    if (fileName == null) {
      throw new IllegalArgumentException("invalid file path " + file);
    }
    String fileNameStr = fileName.toString();
    if (parent == null) {
      Files.move(file, Paths.get(fileNameStr + ".bad"));
    } else {
      Files.move(file, parent.resolve(fileNameStr + ".bad"));
    }
  }

  @Override
  protected void append(final ILoggingEvent eventObject) {
    LogRecord record = Converters.convert(eventObject);
    Instant ts = record.getTs();
    synchronized (sync) {
      if (!started) {
        org.spf4j.base.Runtime.error("Appending to closed appender " + record);
        this.addError("Appending to closed appender " + record);
        return;
      }
      try {
        ensurePartition(ts);
      } catch (IOException | InterruptedException | RuntimeException ex) {
        org.spf4j.base.Runtime.error("Failed to serialize " + record, ex);
        this.addError("Unable to setup log file", ex);
        return;
      }
      try {
        int tries = 0;
        do {
          try {
            this.writer.append(record);
            break;
          } catch (ConcurrentModificationException ex) {
            // this is just a hack to work-arround when a mutable object is being logged.
            tries++;
            if (tries >= this.appendRetries) {
              org.spf4j.base.Runtime.error("Failed to serialize " + record, ex);
              this.addError("Unable to write log " + record, ex);
              break;
            }
          }
        } while (true);
      } catch (IOException | RuntimeException ex) {
        org.spf4j.base.Runtime.error("Failed to serialize " + record, ex);
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

  private static class FileChronoComparator implements Comparator<Path>, Serializable {

    private static final long serialVersionUID = 1L;
    static final Comparator<Path> INSTANCE = new FileChronoComparator();

    @Override
    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    public int compare(final Path p1, final Path p2) {
      return p1.getFileName().toString().compareTo(p2.getFileName().toString());
    }
  }

}
