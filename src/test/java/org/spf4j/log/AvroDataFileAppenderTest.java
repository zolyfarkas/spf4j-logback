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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.avro.AvroFormatConfig;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.exec.store.sys.store.provider.ZookeeperPersistentStoreProvider;
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

  /**
   * see
   */
  public static void configureFormatPlugins(final StoragePluginRegistry pluginRegistry,
          final String storagePlugin, final String avroPath)
          throws ExecutionSetupException {
    FileSystemPlugin fileSystemPlugin = (FileSystemPlugin) pluginRegistry.getPlugin(storagePlugin);
    FileSystemConfig fileSystemConfig = (FileSystemConfig) fileSystemPlugin.getConfig();

    Map<String, FormatPluginConfig> newFormats = new HashMap<>();
    Optional.ofNullable(fileSystemConfig.getFormats())
            .ifPresent(newFormats::putAll);

    AvroFormatConfig avroConfig = new AvroFormatConfig();
    newFormats.put("avro", avroConfig);

    Map<String, WorkspaceConfig> workspaces = new HashMap<>();
    workspaces.putAll(fileSystemConfig.getWorkspaces());
    workspaces.put("tmp", new WorkspaceConfig(avroPath, true, "avro", false));
    workspaces.put("avro", new WorkspaceConfig(avroPath, true, "avro", false));
    workspaces.put(SchemaFactory.DEFAULT_WS_NAME, new WorkspaceConfig(avroPath, true, "avro", false));

    FileSystemConfig newFileSystemConfig = new FileSystemConfig(
            fileSystemConfig.getConnection(),
            fileSystemConfig.getConfig(),
            workspaces,
            newFormats);

    newFileSystemConfig.setEnabled(Boolean.TRUE);
    pluginRegistry.createOrUpdate(storagePlugin, newFileSystemConfig, true);
  }

  @Test
  @Ignore
  // experiments....
  public void testdrill() throws IOException, InterruptedException, Exception {
    final AvroDataFileAppender appender = new AvroDataFileAppender();
    String destFolder = new File(org.spf4j.base.Runtime.TMP_FOLDER + "/avro").getCanonicalPath();
    appender.setDestinationPath(destFolder);
    appender.setFileNameBase("tesAvroLog");
    appender.setPartitionZoneID(ZoneId.systemDefault().getId());
    appender.start();
    for (int i = 0; i < 1000; i++) {
      appender.append(new TestLogEvent(Instant.now(), "", "a", 3, 4, LogAttribute.traceId("cucu")));
      Thread.sleep(1);
    }
    appender.stop();
    String tmpFolder = org.spf4j.base.Runtime.TMP_FOLDER;
    Properties props = new Properties();
    // Properties here mimic those in drill-root/pom.xml, Surefire plugin
    // configuration. They allow tests to run successfully in IDE.
    props.setProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, "false");

    // The CTTAS function requires that the default temporary workspace be
    // writable. By default, the default temporary workspace points to
    // dfs.tmp. But, the test setup marks dfs.tmp as read-only. To work
    // around this, tests are supposed to use dfs. So, we need to
    // set the default temporary workspace to dfs.tmp.
    props.setProperty(ExecConstants.DEFAULT_TEMPORARY_WORKSPACE, "dfs.tmp");
    props.setProperty(ExecConstants.HTTP_ENABLE, "false");
    props.setProperty("drill.catastrophic_to_standard_out", "true");

    // Verbose errors.
    props.setProperty(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY, "true");

    // See Drillbit.close. The Drillbit normally waits a specified amount
    // of time for ZK registration to drop. But, embedded Drillbits normally
    // don't use ZK, so no need to wait.
    props.setProperty(ExecConstants.ZK_REFRESH, "0");

    // This is just a test, no need to be heavy-duty on threads.
    // This is the number of server and client RPC threads. The
    // production default is DEFAULT_SERVER_RPC_THREADS.
    props.setProperty(ExecConstants.BIT_SERVER_RPC_THREADS, "2");

    // No need for many scanners except when explicitly testing that
    // behavior. Production default is DEFAULT_SCAN_THREADS
    props.setProperty(ExecConstants.SCAN_THREADPOOL_SIZE, "4");

    // Define a useful root location for the ZK persistent
    // storage. Profiles will go here when running in distributed
    // mode.
    props.setProperty(ZookeeperPersistentStoreProvider.DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT,
            tmpFolder + "/drill/sstore/zk");
    props.setProperty(ExecConstants.DRILL_TMP_DIR, tmpFolder + "/drill");
    props.setProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_PATH, tmpFolder + "/drill/sstore");

    DrillConfig cfg = DrillConfig.create(props);

    Drillbit bit = new Drillbit(cfg, RemoteServiceSet.getLocalServiceSet());
    bit.run();
    final StoragePluginRegistry pluginRegistry = bit.getContext().getStorage();

    configureFormatPlugins(pluginRegistry, "dfs", destFolder);

    DrillClient client = new DrillClient(true);
    Properties clProps = new Properties();
    clProps.setProperty(DrillProperties.DRILLBIT_CONNECTION, String.format("localhost:%s", bit.getUserPort()));
    client.connect(clProps);
    DrillRpcFuture<UserProtos.CreatePreparedStatementResp> pps
            = client.createPreparedStatement("select * from dfs.avro.`/`");
    List<QueryDataBatch> qdb = client.executePreparedStatement(pps.get().getPreparedStatement().getServerHandle());
    LOG.debug("get result: {}", qdb);
  }

}