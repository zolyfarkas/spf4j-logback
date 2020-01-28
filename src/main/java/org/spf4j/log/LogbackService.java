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

/**
 *
 * @author Zoltan Farkas
 */
public final class LogbackService {

  static {
    redirecJDKLogging2Slf4j();
  }


  private final String applicationName;

  private final String logFolder;

  private final String fileNameBase;

  private final String mainConfigFile;

  public LogbackService(final String applicationName, final String logFolder, final String fileNameBase) {
    this(applicationName, logFolder, fileNameBase, "logback-avro.xml");
  }

  public LogbackService(final String applicationName, final String logFolder, final String fileNameBase,
          final String mainConfigFile) {
    this.applicationName = applicationName;
    this.logFolder = logFolder;
    this.fileNameBase = fileNameBase;
    this.mainConfigFile = mainConfigFile;
    System.setProperty("appName", applicationName); // for logback config xml.
    System.setProperty("logFolder", logFolder); // for logback config xml.
    System.setProperty("logFileBase", fileNameBase); // for logback config xml.
  }

  public static void redirecJDKLogging2Slf4j() {
    // install java.util.logging -> org.slf4j.logging bridge
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }


  public void start() {
    LogbackUtils.reconfigure(mainConfigFile);
  }

  public void stop() {
    LogbackUtils.reconfigure("logback.xml");
  }

  public String getApplicationName() {
    return applicationName;
  }

  public String getLogFolder() {
    return logFolder;
  }

  public String getFileNameBase() {
    return fileNameBase;
  }

  public String getMainConfigFile() {
    return mainConfigFile;
  }

  @Override
  public String toString() {
    return "LogbackService{" + "applicationName=" + applicationName + ", logFolder="
            + logFolder + ", fileNameBase=" + fileNameBase + ", mainConfigFile=" + mainConfigFile + '}';
  }

}
