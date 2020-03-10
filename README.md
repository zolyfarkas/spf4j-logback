# spf4j-logback

A collection of logback components to implement:

 * Log directly to avro data files. Significantly more compacter (5x), structured. See schema [at](https://zolyfarkas.github.io/core-schema/avrodoc.html#/schema/org%2Fspf4j%2Fbase%2Favro%2FLogRecord.avsc/org.spf4j.base.avro.LogRecord)

 * Json encoder to use with stock logback appenders.

 * Available /logs JAX-RS [actuator endpoint](https://github.com/zolyfarkas/jaxrs-spf4j-demo/wiki/JaxRsActuator), that allows you fast access to your logs, across your entire cluster. (in spf4j-jaxrs-actuator)

 latest version: [![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.spf4j/spf4j-logback/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.spf4j/spf4j-logback/)


To use it all you need to do it to add :

```
    <dependency>
      <groupId>org.spf4j</groupId>
      <artifactId>spf4j-logback</artifactId>
      <version>LATEST</version>
    </dependency>
```

and configure your logback.xml accordingly.

The library contains a default [logback.xml](https://github.com/zolyfarkas/spf4j-logback/blob/master/src/main/resources/logback.xml)
that will log to the console in text format. The console output is usually used during the initialization and destruction phase
of the process, before and after [LogbackService](https://github.com/zolyfarkas/spf4j-logback/blob/master/src/main/java/org/spf4j/log/LogbackService.java).
LogbackService defaults to [logback-avro.xml](https://github.com/zolyfarkas/spf4j-logback/blob/master/src/main/resources/logback-avro.xml) which will
log all logs to a avro binary file.

Here is a example logback configuration to enable all logs to a avro log file, and errors to the console in json format as well.

```
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
   <shutdownHook/>

  	<appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
          <encoder class="org.spf4j.log.ReadableLogbackEncoder"/>
              <!-- you want JSON, use this:
		<encoder class="org.spf4j.log.AvroLogbackEncoder"/>
              -->
          <filter class="ch.qos.logback.classic.filter.ThresholdFilter">
            <level>ERROR</level>
          </filter>
	</appender>

	<appender name="default" class="org.spf4j.log.AvroDataFileAppender">
          <fileNameBase>${logFileBase}</fileNameBase>
          <destinationPath>/var/log</destinationPath>
	</appender>

        <appender name="ASYNC_FILE" class="ch.qos.logback.classic.AsyncAppender">
          <appender-ref ref="default"/>
          <queueSize>500</queueSize>
          <maxFlushTime>1000</maxFlushTime>
       </appender>

	<logger name="org.spf4j" level="info" additivity="false">
		<appender-ref ref="ASYNC_FILE" />
	</logger>

	<root level="warn">
          <appender-ref ref="ASYNC_FILE" />
          <appender-ref ref="CONSOLE" />
	</root>

</configuration>
```
