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
import ch.qos.logback.core.encoder.EncoderBase;
import java.nio.charset.Charset;
import java.time.format.DateTimeFormatter;
import org.apache.avro.util.Arrays;

/**
 * @author Zoltan Farkas
 */
public final class ReadableLogbackEncoder extends EncoderBase<ILoggingEvent> {

  /**
   * The charset to use when converting a String into bytes.
   * <p/>
   * By default this property has the value <code>null</null> which corresponds to the system's default charset.
   */
  private Charset charset;

  private LogPrinter printer;

  public ReadableLogbackEncoder() {
    this.charset = Charset.defaultCharset();
  }

  public void setCharset(final String charsetName) {
    this.charset = Charset.forName(charsetName);
  }

  @Override
  public void start() {
    printer = new LogPrinter(DateTimeFormatter.ISO_INSTANT, charset);
    super.start();
  }

  @Override
  public byte[] headerBytes() {
      return Arrays.EMPTY_BYTE_ARRAY;
  }

  @Override
  public byte[] encode(final ILoggingEvent event) {
    return printer.printToBytes(Converters.convert2(event));
  }


  @Override
  public byte[] footerBytes() {
    return Arrays.EMPTY_BYTE_ARRAY;
  }

  @Override
  public String toString() {
    return "ReadableLogbackEncoder{" + "charset=" + charset + ", printer=" + printer + '}';
  }

}
