/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.spf4j.log;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.avro.file.Codec;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;

import org.apache.commons.compress.utils.IOUtils;
import org.spf4j.io.ByteArrayBuilder;
import org.spf4j.recyclable.impl.ArraySuppliers;


/**
 * A more efficient implementation that should thrash less memory.
 * @author Zoltan Farkas
 */
public final class ZstandardCodec extends Codec {

  static class Option extends CodecFactory {

    private final int compressionLevel;
    private final boolean useChecksum;

    Option(final int compressionLevel, final boolean useChecksum) {
      this.compressionLevel = compressionLevel;
      this.useChecksum = useChecksum;
    }

    @Override
    protected Codec createInstance() {
      return new ZstandardCodec(compressionLevel, useChecksum);
    }
  }

  private final int compressionLevel;
  private final boolean useChecksum;

  /**
   * Create a ZstandardCodec instance with the given compressionLevel and checksum option
   *
   */
  public ZstandardCodec(final int compressionLevel, final boolean useChecksum) {
    this.compressionLevel = compressionLevel;
    this.useChecksum = useChecksum;
  }

  @Override
  public String getName() {
    return DataFileConstants.ZSTANDARD_CODEC;
  }

  @Override
  public ByteBuffer compress(final ByteBuffer data) throws IOException {
    int remaining = data.remaining();
    ByteArrayBuilder baos = getOutputBuffer(remaining - remaining / 5);
    try (OutputStream outputStream = ZstandardLoader.output(baos, compressionLevel, useChecksum)) {
      outputStream.write(data.array(), computeOffset(data), remaining);
    }
    return ByteBuffer.wrap(baos.getBuffer(), 0, baos.size());
  }

  @Override
  public ByteBuffer decompress(final ByteBuffer compressedData) throws IOException {
    int remaining = compressedData.remaining();
    ByteArrayBuilder baos = getOutputBuffer(remaining * 2);
    InputStream bytesIn = new ByteArrayInputStream(compressedData.array(), computeOffset(compressedData), remaining);
    try (InputStream ios = ZstandardLoader.input(bytesIn)) {
      IOUtils.copy(ios, baos);
    }
    return ByteBuffer.wrap(baos.getBuffer(), 0, baos.size());
  }

  // get and initialize the output buffer for use.
  private ByteArrayBuilder getOutputBuffer(final int suggestedLength) {
    return new ByteArrayBuilder(suggestedLength, ArraySuppliers.Bytes.JAVA_NEW);
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    return (this == obj) || (obj != null && obj.getClass() == this.getClass());
  }

  @Override
  public String toString() {
    return getName() + "[" + compressionLevel + "]";
  }

  private static final class ZstandardLoader {

    private ZstandardLoader() { }

    static InputStream input(final InputStream compressed) throws IOException {
      return new ZstdInputStream(compressed);
    }

    static OutputStream output(final OutputStream compressed, final int level, final boolean checksum)
            throws IOException {
      int bounded = Math.max(Math.min(level, Zstd.maxCompressionLevel()), Zstd.minCompressionLevel());
      ZstdOutputStream zstdOutputStream = new ZstdOutputStream(compressed, bounded);
      zstdOutputStream.setCloseFrameOnFlush(false);
      zstdOutputStream.setChecksum(checksum);
      return zstdOutputStream;
    }
  }

}
