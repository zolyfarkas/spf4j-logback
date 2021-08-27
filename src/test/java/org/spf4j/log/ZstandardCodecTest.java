/*
 * Copyright 2021 SPF4J.
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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Zoltan Farkas
 */
public class ZstandardCodecTest {

  @Test
  public void testCompDecomp() throws IOException {
    ZstandardCodec codec = new ZstandardCodec(8, true);
    String testString = "This isu a test stringu";
    ByteBuffer comp = codec.compress(ByteBuffer.wrap(testString.getBytes(StandardCharsets.UTF_8)));
    ByteBuffer decomp = codec.decompress(comp);
    Assert.assertEquals(testString,
            new String(decomp.array(), decomp.arrayOffset(), decomp.limit(), StandardCharsets.UTF_8));

  }

}
