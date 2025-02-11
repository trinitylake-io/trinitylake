/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trinitylake;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;

// inspired in part by
// https://github.com/apache/avro/blob/release-1.8.2/lang/java/guava/src/main/java/org/apache/avro/GuavaClasses.java
@SuppressWarnings("ReturnValueIgnored")
public class GuavaClasses {

  /*
   * Referencing Guava classes here includes them in the minimized and relocated Guava jar
   */
  static {
    Throwables.class.getName();
    HashCode.class.getName();
    HashFunction.class.getName();
    Hashing.class.getName();
    Maps.class.getName();
    CharStreams.class.getName();
    CountingOutputStream.class.getName();
    ByteStreams.class.getName();
    MoreExecutors.class.getName();
    Files.class.getName();
  }
}
