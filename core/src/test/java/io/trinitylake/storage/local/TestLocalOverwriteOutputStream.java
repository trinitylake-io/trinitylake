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
package io.trinitylake.storage.local;

import io.trinitylake.relocated.com.google.common.collect.ImmutableMap;
import io.trinitylake.relocated.com.google.common.io.Files;
import io.trinitylake.storage.CommonStorageOpsProperties;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestLocalOverwriteOutputStream {

  @Test
  void testBasicWrite(@TempDir Path tempDir) throws IOException {
    CommonStorageOpsProperties commonProperties =
        new CommonStorageOpsProperties(
            ImmutableMap.of(
                CommonStorageOpsProperties.WRITE_STAGING_DIRECTORY, tempDir.toString()));

    Path targetPath = tempDir.resolve("test-" + UUID.randomUUID() + ".txt");
    LocalOverwriteOutputStream stream =
        new LocalOverwriteOutputStream(
            targetPath, commonProperties, LocalStorageOpsProperties.instance());

    stream.write("test data".getBytes(StandardCharsets.UTF_8));
    stream.close();

    String result = Files.asCharSource(targetPath.toFile(), StandardCharsets.UTF_8).read();
    Assertions.assertThat(result).isEqualTo("test data");
  }

  @Test
  void testOverwrite(@TempDir Path tempDir) throws IOException {
    CommonStorageOpsProperties commonProperties =
        new CommonStorageOpsProperties(
            ImmutableMap.of(
                CommonStorageOpsProperties.WRITE_STAGING_DIRECTORY, tempDir.toString()));

    Path targetPath = tempDir.resolve("test-" + UUID.randomUUID() + ".txt");

    LocalOverwriteOutputStream stream =
        new LocalOverwriteOutputStream(
            targetPath, commonProperties, LocalStorageOpsProperties.instance());
    stream.write("first write".getBytes(StandardCharsets.UTF_8));
    stream.close();

    String result = Files.asCharSource(targetPath.toFile(), StandardCharsets.UTF_8).read();
    Assertions.assertThat(result).isEqualTo("first write");

    // Second write should overwrite
    LocalOverwriteOutputStream stream2 =
        new LocalOverwriteOutputStream(
            targetPath, commonProperties, LocalStorageOpsProperties.instance());
    stream2.write("second write".getBytes(StandardCharsets.UTF_8));
    stream2.close();

    result = Files.asCharSource(targetPath.toFile(), StandardCharsets.UTF_8).read();
    Assertions.assertThat(result).isEqualTo("second write");
  }
}
