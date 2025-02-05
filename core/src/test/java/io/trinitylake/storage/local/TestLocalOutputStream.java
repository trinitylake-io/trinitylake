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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.trinitylake.exception.StorageAtomicSealFailureException;
import io.trinitylake.storage.CommonStorageOpsProperties;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestLocalOutputStream {

  @Test
  public void testWriteAndAtomicallySeal(@TempDir Path tempDir) throws IOException {
    CommonStorageOpsProperties commonProperties =
        new CommonStorageOpsProperties(
            ImmutableMap.of(
                CommonStorageOpsProperties.WRITE_STAGING_DIRECTORY, tempDir.toString()));

    Path targetFilePath = tempDir.resolve("target-" + UUID.randomUUID() + ".txt");
    LocalOutputStream stream =
        new LocalOutputStream(
            targetFilePath, commonProperties, LocalStorageOpsProperties.instance());
    stream.write("some data".getBytes(StandardCharsets.UTF_8));
    stream.close();

    String result = Files.asCharSource(targetFilePath.toFile(), StandardCharsets.UTF_8).read();
    Assertions.assertThat(result).isEqualTo("some data");
  }

  @Test
  public void testTwoConcurrentSeal(@TempDir Path tempDir) throws IOException {
    CommonStorageOpsProperties commonProperties =
        new CommonStorageOpsProperties(
            ImmutableMap.of(
                CommonStorageOpsProperties.WRITE_STAGING_DIRECTORY, tempDir.toString()));

    Path targetFilePath = tempDir.resolve("target-" + UUID.randomUUID() + ".txt");

    LocalOutputStream stream1 =
        new LocalOutputStream(
            targetFilePath, commonProperties, LocalStorageOpsProperties.instance());
    stream1.write("some data 1".getBytes(StandardCharsets.UTF_8));

    LocalOutputStream stream2 =
        new LocalOutputStream(
            targetFilePath, commonProperties, LocalStorageOpsProperties.instance());
    stream2.write("some data 2".getBytes(StandardCharsets.UTF_8));

    stream1.close();
    Assertions.assertThatThrownBy(() -> stream2.close())
        .isInstanceOf(StorageAtomicSealFailureException.class);

    String result = Files.asCharSource(targetFilePath.toFile(), StandardCharsets.UTF_8).read();
    Assertions.assertThat(result).isEqualTo("some data 1");
  }

  @Test
  public void testHighlyConcurrentSeal(@TempDir Path tempDir) throws IOException {
    CommonStorageOpsProperties commonProperties =
        new CommonStorageOpsProperties(
            ImmutableMap.of(
                CommonStorageOpsProperties.WRITE_STAGING_DIRECTORY, tempDir.toString()));

    Path targetFilePath = tempDir.resolve("target-" + UUID.randomUUID() + ".txt");
    AtomicInteger failedWrites = new AtomicInteger(0);
    List<LocalOutputStream> streams =
        IntStream.range(0, 128)
            .mapToObj(
                i -> {
                  try {
                    LocalOutputStream stream =
                        new LocalOutputStream(
                            targetFilePath, commonProperties, LocalStorageOpsProperties.instance());
                    stream.write("some data".getBytes(StandardCharsets.UTF_8));
                    return stream;
                  } catch (IOException e) {
                    failedWrites.incrementAndGet();
                    return null;
                  }
                })
            .collect(Collectors.toList());

    Assertions.assertThat(failedWrites.get()).isEqualTo(0);

    AtomicInteger failedClose = new AtomicInteger(0);
    streams.parallelStream()
        .forEach(
            s -> {
              try {
                s.close();
              } catch (RuntimeException e) {
                Assertions.assertThat(e).isInstanceOf(StorageAtomicSealFailureException.class);
                failedClose.incrementAndGet();
              } catch (IOException e) {
                // this should not happen
                throw new RuntimeException(e);
              }
            });

    String result = Files.asCharSource(targetFilePath.toFile(), StandardCharsets.UTF_8).read();
    Assertions.assertThat(result).isEqualTo("some data");
  }
}
