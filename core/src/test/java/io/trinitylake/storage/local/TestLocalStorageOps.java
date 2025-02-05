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

import io.trinitylake.storage.LiteralURI;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestLocalStorageOps {

  @Test
  public void testListing(@TempDir Path tempDir) throws IOException {
    Path dir = tempDir.resolve("testListing");
    boolean created = dir.toFile().mkdirs();
    Assertions.assertThat(created).isTrue();

    for (int i = 0; i < 10; i++) {
      Path file = dir.resolve("f" + i + ".txt");
      Files.write(file, "data".getBytes());
    }

    LocalStorageOps ops = new LocalStorageOps();
    List<LiteralURI> results = ops.list(new LiteralURI("file://" + dir));
    Assertions.assertThat(results.size()).isEqualTo(10);
  }

  @Test
  public void testFileExists(@TempDir Path tempDir) throws IOException {
    Path file = tempDir.resolve("exists.txt");
    Files.write(file, "data".getBytes());

    LocalStorageOps ops = new LocalStorageOps();
    Assertions.assertThat(ops.exists(new LiteralURI("file://" + file))).isTrue();
    Assertions.assertThat(ops.exists(new LiteralURI("file://" + tempDir.resolve("not-exist.txt"))))
        .isFalse();
  }

  @Test
  public void testDeleteFiles(@TempDir Path tempDir) throws IOException {

    List<Path> files =
        IntStream.range(0, 10)
            .mapToObj(
                i -> {
                  try {
                    Path file = tempDir.resolve("f" + i + ".txt");
                    Files.write(file, "data".getBytes());
                    return file;
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());
    files.stream().forEach(f -> Assertions.assertThat(f.toFile().exists()).isTrue());

    LocalStorageOps ops = new LocalStorageOps();
    ops.delete(files.stream().map(f -> new LiteralURI("file://" + f)).collect(Collectors.toList()));

    files.stream().forEach(f -> Assertions.assertThat(f.toFile().exists()).isFalse());
  }
}
