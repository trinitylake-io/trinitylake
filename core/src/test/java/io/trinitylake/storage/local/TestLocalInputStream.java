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

import io.trinitylake.exception.StorageFileOpenFailureException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestLocalInputStream {

  @Test
  public void testReadFileNotExist() {
    Assertions.assertThatThrownBy(() -> new LocalInputStream(new File("not-exist.txt")))
        .isInstanceOf(StorageFileOpenFailureException.class)
        .hasCauseInstanceOf(FileNotFoundException.class);
  }

  @Test
  public void testReadFileAndSkip(@TempDir Path tempDir) throws IOException {
    Path file = tempDir.resolve("testReadFileAndSkip.txt");
    Files.write(
        file,
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit".getBytes(StandardCharsets.UTF_8));

    LocalInputStream stream = new LocalInputStream(file.toFile());

    byte[] buffer = new byte[10];
    int len = stream.read(buffer);
    Assertions.assertThat(len).isEqualTo(10);
    Assertions.assertThat(new String(buffer, StandardCharsets.UTF_8)).isEqualTo("Lorem ipsu");

    // seek beyond
    stream.seek(20);
    len = stream.read(buffer);
    Assertions.assertThat(len).isEqualTo(10);
    Assertions.assertThat(new String(buffer, StandardCharsets.UTF_8)).isEqualTo("t amet, co");

    // seek back
    stream.seek(10);
    len = stream.read(buffer);
    Assertions.assertThat(len).isEqualTo(10);
    Assertions.assertThat(new String(buffer, StandardCharsets.UTF_8)).isEqualTo("m dolor si");
  }

  @Test
  public void testReadFileSeekBeyondEnd(@TempDir Path tempDir) throws IOException {
    Path file = tempDir.resolve("testReadFileSeekBeyondEnd.txt");
    Files.write(file, "test".getBytes(StandardCharsets.UTF_8));

    LocalInputStream stream = new LocalInputStream(file.toFile());

    stream.seek(10);
    byte[] buffer = new byte[10];
    int len = stream.read(buffer);
    Assertions.assertThat(len).isEqualTo(-1);
  }
}
