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

import io.trinitylake.exception.InvalidArgumentException;
import io.trinitylake.storage.FilePaths;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFilePaths {

  @Test
  public void testVersionToRootNodeFilePath() {
    Assertions.assertThat(FilePaths.rootNodeFilePath(1))
        .isEqualTo("_1000000000000000000000000000000000000000000000000000000000000000.ipc");

    Assertions.assertThat(FilePaths.rootNodeFilePath(12345678))
        .isEqualTo("_0111001010000110001111010000000000000000000000000000000000000000.ipc");

    Assertions.assertThat(FilePaths.rootNodeFilePath(9223372036854775802L))
        .isEqualTo("_0101111111111111111111111111111111111111111111111111111111111110.ipc");

    Assertions.assertThatThrownBy(() -> FilePaths.rootNodeFilePath(-1))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("version must be non-negative");
  }

  @Test
  public void testRootNodeFilePathToVersion() {
    Assertions.assertThat(
            FilePaths.versionFromNodeFilePath(
                "_1000000000000000000000000000000000000000000000000000000000000000.ipc"))
        .isEqualTo(1);

    Assertions.assertThat(
            FilePaths.versionFromNodeFilePath(
                "_0111001010000110001111010000000000000000000000000000000000000000.ipc"))
        .isEqualTo(12345678);

    Assertions.assertThat(
            FilePaths.versionFromNodeFilePath(
                "_0101111111111111111111111111111111111111111111111111111111111110.ipc"))
        .isEqualTo(9223372036854775802L);

    Assertions.assertThat(FilePaths.isRootNodeFilePath("invalid.txt")).isFalse();
    Assertions.assertThatThrownBy(() -> FilePaths.versionFromNodeFilePath("invalid.txt"))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("Root node file path must match pattern");

    Assertions.assertThat(
            FilePaths.isRootNodeFilePath(
                "_2000000000000000000000000000000000000000000000000000000000000000.ipc"))
        .isFalse();
    Assertions.assertThatThrownBy(
            () ->
                FilePaths.versionFromNodeFilePath(
                    "_2000000000000000000000000000000000000000000000000000000000000000.ipc"))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("Root node file path must match pattern");
  }

  @Test
  public void testLakehouseDefFilePath() {
    Assertions.assertThat(FilePaths.newLakehouseDefFilePath())
        .startsWith(FilePaths.LAKEHOUSE_DEF_FILE_PATH_PREFIX)
        .endsWith(FilePaths.PROTOBUF_BINARY_FILE_SUFFIX);
  }

  @Test
  public void testNamespaceDefFilePath() {
    Assertions.assertThat(FilePaths.newNamespaceDefFilePath("ns1"))
        .contains("ns1")
        .endsWith(FilePaths.PROTOBUF_BINARY_FILE_SUFFIX)
        .hasSize(23 + "-ns1-".length() + 36 + FilePaths.PROTOBUF_BINARY_FILE_SUFFIX.length());
  }

  @Test
  public void testTableDefFilePath() {
    Assertions.assertThat(FilePaths.newTableDefFilePath("ns1", "t1"))
        .contains("ns1")
        .contains("t1")
        .endsWith(FilePaths.PROTOBUF_BINARY_FILE_SUFFIX)
        .hasSize(
            23
                + "-ns1-".length()
                + "t1-".length()
                + 36
                + FilePaths.PROTOBUF_BINARY_FILE_SUFFIX.length());
  }
}
