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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.trinitylake.exception.InvalidArgumentException;
import org.junit.jupiter.api.Test;

public class TestFileLocations {

  @Test
  public void testVersionToRootNodeFilePath() {
    assertThat(FileLocations.rootNodeFilePath(1))
        .isEqualTo("_1000000000000000000000000000000000000000000000000000000000000000.ipc");

    assertThat(FileLocations.rootNodeFilePath(12345678))
        .isEqualTo("_0111001010000110001111010000000000000000000000000000000000000000.ipc");

    assertThat(FileLocations.rootNodeFilePath(9223372036854775802L))
        .isEqualTo("_0101111111111111111111111111111111111111111111111111111111111110.ipc");

    assertThatThrownBy(() -> FileLocations.rootNodeFilePath(-1))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("version must be non-negative");
  }

  @Test
  public void testRootNodeFilePathToVersion() {
    assertThat(
            FileLocations.versionFromNodeFilePath(
                "_1000000000000000000000000000000000000000000000000000000000000000.ipc"))
        .isEqualTo(1);

    assertThat(
            FileLocations.versionFromNodeFilePath(
                "_0111001010000110001111010000000000000000000000000000000000000000.ipc"))
        .isEqualTo(12345678);

    assertThat(
            FileLocations.versionFromNodeFilePath(
                "_0101111111111111111111111111111111111111111111111111111111111110.ipc"))
        .isEqualTo(9223372036854775802L);

    assertThat(FileLocations.isRootNodeFilePath("invalid.txt")).isFalse();
    assertThatThrownBy(() -> FileLocations.versionFromNodeFilePath("invalid.txt"))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("Root node file path must match pattern");

    assertThat(
            FileLocations.isRootNodeFilePath(
                "_2000000000000000000000000000000000000000000000000000000000000000.ipc"))
        .isFalse();
    assertThatThrownBy(
            () ->
                FileLocations.versionFromNodeFilePath(
                    "_2000000000000000000000000000000000000000000000000000000000000000.ipc"))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("Root node file path must match pattern");
  }

  @Test
  public void testLakehouseDefFilePath() {
    assertThat(FileLocations.newLakehouseDefFilePath())
        .startsWith(FileLocations.LAKEHOUSE_DEF_FILE_PATH_PREFIX)
        .endsWith(FileLocations.PROTOBUF_BINARY_FILE_SUFFIX);
  }

  @Test
  public void testNamespaceDefFilePath() {
    assertThat(FileLocations.newNamespaceDefFilePath("ns1"))
        .contains("ns1")
        .endsWith(FileLocations.PROTOBUF_BINARY_FILE_SUFFIX)
        .hasSize(23 + "-ns1-".length() + 36 + FileLocations.PROTOBUF_BINARY_FILE_SUFFIX.length());
  }

  @Test
  public void testTableDefFilePath() {
    assertThat(FileLocations.newTableDefFilePath("ns1", "t1"))
        .contains("ns1")
        .contains("t1")
        .endsWith(FileLocations.PROTOBUF_BINARY_FILE_SUFFIX)
        .hasSize(
            23
                + "-ns1-".length()
                + "t1-".length()
                + 36
                + FileLocations.PROTOBUF_BINARY_FILE_SUFFIX.length());
  }
}
