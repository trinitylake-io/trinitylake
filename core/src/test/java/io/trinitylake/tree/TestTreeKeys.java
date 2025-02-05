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
package io.trinitylake.tree;

import io.trinitylake.exception.InvalidArgumentException;
import io.trinitylake.models.LakehouseDef;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTreeKeys {

  @Test
  public void testNamespaceKey() {
    LakehouseDef lakehouseDef = LakehouseDef.newBuilder().setNamespaceNameMaxSizeBytes(8).build();
    Assertions.assertThat(TreeKeys.namespaceKey("ns1", lakehouseDef)).isEqualTo("B===ns1     ");
    Assertions.assertThatThrownBy(() -> TreeKeys.namespaceKey("", lakehouseDef))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("must be provided");
    Assertions.assertThatThrownBy(() -> TreeKeys.namespaceKey("aaaaaaaaa", lakehouseDef))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("must be less than or equal to 8");
  }

  @Test
  public void testIsNamespaceKey() {
    LakehouseDef lakehouseDef = LakehouseDef.newBuilder().setNamespaceNameMaxSizeBytes(8).build();
    Assertions.assertThat(TreeKeys.isNamespaceKey("B===ns1     ", lakehouseDef)).isTrue();
    Assertions.assertThat(TreeKeys.isNamespaceKey("B===ns1  ", lakehouseDef)).isFalse();
    Assertions.assertThat(TreeKeys.isNamespaceKey("b===ns1", lakehouseDef)).isFalse();
  }

  @Test
  public void testNamespaceNameFromKey() {
    LakehouseDef lakehouseDef = LakehouseDef.newBuilder().setNamespaceNameMaxSizeBytes(8).build();
    Assertions.assertThat(TreeKeys.namespaceNameFromKey("B===ns1     ", lakehouseDef))
        .isEqualTo("ns1");
    Assertions.assertThatThrownBy(() -> TreeKeys.namespaceNameFromKey("B===ns1  ", lakehouseDef))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("Invalid namespace key");
  }

  @Test
  public void testTableKey() {
    LakehouseDef lakehouseDef =
        LakehouseDef.newBuilder()
            .setNamespaceNameMaxSizeBytes(8)
            .setTableNameMaxSizeBytes(8)
            .build();
    Assertions.assertThat(TreeKeys.tableKey("ns1", "t1", lakehouseDef))
        .isEqualTo("C===ns1     t1      ");
    Assertions.assertThatThrownBy(() -> TreeKeys.tableKey("", "t1", lakehouseDef))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("must be provided");
    Assertions.assertThatThrownBy(() -> TreeKeys.tableKey("ns1", "", lakehouseDef))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("must be provided");
    Assertions.assertThatThrownBy(() -> TreeKeys.tableKey("aaaaaaaaa", "t1", lakehouseDef))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("must be less than or equal to 8");
    Assertions.assertThatThrownBy(() -> TreeKeys.tableKey("ns1", "aaaaaaaaa", lakehouseDef))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("must be less than or equal to 8");
  }

  @Test
  public void testIsTableKey() {
    LakehouseDef lakehouseDef =
        LakehouseDef.newBuilder()
            .setNamespaceNameMaxSizeBytes(8)
            .setTableNameMaxSizeBytes(8)
            .build();
    Assertions.assertThat(TreeKeys.isTableKey("C===ns1     t1      ", lakehouseDef)).isTrue();
    Assertions.assertThat(TreeKeys.isTableKey("C===ns1  t1   ", lakehouseDef)).isFalse();
    Assertions.assertThat(TreeKeys.isTableKey("c===ns1     t1      ", lakehouseDef)).isFalse();
  }

  @Test
  public void testTableNameFromKey() {
    LakehouseDef lakehouseDef =
        LakehouseDef.newBuilder()
            .setNamespaceNameMaxSizeBytes(8)
            .setTableNameMaxSizeBytes(8)
            .build();
    Assertions.assertThat(TreeKeys.tableNameFromKey("C===ns1     t1      ", lakehouseDef))
        .isEqualTo("t1");
    Assertions.assertThatThrownBy(() -> TreeKeys.tableNameFromKey("B===ns1  ", lakehouseDef))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("Invalid table key");
  }
}
