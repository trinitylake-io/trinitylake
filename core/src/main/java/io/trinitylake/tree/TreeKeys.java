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

import com.google.common.collect.ImmutableSet;
import io.trinitylake.models.LakehouseDef;
import io.trinitylake.util.ValidationUtil;
import java.util.Set;

public class TreeKeys {

  public static String LAKEHOUSE_DEFINITION = "lakehouse_def";

  public static String PREVIOUS_ROOT_NODE = "previous_root";

  public static String VERSION = "version";

  public static String ROLLBACK_FROM_ROOT_NODE = "rollback_from_root";

  public static String CREATED_AT_MILLIS = "created_at_millis";

  public static Set<String> SYSTEM_INTERNAL_KEYS =
      ImmutableSet.<String>builder()
          .add(LAKEHOUSE_DEFINITION)
          .add(PREVIOUS_ROOT_NODE)
          .add(VERSION)
          .add(ROLLBACK_FROM_ROOT_NODE)
          .add(CREATED_AT_MILLIS)
          .build();

  private static final int SCHEMA_ID_PART_SIZE = 4;
  private static final String NAMESPACE_SCHEMA_ID_PART = "B===";
  private static final String TABLE_SCHEMA_ID_PART = "C===";

  public static String namespaceKey(String namespaceName, LakehouseDef lakehouseDef) {
    ValidationUtil.checkArgument(
        namespaceName.length() <= lakehouseDef.getNamespaceNameMaxSizeBytes(),
        "namespace name %s must be less than or equal to %s in lakehouse definition",
        namespaceName,
        lakehouseDef.getNamespaceNameMaxSizeBytes());

    StringBuilder sb = new StringBuilder();
    sb.append(NAMESPACE_SCHEMA_ID_PART);
    sb.append(namespaceName);
    for (int i = 0; i < lakehouseDef.getNamespaceNameMaxSizeBytes() - namespaceName.length(); i++) {
      sb.append(' ');
    }

    return sb.toString();
  }

  public static String namespaceNameFromKey(String namespaceKey) {
    return namespaceKey.substring(SCHEMA_ID_PART_SIZE).trim();
  }

  public static boolean isNamespaceKey(String key) {
    return key.startsWith(NAMESPACE_SCHEMA_ID_PART);
  }

  public static String tableKey(String namespaceName, String tableName, LakehouseDef lakehouseDef) {
    ValidationUtil.checkArgument(
        namespaceName.length() <= lakehouseDef.getNamespaceNameMaxSizeBytes(),
        "namespace name %s must be less than or equal to %s in lakehouse definition",
        namespaceName,
        lakehouseDef.getNamespaceNameMaxSizeBytes());

    ValidationUtil.checkArgument(
        tableName.length() <= lakehouseDef.getTableNameMaxSizeBytes(),
        "table name %s must be less than or equal to %s in lakehouse definition",
        tableName,
        lakehouseDef.getTableNameMaxSizeBytes());

    StringBuilder sb = new StringBuilder();
    sb.append(TABLE_SCHEMA_ID_PART);
    sb.append(namespaceName);
    for (int i = 0; i < lakehouseDef.getNamespaceNameMaxSizeBytes() - namespaceName.length(); i++) {
      sb.append(' ');
    }

    sb.append(tableName);
    for (int i = 0; i < lakehouseDef.getTableNameMaxSizeBytes() - tableName.length(); i++) {
      sb.append(' ');
    }

    return sb.toString();
  }

  public static String tableNameFromKey(String namespaceName, String tableKey) {
    return tableKey.substring(SCHEMA_ID_PART_SIZE + namespaceName.length()).trim();
  }

  public static boolean isTableKey(String key) {
    return key.startsWith(TABLE_SCHEMA_ID_PART);
  }
}
