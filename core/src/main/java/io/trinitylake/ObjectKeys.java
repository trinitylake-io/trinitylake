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

import io.trinitylake.models.LakehouseDef;
import io.trinitylake.relocated.com.google.common.collect.ImmutableSet;
import io.trinitylake.util.ValidationUtil;
import java.nio.charset.StandardCharsets;
import java.util.Set;

public class ObjectKeys {

  public static final String LAKEHOUSE_DEFINITION = "lakehouse_def";
  public static final byte[] LAKEHOUSE_DEFINITION_BYTES =
      LAKEHOUSE_DEFINITION.getBytes(StandardCharsets.UTF_8);

  public static final String PREVIOUS_ROOT_NODE = "previous_root";
  public static final byte[] PREVIOUS_ROOT_NODE_BYTES =
      PREVIOUS_ROOT_NODE.getBytes(StandardCharsets.UTF_8);

  public static final String ROLLBACK_FROM_ROOT_NODE = "rollback_from_root";
  public static final byte[] ROLLBACK_FROM_ROOT_NODE_BYTES =
      ROLLBACK_FROM_ROOT_NODE.getBytes(StandardCharsets.UTF_8);

  public static final String NUMBER_OF_KEYS = "n_keys";
  public static final byte[] NUMBER_OF_KEYS_BYTES = NUMBER_OF_KEYS.getBytes(StandardCharsets.UTF_8);

  public static final String CREATED_AT_MILLIS = "created_at_millis";
  public static final byte[] CREATED_AT_MILLIS_BYTES =
      CREATED_AT_MILLIS.getBytes(StandardCharsets.UTF_8);

  public static final Set<String> SYSTEM_INTERNAL_KEYS =
      ImmutableSet.<String>builder()
          .add(LAKEHOUSE_DEFINITION)
          .add(PREVIOUS_ROOT_NODE)
          .add(ROLLBACK_FROM_ROOT_NODE)
          .add(CREATED_AT_MILLIS)
          .add(NUMBER_OF_KEYS)
          .build();

  private static final int SCHEMA_ID_PART_SIZE = 4;
  private static final String NAMESPACE_SCHEMA_ID_PART = "B===";
  private static final String TABLE_SCHEMA_ID_PART = "C===";
  private static final String VIEW_SCHEMA_ID_PART = "D===";

  private ObjectKeys() {}

  public static String namespaceKey(String namespaceName, LakehouseDef lakehouseDef) {
    ValidationUtil.checkNotNull(lakehouseDef, "Lakehouse definition must be provided");
    ValidationUtil.checkNotNullOrEmptyString(namespaceName, "namespace name must be provided");
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

  public static String namespaceNameFromKey(String namespaceKey, LakehouseDef lakehouseDef) {
    ValidationUtil.checkArgument(
        isNamespaceKey(namespaceKey, lakehouseDef), "Invalid namespace key: %s", namespaceKey);
    return namespaceKey.substring(SCHEMA_ID_PART_SIZE).trim();
  }

  public static boolean isNamespaceKey(String key, LakehouseDef lakehouseDef) {
    ValidationUtil.checkNotNull(lakehouseDef, "Lakehouse definition must be provided");
    ValidationUtil.checkNotNullOrEmptyString(key, "key must be provided");
    return key.startsWith(NAMESPACE_SCHEMA_ID_PART)
        && key.length() == SCHEMA_ID_PART_SIZE + lakehouseDef.getNamespaceNameMaxSizeBytes();
  }

  public static String tableKey(String namespaceName, String tableName, LakehouseDef lakehouseDef) {
    ValidationUtil.checkNotNull(lakehouseDef, "Lakehouse definition must be provided");
    ValidationUtil.checkNotNullOrEmptyString(namespaceName, "namespace name must be provided");
    ValidationUtil.checkNotNullOrEmptyString(tableName, "table name must be provided");
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

  public static String tableNameFromKey(String tableKey, LakehouseDef lakehouseDef) {
    ValidationUtil.checkArgument(
        isTableKey(tableKey, lakehouseDef), "Invalid table key: %s", tableKey);
    return tableKey
        .substring(SCHEMA_ID_PART_SIZE + lakehouseDef.getNamespaceNameMaxSizeBytes())
        .trim();
  }

  public static boolean isTableKey(String key, LakehouseDef lakehouseDef) {
    ValidationUtil.checkNotNull(lakehouseDef, "Lakehouse definition must be provided");
    ValidationUtil.checkNotNullOrEmptyString(key, "key must be provided");
    return key.startsWith(TABLE_SCHEMA_ID_PART)
        && key.length()
            == (SCHEMA_ID_PART_SIZE
                + lakehouseDef.getNamespaceNameMaxSizeBytes()
                + lakehouseDef.getTableNameMaxSizeBytes());
  }

  public static String viewKey(String namespaceName, String viewName, LakehouseDef lakehouseDef) {
    return null;
  }

  public static String viewNameFromKey(String viewName, LakehouseDef lakehouseDef) {
    return null;
  }

  public static boolean isViewKey(String key, LakehouseDef lakehouseDef) {
    return false;
  }
}
