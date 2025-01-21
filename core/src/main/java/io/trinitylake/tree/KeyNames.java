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
import java.util.Set;

public class KeyNames {

  public static String LAKEHOUSE_DEFINITION = "lakehouse_def";

  public static String PREVIOUS_ROOT_NODE = "previous_root";

  public static String VERSION = "version";

  public static String ROLLBACK_FROM_ROOT_NODE = "rollback_from_root";

  public static String CREATED_AT_MILLIS = "created_at_millis";

  public static int NAMESPACE_SCHEMA_ID = 1;
  public static int TABLE_SCHEMA_ID = 2;

  public static Set<String> SYSTEM_INTERNAL_KEYS =
      ImmutableSet.<String>builder()
          .add(LAKEHOUSE_DEFINITION)
          .add(PREVIOUS_ROOT_NODE)
          .add(VERSION)
          .add(ROLLBACK_FROM_ROOT_NODE)
          .add(CREATED_AT_MILLIS)
          .build();

  public static boolean isSystemInternalKey(String key) {
    return key.charAt(0) != ' ';
  }
}
