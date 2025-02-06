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

import java.util.List;
import java.util.Optional;

public interface TreeNode {

  /**
   * Path for a tree node, if the node is persisted in storage
   *
   * @return path
   */
  Optional<String> path();

  void setPath(String path);

  void clearPath();

  /**
   * Creation time of the node file, if the node is persisted in storage
   *
   * @return creation epoch time in millis
   */
  Optional<Long> createdAtMillis();

  void setCreatedAtMillis(long createdAtMillis);

  void clearCreatedAtMillis();

  /**
   * THe number of keys in the node key table
   *
   * @return number of keys
   */
  int numKeys();

  String get(String key);

  void set(String key, String value);

  void remove(String key);

  boolean contains(String key);

  List<NodeKeyTableRow> nodeKeyTable();
}
