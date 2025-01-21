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

import io.trinitylake.util.Pair;

public interface TreeNode {

  long createdAtMillis();

  /**
   * Find value in the current node
   *
   * @param key key
   * @return the specific value if exists at the first position, or the pointer to the next node at
   *     the second position
   */
  Pair<String, String> findValue(String key);
}
