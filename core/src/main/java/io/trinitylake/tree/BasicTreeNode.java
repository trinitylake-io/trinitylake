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

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class BasicTreeNode implements TreeNode {

  private final Map<String, String> values;

  public BasicTreeNode() {
    this.values = Maps.newHashMap();
  }

  @Override
  public Optional<String> path() {
    return Optional.empty();
  }

  @Override
  public String get(String key) {
    return values.get(key);
  }

  @Override
  public void set(String key, String value) {
    values.put(key, value);
  }

  @Override
  public boolean contains(String key) {
    return values.containsKey(key);
  }

  @Override
  public void remove(String key) {
    values.remove(key);
  }

  @Override
  public Set<Map.Entry<String, String>> allKeyValues() {
    return values.entrySet();
  }
}
