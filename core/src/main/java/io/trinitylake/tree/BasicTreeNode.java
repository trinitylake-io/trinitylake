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

import io.trinitylake.relocated.com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class BasicTreeNode implements TreeNode {

  private final Map<String, String> values;
  private String path;
  private Long createdAtMillis;

  public BasicTreeNode() {
    this.values = Maps.newHashMap();
  }

  @Override
  public Optional<String> path() {
    return Optional.ofNullable(path);
  }

  @Override
  public void setPath(String path) {
    this.path = path;
  }

  @Override
  public void clearPath() {
    this.path = null;
  }

  @Override
  public Optional<Long> createdAtMillis() {
    return Optional.ofNullable(createdAtMillis);
  }

  @Override
  public void setCreatedAtMillis(long createdAtMillis) {
    this.createdAtMillis = createdAtMillis;
  }

  @Override
  public void clearCreatedAtMillis() {
    this.createdAtMillis = null;
  }

  @Override
  public int numKeys() {
    return values.size();
  }

  @Override
  public NodeSearchResult search(String key) {
    return ImmutableNodeSearchResult.builder().value(Optional.ofNullable(values.get(key))).build();
  }

  @Override
  public void set(String key, String value) {
    values.put(key, value);
  }

  @Override
  public void remove(String key) {
    values.remove(key);
  }

  @Override
  public List<NodeKeyTableRow> nodeKeyTable() {
    return values.entrySet().stream()
        .map(e -> ImmutableNodeKeyTableRow.builder().key(e.getKey()).value(e.getValue()).build())
        .collect(Collectors.toList());
  }
}
