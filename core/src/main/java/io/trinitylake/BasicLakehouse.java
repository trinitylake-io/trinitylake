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

import io.trinitylake.storage.Storage;
import java.util.List;

public class BasicLakehouse implements Lakehouse {

  private final Storage storage;

  public BasicLakehouse(Storage storage) {
    this.storage = storage;
  }

  @Override
  public BasicLakehouseVersion beginTransaction(String transaction) {
    return null;
  }

  @Override
  public BasicLakehouseVersion commitTransaction(
      String transaction, BasicLakehouseVersion version) {
    return null;
  }

  @Override
  public void rollbackTransaction(String transaction) {}

  @Override
  public List<Long> showLakehouseVersions() {
    return null;
  }

  @Override
  public LakehouseVersion loadLakehouseVersion(long version) {
    return null;
  }

  @Override
  public LakehouseVersion loadLatestLakehouseVersion() {
    return null;
  }
}
