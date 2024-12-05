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
import io.trinitylake.models.NamespaceDef;
import io.trinitylake.models.TableDef;
import io.trinitylake.storage.Storage;
import java.util.List;

public class BasicLakehouseVersion implements LakehouseVersion {

  private final Storage storage;
  private final long version;

  public BasicLakehouseVersion(Storage storage, long version) {
    this.storage = storage;
    this.version = version;
  }

  @Override
  public LakehouseDef describeLakehouse() {
    return null;
  }

  @Override
  public long version() {
    return version;
  }

  @Override
  public NamespaceDef describeNamespace(String namespaceName) {
    return null;
  }

  @Override
  public List<String> showTables(String namespaceName) {
    return null;
  }

  @Override
  public TableDef describeTable(String namespaceName, String tableName) {
    return null;
  }
}
