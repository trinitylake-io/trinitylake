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

import io.trinitylake.exception.StorageReadFailureException;
import io.trinitylake.models.LakehouseDef;
import io.trinitylake.models.NamespaceDef;
import io.trinitylake.models.TableDef;
import io.trinitylake.storage.Storage;
import io.trinitylake.tree.*;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import javax.annotation.Nullable;

public class BasicLakehouseVersion implements LakehouseVersion {

  private final Storage storage;
  private final TreeRoot root;

  public BasicLakehouseVersion(Storage storage, TreeRoot root) {
    this.storage = storage;
    this.root = root;
  }

  @Override
  public long version() {
    return root.version();
  }

  @Override
  public LakehouseDef definition() {
    InputStream stream = storage.startRead(root.lakehouseDefPath());
    try {
      return LakehouseDef.parseFrom(stream);
    } catch (IOException e) {
      throw new StorageReadFailureException(
          "Failed to parse Lakehouse definition from " + root.lakehouseDefPath(), e);
    }
  }

  @Override
  public long createdAtMillis() {
    return root.createdAtMillis();
  }

  @Nullable
  @Override
  public LakehouseVersion previousVersion() {
    TreeRoot previousRoot = new ArrowTreeRoot(storage, root.previousRootPath());
    return new BasicLakehouseVersion(storage, previousRoot);
  }

  @Nullable
  @Override
  public LakehouseVersion rollbackFromVersion() {
    TreeRoot rollbackFromRoot = new ArrowTreeRoot(storage, root.rollbackFromRootPath());
    return new BasicLakehouseVersion(storage, rollbackFromRoot);
  }

  @Override
  public List<String> showNamespaces() {
    return null;
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
