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

import io.trinitylake.models.LakeHouseDef;
import io.trinitylake.models.NamespaceDef;
import io.trinitylake.models.TableDef;
import io.trinitylake.tree.TreeNode;

public class LakeHouseVersion {

  public LakeHouseVersion(TreeNode treeNode) {}

  /**
   * Retrieve the LakeHouse definition
   *
   * @return LakeHouse definition
   */
  public LakeHouseDef def() {
    return null;
  }

  /**
   * Retrieve the namespace definition
   *
   * @param name name of the namespace
   * @return namespace definition
   * @throws io.trinitylake.exception.ObjectNotFoundException if the namespace is not found
   */
  public NamespaceDef namespaceDef(String name) {
    return null;
  }

  /**
   * Retrieve the table definition
   *
   * @param namespaceName name of the namespace
   * @param tableName name of the table
   * @return table definition
   * @throws io.trinitylake.exception.ObjectNotFoundException if the table is not found
   */
  public TableDef tableDef(String namespaceName, String tableName) {
    return null;
  }
}
