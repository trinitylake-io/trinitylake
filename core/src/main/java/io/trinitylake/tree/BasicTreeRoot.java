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

import io.trinitylake.util.ValidationUtil;
import java.util.Optional;

public class BasicTreeRoot extends BasicTreeNode implements TreeRoot {

  private String previousRootNodeFilePath;
  private String rollbackFromRootNodeFilePath;
  private String lakehouseDefFilePath;

  public BasicTreeRoot() {
    super();
  }

  @Override
  public Optional<String> previousRootNodeFilePath() {
    return Optional.ofNullable(previousRootNodeFilePath);
  }

  @Override
  public void setPreviousRootNodeFilePath(String previousRootNodeFilePath) {
    this.previousRootNodeFilePath = previousRootNodeFilePath;
  }

  @Override
  public void clearPreviousRootNodeFilePath() {
    this.previousRootNodeFilePath = null;
  }

  @Override
  public Optional<String> rollbackFromRootNodeFilePath() {
    return Optional.ofNullable(rollbackFromRootNodeFilePath);
  }

  @Override
  public void setRollbackFromRootNodeFilePath(String rollbackFromRootNodeFilePath) {
    this.rollbackFromRootNodeFilePath = rollbackFromRootNodeFilePath;
  }

  @Override
  public void clearRollbackFromRootNodeFilePath() {
    this.rollbackFromRootNodeFilePath = null;
  }

  @Override
  public String lakehouseDefFilePath() {
    ValidationUtil.checkState(
        lakehouseDefFilePath != null,
        "Lakehouse definition file path should be set for a tree root");
    return lakehouseDefFilePath;
  }

  @Override
  public void setLakehouseDefFilePath(String lakehouseDefFilePath) {
    this.lakehouseDefFilePath = lakehouseDefFilePath;
  }
}
