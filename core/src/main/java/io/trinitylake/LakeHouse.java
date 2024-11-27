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

public class LakeHouse {

  private final Storage storage;
  private final String rootLocation;

  public LakeHouse(Storage storage, String rootLocation) {
    this.storage = storage;
    this.rootLocation = rootLocation;
  }

  /**
   * Begin a transaction
   *
   * @param transaction transaction ID
   * @return the version of the LakeHouse for all read and write
   */
  public LakeHouseVersion beginTransaction(String transaction) {
    return null;
  }

  /**
   * Commit a transaction
   *
   * @param transaction transaction ID
   * @return the new version of the LakeHouse after commit
   * @throws io.trinitylake.exception.CommitFailureException if commit failed
   */
  public LakeHouseVersion commitTransaction(String transaction) {
    return null;
  }

  /**
   * Rollback an ongoing transaction
   *
   * @param transaction transaction ID
   */
  public void rollbackTransaction(String transaction) {}
}
