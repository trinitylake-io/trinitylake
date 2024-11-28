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
package io.trinitylake.key;

import io.trinitylake.models.LakeHouseDef;
import io.trinitylake.util.Pair;

public class KeyEncoding {

  private final LakeHouseDef lakeHouseDef;

  public KeyEncoding(LakeHouseDef lakeHouseDef) {
    this.lakeHouseDef = lakeHouseDef;
  }

  public String encodeNamespace(String name) {
    return null;
  }

  public String encodeTable(String namespaceName, String tableName) {
    return null;
  }

  public Pair<ObjectDefType, String> decode(String key) {
    return null;
  }
}
