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
package io.trinitylake.storage;

import com.google.common.collect.ImmutableMap;
import io.trinitylake.util.InitializationUtil;
import io.trinitylake.util.PropertyUtil;
import java.util.Map;

public class LakehouseStorages {

  public static final String STORAGE_TYPE = "storage.type";
  public static final String STORAGE_TYPE_LOCAL = "local";
  public static final String STORAGE_TYPE_S3 = "s3";
  public static final String STORAGE_TYPE_DEFAULT = STORAGE_TYPE_LOCAL;

  public static final String STORAGE_ROT = "storage.root";

  private static final Map<String, String> STORAGE_TYPE_TO_IMPL =
      ImmutableMap.<String, String>builder()
          .put(STORAGE_TYPE_LOCAL, "io.trinitylake.storage.local.LocalStorageOps")
          .put(STORAGE_TYPE_S3, "io.trinitylake.storage.s3.AmazonS3StorageOps")
          .build();

  public static LakehouseStorage initialize(Map<String, String> properties) {
    LiteralURI storageRoot = new LiteralURI(properties.get(STORAGE_ROT));
    StorageOps storageOps = initializeStorageOps(properties);
    return new BasicLakehouseStorage(storageRoot, storageOps);
  }

  public static StorageOps initializeStorageOps(Map<String, String> properties) {
    String storageType =
        PropertyUtil.propertyAsString(properties, STORAGE_TYPE, STORAGE_TYPE_DEFAULT);

    // if not found in default mapping, just treat type as the impl
    String storageImpl = STORAGE_TYPE_TO_IMPL.getOrDefault(storageType, storageType);
    return InitializationUtil.loadInitializable(storageImpl, properties, StorageOps.class);
  }
}
