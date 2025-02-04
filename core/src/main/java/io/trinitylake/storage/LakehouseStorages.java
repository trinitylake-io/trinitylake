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

import io.trinitylake.storage.local.LocalStorageOps;
import io.trinitylake.storage.local.LocalStorageOpsProperties;
import io.trinitylake.storage.s3.AmazonS3StorageOps;
import io.trinitylake.storage.s3.AmazonS3StorageOpsProperties;
import io.trinitylake.util.PropertyUtil;
import java.util.Map;

public class LakehouseStorages {

  public static final String STORAGE_TYPE = "storage.type";
  public static final String STORAGE_TYPE_LOCAL = "local";
  public static final String STORAGE_TYPE_S3 = "s3";
  public static final String STORAGE_TYPE_DEFAULT = STORAGE_TYPE_LOCAL;

  public static final String STORAGE_ROT = "storage.root";

  public static LakehouseStorage initialize(Map<String, String> properties) {
    URI storageRoot = new URI(properties.get(STORAGE_ROT));
    StorageOps storageOps = initializeStorageOps(properties);
    return new BasicLakehouseStorage(storageRoot, storageOps);
  }

  public static StorageOps initializeStorageOps(Map<String, String> properties) {
    String storageType =
        PropertyUtil.propertyAsString(properties, STORAGE_TYPE, STORAGE_TYPE_DEFAULT);
    CommonStorageOpsProperties commonProperties = new CommonStorageOpsProperties(properties);
    if (STORAGE_TYPE_LOCAL.equals(storageType)) {
      LocalStorageOpsProperties localProperties = new LocalStorageOpsProperties(properties);
      return new LocalStorageOps(commonProperties, localProperties);
    }

    if (STORAGE_TYPE_S3.equals(storageType)) {
      AmazonS3StorageOpsProperties s3Properties = new AmazonS3StorageOpsProperties(properties);
      return new AmazonS3StorageOps(commonProperties, s3Properties);
    }

    throw new IllegalArgumentException("Unsupported storage type: " + storageType);
  }
}
