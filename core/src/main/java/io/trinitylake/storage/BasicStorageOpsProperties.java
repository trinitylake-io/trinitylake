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
import io.trinitylake.util.FileUtil;
import io.trinitylake.util.PropertyUtil;
import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class BasicStorageOpsProperties implements StorageOpsProperties {

  private static final BasicStorageOpsProperties INSTANCE = new BasicStorageOpsProperties();

  public static final String DELETE_BATCH_SIZE = "delete.batch-size";
  public static final int DELETE_BATCH_SIZE_DEFAULT = 1000;

  public static final String PREPARE_READ_CACHE_SIZE = "prepare-read.cache-size";
  public static final int PREPARE_READ_CACHE_SIZE_DEFAULT = 1000;

  public static final String PREPARE_READ_CACHE_EXPIRATION_MILLIS =
      "prepare-read.cache-expiration-millis";
  public static final long PREPARE_READ_CACHE_EXPIRATION_MILLIS_DEFAULT =
      TimeUnit.MINUTES.toMillis(10);

  public static final String PREPARE_READ_STAGING_DIRECTORY = "prepare-read.staging-dir";
  public static final String PREPARE_READ_STAGING_DIRECTORY_PATH_DEFAULT =
      System.getProperty("java.io.tmpdir");

  private final Map<String, String> propertiesMap;
  private final int deleteBatchSize;
  private final int prepareReadCacheSize;
  private final long prepareReadCacheExpirationMillis;
  private final String prepareReadStagingDirectoryPath;
  private volatile File prepareReadStagingDirectory;

  public BasicStorageOpsProperties() {
    this(ImmutableMap.of());
  }

  public static BasicStorageOpsProperties instance() {
    return INSTANCE;
  }

  public BasicStorageOpsProperties(Map<String, String> input) {
    this.propertiesMap = ImmutableMap.copyOf(input);
    this.deleteBatchSize =
        PropertyUtil.propertyAsInt(input, DELETE_BATCH_SIZE, DELETE_BATCH_SIZE_DEFAULT);
    this.prepareReadCacheSize =
        PropertyUtil.propertyAsInt(input, PREPARE_READ_CACHE_SIZE, PREPARE_READ_CACHE_SIZE_DEFAULT);
    this.prepareReadCacheExpirationMillis =
        PropertyUtil.propertyAsLong(
            input,
            PREPARE_READ_CACHE_EXPIRATION_MILLIS,
            PREPARE_READ_CACHE_EXPIRATION_MILLIS_DEFAULT);
    this.prepareReadStagingDirectoryPath =
        PropertyUtil.propertyAsString(
            input, PREPARE_READ_STAGING_DIRECTORY, PREPARE_READ_STAGING_DIRECTORY_PATH_DEFAULT);
  }

  @Override
  public Map<String, String> asStringMap() {
    return propertiesMap;
  }

  public int deleteBatchSize() {
    return deleteBatchSize;
  }

  public int prepareReadCacheSize() {
    return prepareReadCacheSize;
  }

  public long prepareReadCacheExpirationMillis() {
    return prepareReadCacheExpirationMillis;
  }

  public File prepareReadStagingDirectory() {
    if (prepareReadStagingDirectory == null) {
      synchronized (this) {
        if (prepareReadStagingDirectory == null) {
          File path = new File(prepareReadStagingDirectoryPath);
          FileUtil.createStagingDirectoryIfNotExists(path);
          this.prepareReadStagingDirectory = path;
        }
      }
    }
    return prepareReadStagingDirectory;
  }
}
