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

import io.trinitylake.relocated.com.google.common.collect.ImmutableMap;
import io.trinitylake.relocated.com.google.common.collect.ImmutableSet;
import io.trinitylake.util.FileUtil;
import io.trinitylake.util.PropertyUtil;
import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class CommonStorageOpsProperties implements StorageOpsProperties {

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

  public static final String WRITE_STAGING_DIRECTORY = "write.staging-dir";
  public static final String WRITE_STAGING_DIRECTORY_PATH_DEFAULT =
      System.getProperty("java.io.tmpdir");

  public static final Set<String> PROPERTIES =
      ImmutableSet.<String>builder()
          .add(DELETE_BATCH_SIZE)
          .add(PREPARE_READ_CACHE_SIZE)
          .add(PREPARE_READ_CACHE_EXPIRATION_MILLIS)
          .add(PREPARE_READ_STAGING_DIRECTORY)
          .add(WRITE_STAGING_DIRECTORY)
          .build();

  private static final CommonStorageOpsProperties INSTANCE = new CommonStorageOpsProperties();

  private final Map<String, String> propertiesMap;
  private final int deleteBatchSize;
  private final int prepareReadCacheSize;
  private final long prepareReadCacheExpirationMillis;
  private final String prepareReadStagingDirectoryPath;
  private volatile File prepareReadStagingDirectory;
  private final String writeStagingDirectoryPath;
  private volatile File writeStagingDirectory;

  public CommonStorageOpsProperties() {
    this(ImmutableMap.of());
  }

  public static CommonStorageOpsProperties instance() {
    return INSTANCE;
  }

  public CommonStorageOpsProperties(Map<String, String> input) {
    this.propertiesMap = PropertyUtil.filterProperties(input, PROPERTIES::contains);
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
    this.writeStagingDirectoryPath =
        PropertyUtil.propertyAsString(
            input, WRITE_STAGING_DIRECTORY, WRITE_STAGING_DIRECTORY_PATH_DEFAULT);
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

  public File writeStagingDirectory() {
    if (writeStagingDirectory == null) {
      synchronized (this) {
        if (writeStagingDirectory == null) {
          File path = new File(writeStagingDirectoryPath);
          FileUtil.createStagingDirectoryIfNotExists(path);
          this.writeStagingDirectory = path;
        }
      }
    }
    return writeStagingDirectory;
  }
}
