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
package io.trinitylake.storage.s3;

import com.google.common.collect.ImmutableMap;
import io.trinitylake.storage.StorageOpsProperties;
import java.util.Map;

public class AmazonS3StorageOpsProperties implements StorageOpsProperties {

  private static final AmazonS3StorageOpsProperties INSTANCE = new AmazonS3StorageOpsProperties();

  private final Map<String, String> propertiesMap;

  public AmazonS3StorageOpsProperties() {
    this(ImmutableMap.of());
  }

  public AmazonS3StorageOpsProperties(Map<String, String> input) {
    this.propertiesMap = ImmutableMap.copyOf(input);
  }

  public static AmazonS3StorageOpsProperties instance() {
    return INSTANCE;
  }

  @Override
  public Map<String, String> asStringMap() {
    return propertiesMap;
  }
}
