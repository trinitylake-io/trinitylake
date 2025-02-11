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

import io.trinitylake.relocated.com.google.common.collect.ImmutableMap;
import io.trinitylake.relocated.com.google.common.collect.ImmutableSet;
import io.trinitylake.storage.StorageOpsProperties;
import io.trinitylake.util.PropertyUtil;
import java.util.Map;
import java.util.Set;

public class AmazonS3StorageOpsProperties implements StorageOpsProperties {

  private static final AmazonS3StorageOpsProperties INSTANCE = new AmazonS3StorageOpsProperties();

  public static final String S3_REGION = "s3.region";
  public static final String S3_ACCESS_KEY_ID = "s3.access-key-id";
  public static final String S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
  public static final String S3_SESSION_TOKEN = "s3.session-token";

  public static final Set<String> PROPERTIES =
      ImmutableSet.<String>builder()
          .add(S3_REGION)
          .add(S3_ACCESS_KEY_ID)
          .add(S3_SECRET_ACCESS_KEY)
          .add(S3_SESSION_TOKEN)
          .build();

  private final Map<String, String> propertiesMap;

  private final String region;
  private final String accessKeyId;
  private final String secretAccessKey;
  private final String sessionToken;

  public AmazonS3StorageOpsProperties() {
    this(ImmutableMap.of());
  }

  public AmazonS3StorageOpsProperties(Map<String, String> input) {
    this.propertiesMap = PropertyUtil.filterProperties(input, PROPERTIES::contains);
    this.region = input.get(S3_REGION);
    this.accessKeyId = input.get(S3_ACCESS_KEY_ID);
    this.secretAccessKey = input.get(S3_SECRET_ACCESS_KEY);
    this.sessionToken = input.get(S3_SESSION_TOKEN);
  }

  public static AmazonS3StorageOpsProperties instance() {
    return INSTANCE;
  }

  @Override
  public Map<String, String> asStringMap() {
    return propertiesMap;
  }

  public String region() {
    return region;
  }

  public String accessKeyId() {
    return accessKeyId;
  }

  public String secretAccessKey() {
    return secretAccessKey;
  }

  public String sessionToken() {
    return sessionToken;
  }
}
