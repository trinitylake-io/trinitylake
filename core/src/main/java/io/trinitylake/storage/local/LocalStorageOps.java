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
package io.trinitylake.storage.local;

import io.trinitylake.storage.*;
import io.trinitylake.util.ValidationUtil;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LocalStorageOps implements StorageOps {

  private CommonStorageOpsProperties commonProperties;
  private LocalStorageOpsProperties localProperties;

  public LocalStorageOps() {
    this(CommonStorageOpsProperties.instance(), LocalStorageOpsProperties.instance());
  }

  public LocalStorageOps(
      CommonStorageOpsProperties commonProperties, LocalStorageOpsProperties localProperties) {
    this.commonProperties = commonProperties;
    this.localProperties = localProperties;
  }

  @Override
  public StorageOpsProperties commonProperties() {
    return commonProperties;
  }

  @Override
  public StorageOpsProperties systemSpecificProperties() {
    return localProperties;
  }

  @Override
  public void prepareToRead(LiteralURI uri) {}

  @Override
  public SeekableInputStream startRead(LiteralURI uri) {
    return startReadLocal(uri);
  }

  @Override
  public LocalInputStream startReadLocal(LiteralURI uri) {
    return new LocalInputStream(new File(uri.toString()));
  }

  @Override
  public AtomicOutputStream startWrite(LiteralURI uri) {
    return new LocalOutputStream(Paths.get(uri.toString()), commonProperties, localProperties);
  }

  @Override
  public boolean exists(LiteralURI uri) {
    return new File(fileSystemPath(uri)).exists();
  }

  @Override
  public void delete(List<LiteralURI> uris) {
    for (LiteralURI uri : uris) {
      File file = new File(fileSystemPath(uri));
      file.delete();
    }
  }

  @Override
  public List<LiteralURI> list(LiteralURI prefix) {
    Path startingPath = Paths.get(fileSystemPath(prefix));
    String[] result = startingPath.toFile().list();
    ValidationUtil.checkNotNull(result, "Invalid prefix for listing: %s", prefix);
    return Arrays.stream(result)
        .map(name -> "file://" + startingPath.resolve(name))
        .map(LiteralURI::new)
        .collect(Collectors.toList());
  }

  @Override
  public void close() throws IOException {}

  @Override
  public void initialize(Map<String, String> properties) {
    this.commonProperties = new CommonStorageOpsProperties(properties);
    this.localProperties = new LocalStorageOpsProperties(properties);
  }

  private static String fileSystemPath(LiteralURI uri) {
    return "/" + uri.path();
  }
}
