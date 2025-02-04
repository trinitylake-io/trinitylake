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

import io.trinitylake.exception.StorageListFailureException;
import io.trinitylake.storage.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
  public void prepareToRead(URI uri) {}

  @Override
  public SeekableInputStream startRead(URI uri) {
    return startReadLocal(uri);
  }

  @Override
  public LocalInputStream startReadLocal(URI uri) {
    return new LocalInputStream(new File(uri.toString()));
  }

  @Override
  public AtomicOutputStream startWrite(URI uri) {
    return new LocalOutputStream(new File(uri.toString()), commonProperties, localProperties);
  }

  @Override
  public boolean exists(URI uri) {
    return new File(uri.toString()).exists();
  }

  @Override
  public void delete(List<URI> uris) {
    for (URI uri : uris) {
      File file = new File(uri.toString());
      file.delete();
    }
  }

  @Override
  public List<URI> list(URI prefix) {
    Path startingPath = Paths.get(prefix.toString());
    try (Stream<Path> stream = Files.walk(startingPath)) {
      return stream
          .filter(Files::isRegularFile)
          .map(path -> new URI(path.toString()))
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new StorageListFailureException(e);
    }
  }

  @Override
  public void close() throws IOException {}

  @Override
  public void initialize(Map<String, String> properties) {
    this.commonProperties = new CommonStorageOpsProperties(properties);
    this.localProperties = new LocalStorageOpsProperties(properties);
  }
}
