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

import io.trinitylake.storage.local.LocalInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A Lakehouse storage starts with a root URI location. Most access to the lakehouse storage should
 * be to paths under the root. Accessing the full URI should directly use {@link #ops()}.
 */
public interface LakehouseStorage extends Closeable {

  LiteralURI root();

  StorageOps ops();

  default void prepareToReadLocal(String path) {
    ops().prepareToReadLocal(root().extendPath(path));
  }

  default SeekableInputStream startRead(String path) {
    return ops().startRead(root().extendPath(path));
  }

  default LocalInputStream startReadLocal(String path) {
    return ops().startReadLocal(root().extendPath(path));
  }

  default boolean exists(String path) {
    return ops().exists(root().extendPath(path));
  }

  default AtomicOutputStream startCommit(String path) {
    return ops().startCommit(root().extendPath(path));
  }

  default OutputStream startOverwrite(String path) {
    return ops().startOverwrite(root().extendPath(path));
  }

  default void delete(List<String> paths) {
    ops().delete(paths.stream().map(root()::extendPath).collect(Collectors.toList()));
  }

  default List<LiteralURI> list(String prefixPath) {
    return ops().list(root().extendPath(prefixPath));
  }

  @Override
  default void close() throws IOException {
    ops().close();
  }
}
