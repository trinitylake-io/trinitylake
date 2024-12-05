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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.reactivestreams.Publisher;

/**
 * Storage that starts with a root URI location. All access to the storage should be to paths under
 * the root. The only exception is for accessing external tables, and that should directly use
 * {@link #ops()} to access the full external URI.
 */
public interface Storage extends Closeable {

  URI root();

  StorageOps ops();

  default void prepareToRead(String path) {
    ops().prepareToRead(root().extendPath(path));
  }

  default SeekableInputStream startRead(String path) {
    return ops().startRead(root().extendPath(path));
  }

  default PositionOutputStream startWrite(String path) {
    return ops().startWrite(root().extendPath(path));
  }

  default void delete(List<String> paths) {
    ops().delete(paths.stream().map(root()::extendPath).collect(Collectors.toList()));
  }

  default Publisher<URI> list(String prefixPath) {
    return ops().list(root().extendPath(prefixPath));
  }

  @Override
  default void close() throws IOException {
    ops().close();
  }
}
