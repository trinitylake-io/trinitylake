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

import io.trinitylake.Initializable;
import io.trinitylake.storage.local.LocalInputStream;
import java.io.Closeable;
import java.io.OutputStream;
import java.util.List;

/** Common operations that should be supported by a TrinityLake storage */
public interface StorageOps extends Closeable, Initializable {

  StorageOpsProperties commonProperties();

  StorageOpsProperties systemSpecificProperties();

  void prepareToReadLocal(LiteralURI uri);

  SeekableInputStream startRead(LiteralURI uri);

  LocalInputStream startReadLocal(LiteralURI uri);

  AtomicOutputStream startCommit(LiteralURI uri);

  OutputStream startOverwrite(LiteralURI uri);

  boolean exists(LiteralURI uri);

  void delete(List<LiteralURI> uris);

  List<LiteralURI> list(LiteralURI prefix);
}
